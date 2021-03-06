package scheduler

import (
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/henrylee2cn/pholcus/app/aid/history"
	"github.com/henrylee2cn/pholcus/app/downloader/request"
	"github.com/henrylee2cn/pholcus/common/util"
	"github.com/henrylee2cn/pholcus/logs"
	"github.com/henrylee2cn/pholcus/runtime/cache"
	"github.com/henrylee2cn/pholcus/runtime/status"
)

// 一个Spider实例的请求矩阵
type Matrix struct {
	maxPage         int64                       // 最大采集页数，以负数形式表示
	resCount        int32                       // 资源使用情况计数
	spiderName      string                      // 所属Spider
	reqs            map[int][]*request.Request  // [优先级]队列，优先级默认为0
	priorities      []int                       // 优先级顺序，从低到高
	history         history.Historier           // 历史记录
	tempHistory     map[string]bool             // 临时记录 [hash(url+method)]true
	failures        map[string]*request.Request // 历史及本次失败请求
	tempHistoryLock sync.RWMutex
	failureLock     sync.Mutex
	sync.Mutex
	CurrentRule string //当前执行的规则 add by lyken 20160516
}

func newMatrix(spiderName, spiderSubName string, maxPage int64) *Matrix {
	matrix := &Matrix{
		spiderName:  spiderName,
		maxPage:     maxPage,
		reqs:        make(map[int][]*request.Request),
		priorities:  []int{},
		history:     history.New(spiderName, spiderSubName),
		tempHistory: make(map[string]bool),
		failures:    make(map[string]*request.Request),
	}
	if cache.Task.Mode != status.SERVER {
		matrix.history.ReadSuccess(cache.Task.OutType, cache.Task.SuccessInherit)
		matrix.history.ReadFailure(cache.Task.OutType, cache.Task.FailureInherit)
		matrix.setFailures(matrix.history.PullFailure())
	}
	return matrix
}

// 添加请求到队列，并发安全
func (self *Matrix) Push(req *request.Request) {
	if sdl.checkStatus(status.STOP) {
		return
	}

	// 禁止并发，降低请求积存量
	self.Lock()
	defer self.Unlock()

	// 达到请求上限，停止该规则运行
	if self.maxPage >= 0 {
		return
	}

	// 暂停状态时等待，降低请求积存量
	waited := false
	for sdl.checkStatus(status.PAUSE) {
		waited = true
		runtime.Gosched()
	}
	if waited && sdl.checkStatus(status.STOP) {
		return
	}

	// 资源使用过多时等待，降低请求积存量
	waited = false
	for self.resCount > sdl.avgRes() {
		waited = true
		runtime.Gosched()
	}
	if waited && sdl.checkStatus(status.STOP) {
		return
	}

	// 不可重复下载的req
	if !req.IsReloadable() {
		hash := makeUnique(req)
		// 已存在成功记录时退出
		if self.hasHistory(hash) {
			return
		}
		// 添加到临时记录
		self.insertTempHistory(hash)
	}

	var priority = req.GetPriority()

	/**
	 **add by lyken 20160516
	 **/
	//---start
	pri_Len := len(self.priorities) - 1
	if pri_Len >= 0 &&
		!req.GetAutoSequence() {
		priority = self.priorities[pri_Len] + 1
	}
	//self.AutoSequence[ruleName] = priority
	self.CurrentRule = req.GetRuleName()
	//---end

	// 初始化该蜘蛛下该优先级队列
	if _, found := self.reqs[priority]; !found {
		self.priorities = append(self.priorities, priority)
		sort.Ints(self.priorities) // 从小到大排序
		self.reqs[priority] = []*request.Request{}
	}

	// 添加请求到队列
	self.reqs[priority] = append(self.reqs[priority], req)

	// 大致限制加入队列的请求量，并发情况下应该会比maxPage多
	atomic.AddInt64(&self.maxPage, 1)
}

// 从队列取出请求，不存在时返回nil，并发安全
func (self *Matrix) Pull() (req *request.Request) {
	if !sdl.checkStatus(status.RUN) {
		return
	}
	self.Lock()
	defer self.Unlock()
	// 按优先级从高到低取出请求
	for i := len(self.reqs) - 1; i >= 0; i-- {
		idx := self.priorities[i]
		if len(self.reqs[idx]) > 0 {
			req = self.reqs[idx][0]
			self.reqs[idx] = self.reqs[idx][1:]
			if sdl.useProxy {
				req.SetProxy(sdl.proxy.GetOne(req.GetUrl()))
			} else {
				req.SetProxy("")
			}
			return
		}
	}
	return
}

func (self *Matrix) Use() {
	defer func() {
		recover()
	}()
	sdl.count <- true
	atomic.AddInt32(&self.resCount, 1)
}

func (self *Matrix) Free() {
	<-sdl.count
	atomic.AddInt32(&self.resCount, -1)
}

// 返回是否作为新的失败请求被添加至队列尾部
func (self *Matrix) DoHistory(req *request.Request, ok bool) bool {
	hash := makeUnique(req)

	if !req.IsReloadable() {
		self.tempHistoryLock.Lock()
		delete(self.tempHistory, hash)
		self.tempHistoryLock.Unlock()

		if ok {
			/**
			*是否不插入成功记录
			*self.history.UpsertSuccess(hash)
			*change by lyken 20160509
			**/
			//start
			if req.TempSuccess {
				self.history.UpsertTempSuccess(hash)
			} else {
				self.history.UpsertSuccess(hash)
			}
			//end
			return false
		}
	}

	if ok {
		return false
	}

	self.failureLock.Lock()
	defer self.failureLock.Unlock()
	if _, ok := self.failures[hash]; !ok {
		// 首次失败时，在任务队列末尾重新执行一次
		self.failures[hash] = req
		logs.Log.Informational(" *     + 失败请求: [%v]\n", req.GetUrl())
		return true
	}
	// 失败两次后，加入历史失败记录
	self.history.UpsertFailure(req)
	return false
}

func (self *Matrix) CanStop() bool {
	if sdl.checkStatus(status.STOP) {
		return true
	}
	if self.maxPage >= 0 {
		return true
	}
	if self.resCount != 0 {
		return false
	}
	if self.Len() > 0 {
		return false
	}

	self.failureLock.Lock()
	defer self.failureLock.Unlock()
	if len(self.failures) > 0 {
		// 重新下载历史记录中失败的请求
		var goon bool
		for hash, req := range self.failures {
			if req == nil {
				continue
			}
			self.failures[hash] = nil
			goon = true
			logs.Log.Informational(" *     - 失败请求: [%v]\n", req.GetUrl())
			self.Push(req)
		}
		if goon {
			return false
		}
	}
	return true
}

// 非服务器模式下保存历史成功记录
func (self *Matrix) TryFlushSuccess() {
	if cache.Task.Mode != status.SERVER && cache.Task.SuccessInherit {
		self.history.FlushSuccess(cache.Task.OutType)
	}
}

// 非服务器模式下保存历史失败记录
func (self *Matrix) TryFlushFailure() {
	if cache.Task.Mode != status.SERVER && cache.Task.FailureInherit {
		self.history.FlushFailure(cache.Task.OutType)
	}
}

// 等待处理中的请求完成
func (self *Matrix) Wait() {
	for self.resCount != 0 {
		runtime.Gosched()
	}
}

func (self *Matrix) Len() int {
	self.Lock()
	defer self.Unlock()
	var l int
	for _, reqs := range self.reqs {
		l += len(reqs)
	}
	return l
}

func (self *Matrix) hasHistory(hash string) bool {
	if self.history.HasSuccess(hash) {
		return true
	}
	self.tempHistoryLock.RLock()
	has := self.tempHistory[hash]
	self.tempHistoryLock.RUnlock()
	return has
}

/**
**add by lyken 20160510
**/
func (self *Matrix) HasTempHistory(req *request.Request) bool {
	hash := makeUnique(req)
	if self.history.HasTempSuccess(hash) {
		return true
	} else {
		return false
	}
}

/**
**add by lyken 20160510
**/
func (self *Matrix) DeleteTempSuccess(req *request.Request) {
	hash := makeUnique(req)
	self.history.DeleteTempSuccess(hash)
}

func (self *Matrix) insertTempHistory(hash string) {
	self.tempHistoryLock.Lock()
	self.tempHistory[hash] = true
	self.tempHistoryLock.Unlock()
}

func (self *Matrix) setFailures(reqs map[*request.Request]bool) {
	self.failureLock.Lock()
	defer self.failureLock.Unlock()
	for req := range reqs {
		self.failures[makeUnique(req)] = req
		logs.Log.Informational(" *     + 失败请求: [%v]\n", req.GetUrl())
	}
}

func makeUnique(req *request.Request) string {
	return util.MakeUnique(req.GetUrl() + req.GetMethod())
}

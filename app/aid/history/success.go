package history

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/henrylee2cn/pholcus/common/mgo"
	"github.com/henrylee2cn/pholcus/common/mysql"
	"github.com/henrylee2cn/pholcus/config"
)

type Success struct {
	tabName      string
	tabTempName  string //add by lyken 20160510
	fileName     string
	fileTempName string          //add by lyken 20160510
	new          map[string]bool // [hash(url+method)]true
	old          map[string]bool // [hash(url+method)]true
	tmp          map[string]bool // [hash(url+method)]true add by lyken 20160510
	tmpNew       map[string]bool // [hash(url+method)]true add by lyken 20160510
	deleteTmp    map[string]bool // [hash(url+method)]true add by lyken 20160510
	inheritable  bool
	sync.RWMutex
}

// 更新或加入成功记录，
// 对比是否已存在，不存在就记录，
// 返回值表示是否有插入操作。
func (self *Success) UpsertSuccess(hash string) bool {
	self.RWMutex.Lock()
	defer self.RWMutex.Unlock()

	if self.old[hash] {
		return false
	}
	if self.new[hash] {
		return false
	}
	self.new[hash] = true
	return true
}

/**
** 更新或加入缓存中的成功记录
** 对比是否已存在，不存在就记录，
** 返回值表示是否有插入操作。
** add by lyken 20160510
**/
func (self *Success) UpsertTempSuccess(hash string) bool {
	self.RWMutex.Lock()
	defer self.RWMutex.Unlock()
	fmt.Println("hash:", hash)
	if self.old[hash] {
		return false
	}
	if self.new[hash] {
		return false
	}
	if self.tmpNew[hash] {
		return false
	}
	if self.tmp[hash] {
		return false
	} else {
		self.tmpNew[hash] = true
	}
	return true
}

func (self *Success) HasSuccess(hash string) bool {
	self.RWMutex.Lock()
	has := self.old[hash] || self.new[hash]
	self.RWMutex.Unlock()
	return has
}

// 删除成功记录
func (self *Success) DeleteSuccess(hash string) {
	self.RWMutex.Lock()
	delete(self.new, hash)
	self.RWMutex.Unlock()
}

/**
**add by lyken 20160510
**/
func (self *Success) HasTempSuccess(hash string) bool {
	self.RWMutex.Lock()
	has := self.tmp[hash] //|| self.tmpNew[hash]
	self.RWMutex.Unlock()
	return has
}

/**
**删除缓存中的成功记录
**add by lyken 20160510
**/
func (self *Success) DeleteTempSuccess(hash string) {
	self.RWMutex.Lock()
	if self.tmp[hash] || self.tmpNew[hash] {
		delete(self.tmp, hash)
		delete(self.tmpNew, hash)
		self.deleteTmp[hash] = true
	}
	self.RWMutex.Unlock()
}

func (self *Success) flush(provider string) (sLen int, err error) {
	self.RWMutex.Lock()
	defer self.RWMutex.Unlock()

	sLen = len(self.new)
	sLenTmp := len(self.tmpNew)          //add by lyken
	sLenDeleteTmp := len(self.deleteTmp) //add by lyken
	if sLen == 0 && sLenTmp == 0 && sLenDeleteTmp == 0 {
		return
	}

	switch provider {
	case "mgo":
		if mgo.Error() != nil {
			err = fmt.Errorf(" *     Fail  [添加成功记录][mgo]: %v 条 [ERROR]  %v\n", sLen, mgo.Error())
			return
		}
		var docs = make([]map[string]interface{}, sLen)
		var i int
		for key := range self.new {
			docs[i] = map[string]interface{}{"_id": key}
			self.old[key] = true
			i++
		}
		err := mgo.Mgo(nil, "insert", map[string]interface{}{
			"Database":   config.DB_NAME,
			"Collection": self.tabName,
			"Docs":       docs,
		})
		if err != nil {
			err = fmt.Errorf(" *     Fail  [添加成功记录][mgo]: %v 条 [ERROR]  %v\n", sLen, err)
		}

		/**
		**
		** add by lyken 20160510
		**/
		//start
		if sLenTmp > 0 {
			var docsTmp = make([]map[string]interface{}, sLenTmp)
			var iTmp int
			for keyTmp := range self.tmpNew {
				docsTmp[iTmp] = map[string]interface{}{"_id": keyTmp}
				iTmp++
			}
			err = mgo.Mgo(nil, "insert", map[string]interface{}{
				"Database":   config.DB_NAME,
				"Collection": self.tabTempName,
				"Docs":       docsTmp,
			})
			if err != nil {
				err = fmt.Errorf(" *     Fail  [添加成功记录][mgo]: %v 条 [ERROR]  %v\n", sLen, err)
			}
		}
		//end

	case "mysql":
		_, err := mysql.DB()
		if err != nil {
			return sLen, fmt.Errorf(" *     Fail  [添加成功记录][mysql]: %v 条 [ERROR]  %v\n", sLen, err)
		}
		table, ok := getWriteMysqlTable(self.tabName)
		if !ok {
			table = mysql.New()
			table.SetTableName("`" + self.tabName + "`").CustomPrimaryKey(`id VARCHAR(255) not null primary key`)
			err = table.Create()
			if err != nil {
				return sLen, fmt.Errorf(" *     Fail  [添加成功记录][mysql]: %v 条 [ERROR]  %v\n", sLen, err)
			}
			setWriteMysqlTable(self.tabName, table)
		}
		for key := range self.new {
			table.AutoInsert([]string{key})
			self.old[key] = true
		}
		err = table.FlushInsert()

		if err != nil {
			return sLen, fmt.Errorf(" *     Fail  [添加成功记录][mysql]: %v 条 [ERROR]  %v\n", sLen, err)
		}

		/**
		**
		** add by lyken 20160510
		**/
		//start
		if sLenTmp > 0 {
			tableTmp, okTmp := getWriteMysqlTable(self.tabTempName)
			if !okTmp {
				tableTmp = mysql.New()
				tableTmp.SetTableName("`" + self.tabTempName + "`").CustomPrimaryKey(`id VARCHAR(255) not null primary key`)
				err = tableTmp.Create()
				if err != nil {
					return sLenTmp, fmt.Errorf(" *     Fail-tmp  [添加成功记录][mysql]: %v 条 [ERROR]  %v\n", sLenTmp, err)
				}
				setWriteMysqlTable(self.tabTempName, tableTmp)
			}
			for key := range self.tmpNew {
				tableTmp.AutoInsert([]string{key})
			}
			err = tableTmp.FlushInsert()
			if err != nil {
				return sLenTmp, fmt.Errorf(" *     Fail-tmp  [添加成功记录][mysql]: %v 条 [ERROR]  %v\n", sLenTmp, err)
			}

		}

		if sLenDeleteTmp > 0 {
			tableDeleteTmp, okTmp := getWriteMysqlTable(self.tabTempName)

			if !okTmp {
				tableDeleteTmp = mysql.New()
				tableDeleteTmp.SetTableName("`" + self.tabTempName + "`").CustomPrimaryKey(`id VARCHAR(255) not null primary key`)
				err = tableDeleteTmp.Create()
				if err != nil {
					return sLenTmp, fmt.Errorf(" *     Fail-tmp  [添加成功记录][mysql]: %v 条 [ERROR]  %v\n", sLenTmp, err)
				}
				setWriteMysqlTable(self.tabTempName, tableDeleteTmp)
			}
			for key := range self.deleteTmp {
				tableDeleteTmp.AddAnd("id=" + `'` + key + `'`)
			}
			err = tableDeleteTmp.FlushDelete()
			if err != nil {
				return sLenDeleteTmp, fmt.Errorf(" *     Fail-tmp  [添加成功记录][mysql]: %v 条 [ERROR]  %v\n", sLenDeleteTmp, err)
			}
		}
		//end

	default:
		f, _ := os.OpenFile(self.fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0777)

		b, _ := json.Marshal(self.new)
		b[0] = ','
		f.Write(b[:len(b)-1])
		f.Close()

		for key := range self.new {
			self.old[key] = true
		}

		/**
		**
		** add by lyken 20160510
		**/
		//start
		fTmp, _ := os.OpenFile(self.fileTempName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0660)

		bTmp, _ := json.Marshal(self.tmpNew)
		bTmp[0] = ','
		fTmp.Write(bTmp[:len(bTmp)-1])
		fTmp.Close()
		//end
	}
	self.new = make(map[string]bool)
	self.tmpNew = make(map[string]bool)
	return
}

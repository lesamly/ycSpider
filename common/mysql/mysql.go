package mysql

import (
	"database/sql"
	"errors"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"

	"github.com/henrylee2cn/pholcus/common/util"
	"github.com/henrylee2cn/pholcus/config"
	"github.com/henrylee2cn/pholcus/logs"
)

/************************ Mysql 输出 ***************************/
//sql转换结构体
type MyTable struct {
	tableName        string
	columnNames      [][2]string // 标题字段
	rows             [][]string  // 多行数据
	sqlCode          string
	otherSqlCode     string   //其他sql，现主要为建自定义主键  add by lyken 20160425
	selectColumn     []string //select add by lyken 20160510
	conditions       []string //where condition add by lyken 20160510
	groupBy          []string //group by add by lyken 20160510
	orderBy          []string //order by condition add by lyken 20160510
	orderSub         string   //order by condition add by lyken 20160510
	updateSets       []string //set a=b condition add by lyken 20160510
	customPrimaryKey bool
	size             int //内容大小的近似值
}

var (
	db                 *sql.DB
	err                error
	stmtChan           = make(chan bool, config.MYSQL_CONN_CAP)
	max_allowed_packet = config.MYSQL_MAX_ALLOWED_PACKET - 1024
	lock               sync.RWMutex
)

func DB() (*sql.DB, error) {
	return db, err
}

func Refresh() {
	lock.Lock()
	defer lock.Unlock()
	db, err = sql.Open("mysql", config.MYSQL_CONN_STR+"/"+config.DB_NAME+"?charset=utf8")
	if err != nil {
		logs.Log.Error("Mysql：%v\n", err)
		return
	}
	db.SetMaxOpenConns(config.MYSQL_CONN_CAP)
	db.SetMaxIdleConns(config.MYSQL_CONN_CAP)
	if err = db.Ping(); err != nil {
		logs.Log.Error("Mysql：%v\n", err)
	}
}

func New() *MyTable {
	return &MyTable{}
}

//设置表名
func (self *MyTable) SetTableName(name string) *MyTable {
	self.tableName = name
	return self
}

//设置表单列
func (self *MyTable) AddColumn(names ...string) *MyTable {
	for _, name := range names {
		name = strings.Trim(name, " ")
		idx := strings.Index(name, " ")
		self.columnNames = append(self.columnNames, [2]string{string(name[:idx]), string(name[idx+1:])})
	}
	return self
}

/**
**添加and条件[where]
**add by lyken 20160510
**/
func (self *MyTable) AddAnd(conditions ...string) *MyTable {
	for _, condition := range conditions { //conditions
		self.conditions = append(self.conditions, ` and `, condition)
	}
	return self
}

/**
**添加or条件[where]
**add by lyken 20160510
**/
func (self *MyTable) AddOr(conditions ...string) *MyTable {
	for _, condition := range conditions { //conditions
		self.conditions = append(self.conditions, ` or `, condition)
	}
	return self
}

/**
**添加group by条件[group]
**add by lyken 20160510
**/
func (self *MyTable) AddGroupBy(groups ...string) *MyTable {
	for _, group := range groups {
		self.groupBy = append(self.groupBy, group)
	}
	return self
}

/**
**添加order by条件[order]
**add by lyken 20160510
**/
func (self *MyTable) AddOrderBy(orders ...string) *MyTable {
	for _, order := range orders {
		self.orderBy = append(self.orderBy, order)
	}
	return self
}

/**
**添加setValue条件[set]
**add by lyken 20160510
**/
func (self *MyTable) AddUpdateSets(updateSets ...string) *MyTable {
	for _, updateSet := range updateSets {
		self.updateSets = append(self.updateSets, updateSet)
	}
	return self
}

/**
**添加selectColumn条件[select]
**add by lyken 20160510
**/
func (self *MyTable) AddSelectColumn(columns ...string) *MyTable {
	for _, column := range columns {
		self.selectColumn = append(self.selectColumn, column)
	}
	return self
}

/**
**添加orderSub[order]
**add by lyken 20160510
**/
func (self *MyTable) AddOrderSub(orderSub string) *MyTable {
	self.orderSub = orderSub
	return self
}

///orderSub

//设置主键的语句（可选）
func (self *MyTable) CustomPrimaryKey(primaryKeyCode string) *MyTable {
	self.AddColumn(primaryKeyCode)
	self.customPrimaryKey = true
	return self
}

//生成"创建表单"的语句，执行前须保证SetTableName()、AddColumn()已经执行
func (self *MyTable) Create() error {
	if len(self.columnNames) == 0 {
		return errors.New("Column can not be empty")
	}
	self.sqlCode = `create table if not exists ` + self.tableName + `(`
	if !self.customPrimaryKey {
		self.sqlCode += `id int(12) not null primary key auto_increment,`
	}
	for _, title := range self.columnNames {
		self.sqlCode += title[0] + ` ` + title[1] + `,`
	}
	self.sqlCode = string(self.sqlCode[:len(self.sqlCode)-1])
	self.sqlCode += self.otherSqlCode //add by lyken 20160425
	self.sqlCode += `);`

	stmtChan <- true
	defer func() {
		<-stmtChan
	}()
	lock.RLock()
	defer lock.RUnlock()
	stmt, err := db.Prepare(self.sqlCode)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	return err
}

//add by lyken 20160425
func (self *MyTable) SetCustomPrimaryKey(bol bool) *MyTable {
	self.customPrimaryKey = bol
	return self
}
func (self *MyTable) SetOtherSqlCode(otherSqlCode string) *MyTable {
	self.otherSqlCode = otherSqlCode
	return self
}

//end add by lyken 20160425

//设置插入的1行数据
func (self *MyTable) addRow(value []string) *MyTable {
	self.rows = append(self.rows, value)
	return self
}

//智能插入数据，每次1行
func (self *MyTable) AutoInsert(value []string) *MyTable {
	var nsize int
	for _, v := range value {
		nsize += len(v)
	}
	if nsize > max_allowed_packet {
		logs.Log.Error("%v", "packet for query is too large. Try adjusting the 'maxallowedpacket'variable in the 'config.ini'")
		return self
	}
	self.size += nsize
	if self.size > max_allowed_packet {
		util.CheckErr(self.FlushInsert())
		return self.AutoInsert(value)
	}
	return self.addRow(value)
}

//向sqlCode添加"插入数据"的语句，执行前须保证Create()、AutoInsert()已经执行
//insert into table1(field1,field2) values(rows[0]),(rows[1])...
func (self *MyTable) FlushInsert() error {
	if len(self.rows) == 0 {
		return nil
	}

	self.sqlCode = `insert into ` + self.tableName + `(`
	if len(self.columnNames) != 0 {
		for _, v := range self.columnNames {
			self.sqlCode += "`" + v[0] + "`,"
		}
		self.sqlCode = self.sqlCode[:len(self.sqlCode)-1] + `)values`
	}
	for _, row := range self.rows {
		self.sqlCode += `(`
		for _, v := range row {
			v = strings.Replace(v, `"`, `\"`, -1)
			//self.sqlCode += `"` + v + `",`   --change by lyken 20160520
			self.sqlCode += `'` + v + `',`
		}
		self.sqlCode = self.sqlCode[:len(self.sqlCode)-1] + `),`
	}
	self.sqlCode = self.sqlCode[:len(self.sqlCode)-1] + `;`

	stmtChan <- true
	defer func() {
		<-stmtChan
	}()
	lock.RLock()
	defer lock.RUnlock()
	defer func() {
		// 清空临时数据
		self.rows = [][]string{}
		self.size = 0
		self.sqlCode = ""
	}()
	stmt, err := db.Prepare(self.sqlCode)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	return err
}

// 获取全部数据
func (self *MyTable) SelectAll() (*sql.Rows, error) {
	if self.tableName == "" {
		return nil, errors.New("表名不能为空")
	}
	self.sqlCode = `select * from ` + self.tableName + `;`
	lock.RLock()
	defer lock.RUnlock()
	return db.Query(self.sqlCode)
}

/**
**更新数据
**add by lyken 20160510
**/
func (self *MyTable) FlushUpdate() error {
	if self.tableName == "" {
		return errors.New("表名不能为空")
	}
	self.sqlCode = `update ` + self.tableName + ` set ` //+ +`;`
	if len(self.updateSets) != 0 && len(self.conditions) != 0 {
		for _, v := range self.updateSets {
			self.sqlCode += v + `,`
		}
		self.sqlCode = self.sqlCode[:len(self.sqlCode)-1] + ` where 1=1 `

		for _, v := range self.conditions {
			self.sqlCode += v + ` `
		}
		self.sqlCode = self.sqlCode[:len(self.sqlCode)-1] + `;`
	} else {
		return errors.New("更新的内容和条件不可为空")
	}
	stmtChan <- true
	defer func() {
		<-stmtChan
	}()
	lock.RLock()
	defer lock.RUnlock()
	defer func() {
		// 清空临时数据
		self.rows = [][]string{}
		self.conditions = []string{}
		self.groupBy = []string{}
		self.orderBy = []string{}
		self.selectColumn = []string{}
		self.orderSub = ""
		self.updateSets = []string{}
		self.size = 0
		self.sqlCode = ""
		self.otherSqlCode = ""
	}()
	stmt, err := db.Prepare(self.sqlCode)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	return err
}

/**
**删除数据
**add by lyken 20160510
**/
func (self *MyTable) FlushDelete() error {
	if self.tableName == "" {
		return errors.New("表名不能为空")
	}
	self.sqlCode = `delete from ` + self.tableName + ` where 1=1 ` //+ +`;`
	if len(self.conditions) != 0 {
		for _, v := range self.conditions {
			self.sqlCode += v + ` `
		}
		self.sqlCode = self.sqlCode[:len(self.sqlCode)-1] + `;`
	} else {
		return errors.New("更新的内容和条件不可为空")
	}
	stmtChan <- true
	defer func() {
		<-stmtChan
	}()
	lock.RLock()
	defer lock.RUnlock()
	defer func() {
		// 清空临时数据
		self.rows = [][]string{}
		self.conditions = []string{}
		self.groupBy = []string{}
		self.orderBy = []string{}
		self.selectColumn = []string{}
		self.orderSub = ""
		self.updateSets = []string{}
		self.size = 0
		self.sqlCode = ""
		self.otherSqlCode = ""
	}()
	stmt, err := db.Prepare(self.sqlCode)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	return err
}

/**
**查询数据
**add by lyken 20160510
**/
func (self *MyTable) FlushSelect() error {
	if self.tableName == "" {
		return errors.New("表名不能为空")
	}
	if len(self.selectColumn) != 0 {
		for _, v := range self.selectColumn {
			self.sqlCode += v + `,`
		}
		self.sqlCode = self.sqlCode[:len(self.sqlCode)-1]
	} else {
		self.sqlCode = `select * from ` + self.tableName
	}

	if len(self.conditions) != 0 {
		self.sqlCode += ` where 1=1 `
		for _, v := range self.conditions {
			self.sqlCode += v + ` `
		}
		self.sqlCode = self.sqlCode[:len(self.sqlCode)-1]
	}

	if len(self.groupBy) != 0 {
		self.sqlCode += ` group by `
		for _, v := range self.groupBy {
			self.sqlCode += v + `,`
		}
		self.sqlCode = self.sqlCode[:len(self.sqlCode)-1]
	}

	if len(self.orderBy) != 0 {
		self.sqlCode += ` order by `
		for _, v := range self.orderBy {
			self.sqlCode += v + `,`
		}
		self.sqlCode = self.sqlCode[:len(self.sqlCode)-1] + self.orderSub
	}
	self.sqlCode += `;`
	stmtChan <- true
	defer func() {
		<-stmtChan
	}()
	lock.RLock()
	defer lock.RUnlock()
	defer func() {
		// 清空临时数据
		self.rows = [][]string{}
		self.conditions = []string{}
		self.groupBy = []string{}
		self.orderBy = []string{}
		self.selectColumn = []string{}
		self.orderSub = ""
		self.updateSets = []string{}
		self.size = 0
		self.sqlCode = ""
		self.otherSqlCode = ""
	}()
	stmt, err := db.Prepare(self.sqlCode)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	return err
}

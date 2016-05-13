package collector

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/henrylee2cn/pholcus/common/mysql"
	"github.com/henrylee2cn/pholcus/common/util"
	"github.com/henrylee2cn/pholcus/logs"
)

/************************ Mysql 输出 ***************************/

func init() {
	var (
		mysqlTable     = map[string]*mysql.MyTable{}
		mysqlTableLock sync.RWMutex
	)

	var getMysqlTable = func(name string) (*mysql.MyTable, bool) {
		mysqlTableLock.RLock()
		tab, ok := mysqlTable[name]
		mysqlTableLock.RUnlock()
		return tab, ok
	}

	var setMysqlTable = func(name string, tab *mysql.MyTable) {
		mysqlTableLock.Lock()
		mysqlTable[name] = tab
		mysqlTableLock.Unlock()
	}

	Output["mysql"] = func(self *Collector, dataIndex int) error {
		_, err := mysql.DB()
		if err != nil {
			return fmt.Errorf("Mysql数据库链接失败: %v", err)
		}
		var (
			mysqls    = make(map[string]*mysql.MyTable)
			namespace = util.FileNameReplace(self.namespace())
		)
		for _, datacell := range self.DockerQueue.Dockers[dataIndex] {
			var tName = namespace
			subNamespace := util.FileNameReplace(self.subNamespace(datacell))
			if subNamespace != "" {
				//tName += "__" + subNamespace
				//add by lyken 20160422
				if len(tName) > 0 {
					tName += "__" + subNamespace
				} else {
					tName += subNamespace
				}
			}
			table, ok := mysqls[tName]
			if !ok {
				table, ok = getMysqlTable(tName)
				if ok {
					mysqls[tName] = table
				} else {
					table = mysql.New()
					table.SetTableName(tName)
					//add by lyken 20160425  otherSqlCode
					rule := self.MustGetRule(datacell["RuleName"].(string))
					CustomPrimaryKey := rule.PrimaryKeys //add param PrimaryKeys
					primaryKeys := ""

					//for _, title := range self.MustGetRule(datacell["RuleName"].(string)).ItemFields {
					//change by lyken 20160425
					for _, title := range rule.ItemFields {
						//-----add by lyken 20160425
						//_updateSet：此字段存在，则判断此数据为更新数据，此字段用在更新语句中的set语句 分隔符","
						//_updateWhere：此字段存在，则判断此数据为更新数据，此字段用在更新语句中的where语句 分隔符","
						if title == "_updateSet" || title == "_updateWhere" {
							continue
						}

						v, ok := CustomPrimaryKey[title]
						if ok {
							if len(v) > 0 {
								table.AddColumn(title + ` ` + v)
							} else {
								table.AddColumn(title + ` int(12) not null`)
							}
							primaryKeys += "," + title
							continue
						}
						//--------end add

						table.AddColumn(title + ` MEDIUMTEXT`)
					}
					if self.Spider.OutDefaultField() {
						table.AddColumn(`Url VARCHAR(255)`, `ParentUrl VARCHAR(255)`, `DownloadTime VARCHAR(50)`)
					}
					//-----add by lyken 20160425
					if len(primaryKeys) > 0 {
						table.SetCustomPrimaryKey(true)
						table.SetOtherSqlCode(", primary key(" + primaryKeys[1:] + ")")
					}
					//--------end add

					if err := table.Create(); err != nil {
						logs.Log.Error("%v", err)
						continue
					} else {
						setMysqlTable(tName, table)
						mysqls[tName] = table
					}
				}
			}
			data := []string{}
			isUP := false
			for _, title := range self.MustGetRule(datacell["RuleName"].(string)).ItemFields {
				vd := datacell["Data"].(map[string]interface{})
				//----add by lyken 20160510
				//_updateSet：此字段存在，则判断此数据为更新数据，此字段用在更新语句中的set语句 分隔符","
				//_updateWhere：此字段存在，则判断此数据为更新数据，此字段用在更新语句中的where语句 分隔符","
				if title == "_updateSet" {
					if v, ok := vd[title].(string); ok || vd[title] == nil {
						for _, val := range strings.Split(v, ",") {
							var buffer bytes.Buffer
							val = strings.Trim(val, " ")
							if len(val) > 0 {
								_val, ok := vd[val]
								if ok {
									buffer.WriteString(val)
									buffer.WriteString(`='`)
									buffer.WriteString(_val.(string))
									buffer.WriteString(`'`)
									table.AddUpdateSets(buffer.String())
								}
								isUP = true
							}
						}
					}
					continue
				} else if title == "_updateWhere" {
					if v, ok := vd[title].(string); ok || vd[title] == nil {
						for _, val := range strings.Split(v, ",") {
							var buffer bytes.Buffer
							val = strings.Trim(val, " ")
							if len(val) > 0 {
								_val, ok := vd[val]
								if ok {
									buffer.WriteString(val)
									buffer.WriteString(`='`)
									buffer.WriteString(_val.(string))
									buffer.WriteString(`'`)
									table.AddAnd(buffer.String())
								}
								isUP = true
							}
						}
					}
					continue
				}
				//----end add

				if v, ok := vd[title].(string); ok || vd[title] == nil {
					data = append(data, v)
				} else {
					data = append(data, util.JsonString(vd[title]))
				}
			}
			//--------change by lyken 20160510 start
			if isUP {
				table.FlushUpdate()
			} else {
				//----old source start
				if self.Spider.OutDefaultField() {
					data = append(data, datacell["Url"].(string), datacell["ParentUrl"].(string), datacell["DownloadTime"].(string))
				}
				table.AutoInsert(data)
				//----old source end
			}
			//---------end add
		}
		for _, tab := range mysqls {
			util.CheckErr(tab.FlushInsert())
		}
		return nil
	}
}

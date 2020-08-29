package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
)

//初始化
func init() {
	initConfig()
	initConnect()
}

// 数据库参数
type database struct {
	host     string
	port     int
	db       string
	username string
	password string
}

var (
	newDbConfig, oldDbConfig database
	newConn, oldConn         *sql.DB
)

var (
	//数据列
	SQLTableColumn = "SELECT `COLUMN_NAME` FROM `information_schema`.`columns` WHERE `table_schema`=? AND `table_name`=? ORDER BY `ORDINAL_POSITION` ASC"
)

//初始化配置文件
func initConfig() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Println("no such config file")
		} else {
			log.Println("read config err")
		}
		log.Fatal(err)
	}

	newDbConfig.host = viper.GetString(`database.new.host`)
	newDbConfig.port = viper.GetInt(`database.new.port`)
	newDbConfig.db = viper.GetString(`database.new.db`)
	newDbConfig.username = viper.GetString(`database.new.username`)
	newDbConfig.password = viper.GetString(`database.new.password`)

	oldDbConfig.host = viper.GetString(`database.old.host`)
	oldDbConfig.port = viper.GetInt(`database.old.port`)
	oldDbConfig.db = viper.GetString(`database.old.db`)
	oldDbConfig.username = viper.GetString(`database.old.username`)
	oldDbConfig.password = viper.GetString(`database.old.password`)
}

//初始化数据库连接
func initConnect() {
	newConn, _ = connect(&newDbConfig)
	oldConn, _ = connect(&oldDbConfig)
}

// 数据库连接
func connect(dbConfig *database) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", dbConfig.username, dbConfig.password, dbConfig.host, dbConfig.port, dbConfig.db, "utf8")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("mysql connect failed, detail is [%v]", err)
	}
	return db, err
}

//获取表中所有列
func queryTableColumn(db *sql.DB, dbName string, table string) ([]string, error) {
	var columns []string
	rows, err := db.Query(SQLTableColumn, dbName, table)
	if err != nil {
		fmt.Printf("execute query table column action error, detail is [%v] \n", err)
		return columns, err
	}

	for rows.Next() {
		var column string
		err = rows.Scan(&column)
		if err != nil {
			fmt.Printf("query table column scan error, detail is [%v]\n", err.Error())
			return columns, err
		}
		columns = append(columns, column)
	}
	return columns, err
}

//迁移一张表的数据
func migrateData(fromDb *sql.DB, fromTable string, toDb *sql.DB, toTable string) (int, error) {
	rows, err := fromDb.Query("SELECT * FROM " + fromTable)
	if err != nil {
		panic(err.Error())
	}

	columns, err := rows.Columns()
	if err != nil {
		panic(err.Error())
	}

	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))

	for i := range values {
		scanArgs[i] = &values[i]
	}

	placeHolders := "("
	columnsString := "("
	for i := 0; i < len(columns); i++ {
		if i == (len(columns) - 1) {
			placeHolders += "?)"
			columnsString += "`" + columns[i] + "`)"
			break
		}
		placeHolders += "?,"
		columnsString += "`" + columns[i] + "`,"
	}

	bulkValues := []interface{}{}

	valueStrings := make([]string, 0)

	record := make([]interface{}, len(columns))
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}
		for i, col := range values {
			if col == nil {
				record[i] = col
			} else {
				record[i] = string(col)
			}
		}
		valueStrings = append(valueStrings, placeHolders)
		bulkValues = append(bulkValues, record...)
	}

	count := len(valueStrings)
	if count > 0 {
		stmStr := fmt.Sprintf("INSERT INTO "+toTable+"%s VALUES %s", columnsString, strings.Join(valueStrings, ","))
		_, err = toDb.Exec(stmStr, bulkValues...)
		if err != nil {
			fmt.Println("错误来自：" + toTable)
			panic(err.Error())
		}
	}
	fmt.Printf("迁移表 [%s] 成功，操作%d条数据 \n", fromTable, count)
	return count, nil
}

//填充订单表数据
func fillOrderTable() {
	type fillOrderModel struct {
		ID             int
		UserID         int
		BorrowID       int
		Type           string
		BorrowRealname string
		ClassName      string
		LabName        string
		LabCode        string
		EquipName      string
		EquipModel     string
	}
	var fillOrderModels []fillOrderModel
	rows, err := newConn.Query("SELECT id, userid, borrow_id, type FROM lims_order")
	if err != nil {
		fmt.Printf("查询订单列表失败：%v \n", err.Error())
	}

	for rows.Next() {
		var order fillOrderModel
		err = rows.Scan(
			&order.ID,
			&order.UserID,
			&order.BorrowID,
			&order.Type)
		if err != nil {
			fmt.Printf("读取订单记录失败：%v \n", err.Error())
		}
		fillOrderModels = append(fillOrderModels, order)
	}
	//获取填充数据
	for i, currOrder := range fillOrderModels {
		//需优化，这里后面通过IN，一次性查出所有。
		err = newConn.QueryRow("SELECT realname, className FROM lims_user WHERE id=?", currOrder.UserID).Scan(&fillOrderModels[i].BorrowRealname, &fillOrderModels[i].ClassName)
		if err != nil {
			fmt.Printf("读取用户信息失败：%v \n", err.Error())
		}
		if currOrder.Type == "lab" {
			err = newConn.QueryRow(
				"SELECT lab.name, lab.code FROM lims_flow_bind fb LEFT JOIN lims_lab lab ON fb.bindid=lab.id WHERE fb.id=? AND fb.type='lab'",
				currOrder.BorrowID).Scan(&fillOrderModels[i].LabName, &fillOrderModels[i].LabCode)
		} else {
			err = newConn.QueryRow(
				"SELECT l.name,l.code,e.name,e.modelname FROM lims_flow_bind fb LEFT JOIN lims_equipments e ON fb.bindid=e.id LEFT JOIN lims_lab l ON e.lab_id=l.id WHERE fb.type='equipments' AND fb.id=?",
				currOrder.BorrowID).Scan(&fillOrderModels[i].LabName, &fillOrderModels[i].LabCode, &fillOrderModels[i].EquipName, &fillOrderModels[i].EquipModel)
		}
	}

	//单条记录慢慢更新
	for _, currOrder := range fillOrderModels {
		_, err := newConn.Exec(
			"UPDATE lims_order SET borrow_realname=?,class_name=?,lab_name=?,lab_code=?,equip_name=?,equip_model=? WHERE id=?",
			currOrder.BorrowRealname, currOrder.ClassName, currOrder.LabName, currOrder.LabCode, currOrder.EquipName, currOrder.EquipModel, currOrder.ID)
		if err != nil {
			fmt.Println("错误来自ID为：" + string(currOrder.ID))
			panic(err.Error())
		}
	}
	fmt.Println("订单数据填充完毕")
}

func replaceFlowID(newID int, oldID []int) {
	oldIDString := make([]string, len(oldID))
	for i, v := range oldID {
		oldIDString[i] = strconv.Itoa(v)
	}
	_, err := newConn.Exec(
		"UPDATE lims_flow_bind_data SET flowid=? WHERE flowid IN (?);",
		newID, strings.Join(oldIDString, ","))
	if err != nil {
		fmt.Printf("替换flow_bind_data流程id错误: %v", err.Error())
		panic(err.Error())
	}

	_, orderErr := newConn.Exec(
		"UPDATE lims_order SET flow_id=? WHERE flow_id IN (?);",
		newID, strings.Join(oldIDString, ","))
	if orderErr != nil {
		fmt.Printf("替换order流程id错误: %v", orderErr.Error())
		panic(err.Error())
	}
	fmt.Println("替换流程id完毕")
}

//迁移相同结构表
func sameTableMigrate() {
	prefix := "lims_"
	tables := []string{
		"user",
		"lab",
		"equipments",
		"equipment_child",
		"flow_bind",
		"flow_bind_data",
		// "basesetting",
		"blacklist",
		"blacklist_log",
		"filepath",
		"flow",
		"flow_audit",
		"flow_bind_equipmentdata",
		"flow_bind_time",
		"flow_node",
		"lab_safe_check",
		"labs",
		"message",
		"order",
		"order_equipment",
		"order_log",
		"record_data",
	}

	for _, table := range tables {
		migrateData(oldConn, prefix+table, newConn, prefix+table)
	}
	fmt.Println("旧数据迁移完毕")
}

func deleteAll() {
	deleteAll := []string{
		"delete from lims_lab;",
		"delete from lims_user;",
		"delete from lims_equipment_child;",
		"delete from lims_equipments;",
		"delete from lims_flow_bind;",
		"delete from lims_flow_bind_data;",
		"delete from lims_basesetting;",
		"delete from lims_blacklist;",
		"delete from lims_blacklist_log;",
		"delete from lims_filepath;",
		"delete from lims_flow;",
		"delete from lims_flow_bind_equipmentdata;",
		"delete from lims_flow_bind_time;",
		"delete from lims_flow_node;",
		"delete from lims_lab_safe_check;",
		"delete from lims_labs;",
		"delete from lims_message;",
		"delete from lims_order;",
		"delete from lims_order_equipment;",
		"delete from lims_order_log;",
		"delete from lims_record_data;",
	}
	for _, sql := range deleteAll {
		_, err := newConn.Query(sql)
		if err != nil {
			fmt.Printf("全部删除出现错误: %v \n", err.Error())
		}
	}
	fmt.Println("删除完毕")
}

func main() {
	// sameTableMigrate()
	// fillOrderTable()
	// deleteAll()
	old := []int{54}
	replaceFlowID(1, old)
}

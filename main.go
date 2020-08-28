package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

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
			panic(err.Error())
		}
	}
	fmt.Printf("迁移表 [%s -> %s] 成功，操作%d条数据 \n", fromTable, toTable, count)
	return count, nil
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
		"basesetting",
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
	}

	for _, table := range tables {
		migrateData(oldConn, prefix+table, newConn, prefix+table)
	}
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
	}
	for _, sql := range deleteAll {
		_, err := newConn.Query(sql)
		if err != nil {
			fmt.Printf("全部删除出现错误: %v", err.Error())
		}
	}
}

func main() {
	deleteAll()
}

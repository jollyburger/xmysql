package xmysql

import (
	"database/sql"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

/*
 * New Feature
 * 1. Read/Write Splitting
 * 2. Master/Backup DB, Multi Backup DB with weights
 * TODO:
 * 3. Write Op Verification
 * 4. More Operation
 * 5. Force Master
 */
type DbConf struct {
	address        string
	weight         int //in configuration
	current_weight int //in runtime
}

type MysqlConn struct {
	//master
	master_addr    string
	masterInstance *sql.DB
	//backup
	backup_addr     []DbConf
	backupInstances map[string]*sql.DB
	total_weight    int
}

func initInstance(connStr string) (*sql.DB, error) {
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func RegisterMysql(master_addr string, backup_addr string) (*MysqlConn, error) {
	mysql_conn := new(MysqlConn)
	master_db_instance, err = initInstance(master_addr)
	if err != nil {
		return nil, err
	}
	mysql_conn.masterInstance = master_db_instance
	backup_fields := strings.Split(backup_addr, ";")
	for _, value := range backup_fields {
		var (
			backup_addr string
			weight      int
		)
		if len(strings.Split(value, "|")) == 2 {
			weight, _ = strconv.Atoi(strings.Split(value, "|")[1])
		}
		backup_addr = strings.Split(value, "|")[0]
		backup_db_instance, err := initInstance(backup_addr)
		if err != nil {
			return nil, err
		}
		mysql_conn.backupInstances[value] = backup_db_instance
		var db_conf DbConf
		db_conf.address = backup_addr
		db_conf.weight = weight
		total_weight += weight
		mysql_conn.backup_addr = append(mysql_conn.backup_addr, db_conf)
	}
	return mysql_conn, nil
}

func (c *MysqlConn) Insert(sql string, args ...interface{}) (lastInsertId int64, err error) {
	res, err := c.masterInstance.Exec(sql, args...)
	if err != nil {
		return
	}
	return res.LastInsertId()
}

func (c *MysqlConn) Update(sql string, args ...interface{}) (rowsAffected int64, err error) {
	res, err := c.masterInstance.Exec(sql, args...)
	if err != nil {
		return
	}
	return res.RowsAffected()
}

func (c *MysqlConn) Delete(sql string, args ...interface{}) (rowsAffected int64, err error) {
	res, err := c.masterInstance.Exec(sql, args...)
	if err != nil {
		return
	}
	return res.RowsAffected()
}

func (c *MysqlConn) chooseBackup() *sql.DB {
	var (
		chosen_db          *sql.DB
		index              int
		max_current_weight int
	)
	for i, bk_db := range c.backup_addr {
		bk_db.current_weight += bk_db.weight
		if bk_db.current_weight > max_current_weight {
			index = i
			max_current_weight = bk_db.current_weight
		}
	}
	name := c.backup_addr[i].address + "|" + strconv.Itoa(c.backup_addr[i].weight)
	c.backup_addr[i].current_weight -= c.total_weight
	chosen_db = c.backupInstances[name]
	return chosen_db
}

func (c *MysqlConn) Select(sql string, args ...interface{}) (results []map[string]string, err error) {
	var db_instance *sql.DB
	if len(c.backup_addr) == 0 {
		db_instance = c.masterInstance
	} else {
		db_instance = c.chooseBackup()
	}
	rows, err := c.db.Query(sql, args...)
	if err != nil {
		return
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}
	results = make([]map[string]string, 0)
	for rows.Next() {
		if err = rows.Scan(scans...); err != nil {
			return
		}
		row := make(map[string]string)
		for k, v := range values {
			key := columns[k]
			row[key] = string(v)
		}
		results = append(results, row)
	}
	return
}

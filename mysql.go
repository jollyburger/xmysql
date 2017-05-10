package xmysql

import (
	"errors"
	"sync"
)

type MysqlProxy struct {
	mux           sync.RWMutex
	mysqlConnPool map[string]*MysqlConn
}

var (
	GMysqlProxy *MysqlProxy
)

/*
 *  master_addr: user:passwd@tcp(address:port)/dbtabase
 *  backup_addr: user1:passwd1@tcp(address1:port1)/db1|weight1;user2:passwd2@tcp(address2:port2)/db2|weight
 */
func RegisterMysqlService(service string, master_addr, backup_addr string) error {
	mysql_conn, err := RegisterMysql(master_addr, backup_addr)
	if err != nil {
		return err
	}
	GMysqlProxy.mux.Lock()
	GMysqlProxy.mysqlConnPool[service] = mysql_conn
	GMysqlProxy.mux.Unlock()
	return nil
}

func Insert(service string, sql string, args ...interface{}) (lastInsertId int64, err error) {
	GMysqlProxy.RLock()
	defer GMysqlProxy.Unlock()
	if conn, ok := GMysqlProxy.mysqlConnPool[service]; ok {
		return conn.Insert(sql, args)
	}
	err = errors.New("not found db instance")
	return
}

func Update(service string, sql string, args ...interface{}) (rowsAffected int64, err error) {
	GMysqlProxy.RLock()
	defer GMysqlProxy.Unlock()
	if conn, ok := GMysqlProxy.mysqlConnPool[service]; ok {
		return conn.Update(sql, args)
	}
	err = errors.New("not found db instance")
	return

}

func Delete(service string, sql string, args ...interface{}) (rowsAffected int64, err error) {
	GMysqlProxy.RLock()
	defer GMysqlProxy.Unlock()
	if conn, ok := GMysqlProxy.mysqlConnPool[service]; ok {
		return conn.Delete(sql, args)
	}
	err = errors.New("not found db instance")
	return

}

func Select(service string, sql string, args ...interface{}) (result []map[string]string, err error) {
	GMysqlProxy.RLock()
	defer GMysqlProxy.Unlock()
	if conn, ok := GMysqlProxy.mysqlConnPool[service]; ok {
		return conn.Select(sql, args)
	}
	err = errors.New("not found db instance")
	return
}

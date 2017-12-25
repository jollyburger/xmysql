package xmysql

import (
	"errors"
	"reflect"
	"strconv"
)

var (
	MYSQL_TAG = "mysql"
)

func convertType(ft reflect.StructField, fv reflect.Value, value string) error {
	var v interface{}
	switch ft.Type.Kind() {
	case reflect.Int, reflect.Int16, reflect.Int32:
		tmp_v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		v = tmp_v
	case reflect.Int64:
		tmp_v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		v = tmp_v
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		tmp_v, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return err
		}
		v = tmp_v
	case reflect.Float32, reflect.Float64:
		tmp_v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		v = tmp_v
	case reflect.String:
		v = value
	case reflect.Bool:
		v = (value == "1")
	}
	fv.Set(reflect.ValueOf(v))
	return nil
}

func mapToStruct(result map[string]string, output interface{}) error {
	var (
		err error
	)
	// output is ptr to the struct
	ov := reflect.Indirect(reflect.ValueOf(output))
	ot := ov.Type()
	if ot.Kind() != reflect.Struct {
		return errors.New("output is not struct type")
	}
	for i := 0; i < ot.NumField(); i++ {
		ft := ot.Field(i)
		fv := ov.Field(i)
		if _, ok := ft.Tag.Lookup(MYSQL_TAG); !ok {
			continue
		}
		tag_name := ft.Tag.Get(MYSQL_TAG)
		if _, ok := result[tag_name]; !ok {
			continue
		}
		v := result[tag_name]
		err = convertType(ft, fv, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func Find(service string, output interface{}, sql string, args ...interface{}) error {
	var (
		result = make([]map[string]string, 0)
		err    error
	)
	GMysqlProxy.mux.RLock()
	defer GMysqlProxy.mux.RUnlock()
	if conn, ok := GMysqlProxy.mysqlConnPool[service]; ok {
		result, err = conn.Select(sql, args...)
	} else {
		err = errors.New("not found db instance")
	}
	if err != nil {
		return err
	}
	if len(result) == 0 {
		return nil
	}
	err = mapToStruct(result[0], output)
	if err != nil {
		return err
	}
	return nil
}

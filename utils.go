package xmysql

import (
	"errors"
	"strconv"
)

func checkCount(result []map[string]string) (count int, err error) {
	if len(result) == 0 {
		err = errors.New("result is empty")
		return
	}
	count, err = strconv.Atoi(result[0]["count(*)"])
	return
}

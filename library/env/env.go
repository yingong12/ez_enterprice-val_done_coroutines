package env

import (
	"os"
	"strconv"
)

func GetIntVal(key string) (res int) {
	iv := os.Getenv(key)
	res, _ = strconv.Atoi(iv)
	return
}

func GetStringVal(key string) (res string) {
	res = os.Getenv(key)
	return
}

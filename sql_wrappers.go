package gorp_queries

import (
	"fmt"
)

type functionWrapper struct {
	actualValue interface{}
	functionName string
}

func (wrapper functionWrapper) ActualValue() interface{} {
	return wrapper.actualValue
}

func (wrapper functionWrapper) WriteSql(sqlValue string) string {
	return fmt.Sprintf("%s(%s)", wrapper.functionName, sqlValue)
}

func Lower(value interface{}) functionWrapper {
	return functionWrapper{
		actualValue: value,
		functionName: "lower",
	}
}

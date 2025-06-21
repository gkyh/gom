package gom

import (
	"database/sql"
	"reflect"
	"strings"
	"sync"
)

// ///////////////////////单行数据映射Struct////////////////////////////
func RowToStruct(rows *sql.Rows, out interface{}) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	if !rows.Next() {
		return sql.ErrNoRows
	}

	eleType := reflect.TypeOf(out).Elem()
	fieldMap := getFieldMap(eleType)
	element := reflect.ValueOf(out).Elem()

	scanVals := make([]interface{}, len(cols))
	scanDests := make([]interface{}, len(cols))
	for i := range scanVals {
		scanDests[i] = &scanVals[i]
	}

	if err := rows.Scan(scanDests...); err != nil {
		return err
	}

	for i, col := range cols {
		idx, ok := fieldMap[strings.ToLower(col)]
		if !ok {
			continue
		}
		field := element.FieldByIndex(idx.Index)
		val := reflect.ValueOf(scanVals[i])
		if val.Kind() == reflect.Ptr && val.IsNil() {
			continue
		}
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
		if val.Type().ConvertibleTo(field.Type()) {
			field.Set(val.Convert(field.Type()))
		}
	}

	return nil
}

func RowsToMap(rows *sql.Rows) (map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		return nil, sql.ErrNoRows
	}

	scanVals := make([]interface{}, len(columns))
	scanDests := make([]interface{}, len(columns))
	for i := range scanVals {
		scanDests[i] = &scanVals[i]
	}

	if err := rows.Scan(scanDests...); err != nil {
		return nil, err
	}

	rowMap := make(map[string]interface{})
	for i, col := range columns {
		val := reflect.ValueOf(scanVals[i])
		if val.Kind() == reflect.Ptr && val.IsNil() {
			rowMap[col] = nil
			continue
		}
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
		rowMap[col] = val.Interface()
	}
	return rowMap, nil
}

func RowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		scanVals := make([]interface{}, len(columns))
		scanDests := make([]interface{}, len(columns))
		for i := range scanVals {
			scanDests[i] = &scanVals[i]
		}

		if err := rows.Scan(scanDests...); err != nil {
			return nil, err
		}

		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := reflect.ValueOf(scanVals[i])
			if val.Kind() == reflect.Ptr && val.IsNil() {
				rowMap[col] = nil
				continue
			}
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			rowMap[col] = val.Interface()
		}
		results = append(results, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// /////////////////////查询结果映射到struct/////////////////////////
var fieldCache sync.Map // map[reflect.Type]map[string]int

type fieldIndex struct {
	Index []int // 适配嵌套字段
	Type  reflect.Type
}

func getFieldMap(t reflect.Type) map[string]fieldIndex {
	if v, ok := fieldCache.Load(t); ok {
		return v.(map[string]fieldIndex)
	}
	result := make(map[string]fieldIndex)
	collectFields(t, nil, result)
	fieldCache.Store(t, result)
	return result
}

// collectFields 支持嵌套匿名字段的递归收集
func collectFields(t reflect.Type, parent []int, out map[string]fieldIndex) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		idx := append([]int{}, parent...)
		idx = append(idx, i)

		if f.Anonymous {
			ft := f.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				collectFields(ft, idx, out)
				continue
			}
		}

		tag := f.Tag.Get("db")
		if tag == "" {
			tag = f.Tag.Get("json")
		}
		if tag == "" {
			tag = f.Name
		}
		out[strings.ToLower(tag)] = fieldIndex{Index: idx, Type: f.Type}
	}
}

func RowsToList(rows *sql.Rows, out interface{}) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	sliceValue := reflect.ValueOf(out).Elem()
	eleType := sliceValue.Type().Elem()
	fieldMap := getFieldMap(eleType)

	for rows.Next() {
		element := reflect.New(eleType).Elem()
		scanDest := make([]interface{}, len(columns))
		scanVals := make([]interface{}, len(columns))
		for i := range scanVals {
			scanDest[i] = &scanVals[i]
		}
		if err := rows.Scan(scanDest...); err != nil {
			return err
		}

		for i, col := range columns {
			idx, ok := fieldMap[strings.ToLower(col)]
			if !ok {
				continue
			}
			field := element.FieldByIndex(idx.Index)
			val := reflect.ValueOf(scanVals[i])
			if val.Kind() == reflect.Ptr && val.IsNil() {
				continue
			}
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			if val.Type().ConvertibleTo(field.Type()) {
				field.Set(val.Convert(field.Type()))
			}
		}
		sliceValue.Set(reflect.Append(sliceValue, element))
	}
	return rows.Err()
}


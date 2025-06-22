package gom

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

func getTable(class interface{}) string {

	var table string
	se := reflect.TypeOf(class).String()
	//se := fmt.Sprintf("%v", ts)

	idx := strings.LastIndex(se, ".")
	if idx > 0 {

		idx++
		ss := string([]rune(se)[idx:len(se)])
		table = strings.ToLower(ss)
	} else {
		table = se
	}

	return prefix + table
}

// arr 为struct Slice 或 strcut Slice 指针
func (m *ConDB) StructModel(arr interface{}) *ConDB {

	t := reflect.TypeOf(arr)

	// 判断 arr 是否指针
	if t.Kind() != reflect.Ptr {

		// 确保 arr 是切片
		if t.Kind() == reflect.Slice {

			// 获取切片的元素类型
			elemType := t.Elem()
			if elemType.Kind() == reflect.Struct {
				//fmt.Println("Struct Name:", elemType.Name())
				return m.Table(prefix + strings.ToLower(elemType.Name()))
			} else {
				fmt.Println("Not a struct slice")
			}
		}
		return m

	}

	// 解除指针，获取实际的切片类型
	elemType := t.Elem()
	if elemType.Kind() == reflect.Slice {

		// 获取切片的元素类型
		structType := elemType.Elem()
		if structType.Kind() == reflect.Struct {
			//fmt.Println("Struct Name:", structType.Name())
			return m.Table(prefix + strings.ToLower(structType.Name()))
		} else {
			fmt.Println("Not a struct slice")
		}
	}

	return m

}

// ///////////////////////单行数据映射Struct////////////////////////////
func RowToStruct(row *sql.Row, out interface{}) error {
	cols := getFieldMap(reflect.TypeOf(out).Elem())
	dest := make([]interface{}, len(cols))
	scan := make([]interface{}, len(cols))
	for i := range scan {
		scan[i] = &dest[i]
	}

	if err := row.Scan(scan...); err != nil {
		return err
	}

	element := reflect.ValueOf(out).Elem()
	for _, idx := range cols {
		field := element.FieldByIndex(idx.Index)
		raw := dest[idx.Index[0]]

		if raw == nil {
			continue
		}

		fieldType := field.Type()

		if fieldType == reflect.TypeOf(time.Time{}) {
			switch v := raw.(type) {
			case time.Time:
				field.Set(reflect.ValueOf(v))
			case []byte:
				if t, err := parseTime(string(v)); err == nil {
					field.Set(reflect.ValueOf(t))
				}
			case string:
				if t, err := parseTime(v); err == nil {
					field.Set(reflect.ValueOf(t))
				}
			}
			continue
		}

		val := reflect.ValueOf(raw)
		if val.Kind() == reflect.Ptr && val.IsNil() {
			continue
		}
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
		if val.IsValid() && val.Type().ConvertibleTo(fieldType) {
			field.Set(val.Convert(fieldType))
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
		val := scanVals[i]

		switch v := val.(type) {
		case nil:
			rowMap[col] = nil
		case []byte:
			s := string(v)
			if t, err := parseTime(s); err == nil {
				rowMap[col] = t
			} else {
				rowMap[col] = s
			}
		default:
			rowMap[col] = v
		}
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
			val := scanVals[i]
			switch v := val.(type) {
			case nil:
				rowMap[col] = nil
			case []byte:
				s := string(v)
				if t, err := parseTime(s); err == nil {
					rowMap[col] = t
				} else {
					rowMap[col] = s
				}
			default:
				rowMap[col] = v
			}
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
		scanVals := make([]interface{}, len(columns))
		scanDest := make([]interface{}, len(columns))
		for i := range scanVals {
			scanDest[i] = &scanVals[i]
		}

		if err := rows.Scan(scanDest...); err != nil {
			return err
		}

		for i, col := range columns {
			raw := scanVals[i]
			if raw == nil {
				continue
			}

			idx, ok := fieldMap[strings.ToLower(col)]
			if !ok {
				continue
			}

			field := element.FieldByIndex(idx.Index)
			fieldType := field.Type()

			// 时间类型特殊处理
			if fieldType == reflect.TypeOf(time.Time{}) {
				switch v := raw.(type) {
				case time.Time:
					field.Set(reflect.ValueOf(v))
				case []byte:
					if t, err := parseTime(string(v)); err == nil {
						field.Set(reflect.ValueOf(t))
					}
				case string:
					if t, err := parseTime(v); err == nil {
						field.Set(reflect.ValueOf(t))
					}
				}
				continue
			}

			// 通用处理
			val := reflect.ValueOf(raw)
			if val.Kind() == reflect.Ptr && val.IsNil() {
				continue
			}
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			if val.IsValid() && val.Type().ConvertibleTo(fieldType) {
				field.Set(val.Convert(fieldType))
			}
		}

		sliceValue.Set(reflect.Append(sliceValue, element))
	}

	return rows.Err()
}

func parseTime(s string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
		"2006-01-02 15:04:05.999999",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time format: %s", s)
}

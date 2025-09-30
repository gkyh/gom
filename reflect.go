package gom

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strconv"
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
		raw := scanVals[i]
		if raw == nil {
			continue
		}

		idx, ok := fieldMap[strings.ToLower(col)]
		if !ok {
			continue
		}

		field := element.FieldByIndex(idx.Index)
		if val, ok := ConvertValue(raw, field.Type(), idx.Tag); ok {
			field.Set(val)
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
		rowMap[col] = ConvertValueAuto(scanVals[i])
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
			rowMap[col] = ConvertValueAuto(scanVals[i])
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
	Tag   reflect.StructTag
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

		if f.Tag.Get("ignore") == "true" {
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
				fmt.Println(ft.String())
				collectFields(ft, idx, out)
				continue
			}
		}

		tag := f.Tag.Get("db")
		if tag == "" {
			tag = f.Tag.Get("gom")
		}
		if tag == "" {
			tag = f.Name
		}
		out[strings.ToLower(tag)] = fieldIndex{Index: idx, Type: f.Type, Tag: f.Tag}
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
			if val, ok := ConvertValue(raw, field.Type(), idx.Tag); ok {
				field.Set(val)
			}

		}

		sliceValue.Set(reflect.Append(sliceValue, element))
	}

	return rows.Err()
}
func ConvertValue(raw interface{}, targetType reflect.Type, tag reflect.StructTag) (reflect.Value, bool) {
	if raw == nil {
		return reflect.Zero(targetType), false
	}

	// 处理 time.Time
	if targetType == reflect.TypeOf(time.Time{}) {
		switch v := raw.(type) {
		case time.Time:
			return reflect.ValueOf(v), true
		case []byte:
			if t, err := parseTime(string(v)); err == nil {
				return reflect.ValueOf(t), true
			}
		case string:
			if t, err := parseTime(v); err == nil {
				return reflect.ValueOf(t), true
			}
		}
		return reflect.Zero(targetType), false
	}

	// 处理 float64 (如 DECIMAL)
	if tag.Get("type") == "decimal" {
		switch v := raw.(type) {
		case []byte:
			return reflect.ValueOf(string(v)), true
		case string:
			return reflect.ValueOf(v), true
		}
	}
	if targetType.Kind() == reflect.Float64 {
		switch v := raw.(type) {
		case float64:
			return reflect.ValueOf(v), true
		case []byte:
			if f, err := strconv.ParseFloat(string(v), 64); err == nil {
				return reflect.ValueOf(f), true
			}
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return reflect.ValueOf(f), true
			}
		}
		return reflect.Zero(targetType), false
	}

	// 处理 int64（如 BIGINT）
	if targetType.Kind() == reflect.Int64 {
		switch v := raw.(type) {
		case uint64:
			if v <= math.MaxInt64 {
				return reflect.ValueOf(int64(v)), true
			}
		case int64:
			return reflect.ValueOf(v), true
		case []byte:
			if i, err := strconv.ParseInt(string(v), 10, 64); err == nil {
				return reflect.ValueOf(i), true
			}
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return reflect.ValueOf(i), true
			}
		}
		return reflect.Zero(targetType), false
	}

	// 处理 bool（BOOLEAN, TINYINT）
	if targetType.Kind() == reflect.Bool {
		switch v := raw.(type) {
		case bool:
			return reflect.ValueOf(v), true
		case int64:
			return reflect.ValueOf(v != 0), true
		case []byte:
			s := string(v)
			if s == "1" || strings.ToLower(s) == "true" {
				return reflect.ValueOf(true), true
			} else if s == "0" || strings.ToLower(s) == "false" {
				return reflect.ValueOf(false), true
			}
		case string:
			if v == "1" || strings.ToLower(v) == "true" {
				return reflect.ValueOf(true), true
			} else if v == "0" || strings.ToLower(v) == "false" {
				return reflect.ValueOf(false), true
			}
		}
		return reflect.Zero(targetType), false
	}

	// 处理 string（CHAR, VARCHAR, TEXT）
	if targetType.Kind() == reflect.String {
		switch v := raw.(type) {
		case string:
			return reflect.ValueOf(v), true
		case []byte:
			return reflect.ValueOf(string(v)), true
		}
	}

	// 默认处理：如果可以转换
	val := reflect.ValueOf(raw)
	if val.Kind() == reflect.Ptr && val.IsNil() {
		return reflect.Zero(targetType), false
	}
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.IsValid() && val.Type().ConvertibleTo(targetType) {
		return val.Convert(targetType), true
	}

	return reflect.Zero(targetType), false
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

func ConvertValueAuto(raw interface{}) interface{} {
	switch v := raw.(type) {
	case nil:
		return nil
	case []byte:
		s := string(v)

		// bool
		if s == "0" || s == "1" {
			return s == "1"
		}

		// float64
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}

		// time.Time
		if t, err := parseTime(s); err == nil {
			return t
		}

		// fallback
		return s

	case string:
		// optional: same logic for string
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		if t, err := parseTime(v); err == nil {
			return t
		}
		return v

	default:
		return v
	}
}

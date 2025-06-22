package gom

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// =================== ConDB 插入方法（安全优化） ===================

func (m *ConDB) Insert(i interface{}) error {
	var db *ConDB
	if m.parent == nil {
		db = m.clone()
	} else {
		db = m
	}

	table := db.builder.table
	if table == "" {
		table = getTable(i)
	}

	fields, placeholders, args := buildInsertParts(i)

	sqlStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(fields, ","), strings.Join(placeholders, ","))
	m.trace(sqlStr, args)

	var result sql.Result
	var err error
	if db.tx == nil {
		result, err = db.Db.Exec(sqlStr, args...)
	} else {
		result, err = db.tx.Exec(sqlStr, args...)
	}
	if err != nil {
		db.Err = err
		return err
	}

	insertId, err := result.LastInsertId()
	if err == nil {
		// 设置 struct 中的 Id 字段
		rv := reflect.ValueOf(i).Elem()
		rt := rv.Type()

		var setField func(reflect.Value, reflect.Type) bool

		setField = func(val reflect.Value, typ reflect.Type) bool {
			for i := 0; i < typ.NumField(); i++ {
				field := typ.Field(i)
				fv := val.Field(i)

				// 处理嵌套匿名字段
				if field.Anonymous && field.Type.Kind() == reflect.Struct {
					if setField(fv, field.Type) {
						return true
					}
				}

				tag := strings.ToLower(field.Tag.Get("db"))
				if tag == "id" || field.Name == "Id" {
					if fv.CanSet() {
						switch fv.Kind() {
						case reflect.Int:
							fv.SetInt(insertId)
						case reflect.Int32:
							fv.SetInt(int64(int32(insertId)))
						case reflect.Int64:
							fv.SetInt(insertId)
						}
						return true
					}
				}
			}
			return false
		}

		setField(rv, rt)
	} else {
		db.trace("RowsAffected error:", err)
	}
	return err
}

func buildInsertParts(i interface{}) ([]string, []string, []interface{}) {
	val := reflect.ValueOf(i)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	typ := val.Type()

	// 调用 PreInsert() 方法（如果有）
	if mth, ok := reflect.ValueOf(i).Type().MethodByName("PreInsert"); ok {
		mth.Func.Call([]reflect.Value{reflect.ValueOf(i)})
	}

	fields := []string{}
	placeholders := []string{}
	args := []interface{}{}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		value := val.Field(i).Interface()
		tag := field.Tag.Get("db")
		if tag == "" {
			continue
		}
		if strings.ToLower(tag) == "id" {
			idStr := parseString(value)
			if idStr == "" || idStr == "0" {
				continue // 跳过自增 ID
			}
		}

		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			// 嵌套匿名结构体支持
			nestedFields, nestedPlaceholders, nestedArgs := buildInsertParts(val.Field(i).Addr().Interface())
			fields = append(fields, nestedFields...)
			placeholders = append(placeholders, nestedPlaceholders...)
			args = append(args, nestedArgs...)
			continue
		}

		valStr := fmt.Sprintf("%v", value)
		typeHint := field.Tag.Get("type")
		if typeHint == "date" {
			valStr = FormatToDate(valStr)
			if valStr == "" {
				continue
			}
			value = valStr
		} else if typeHint == "datetime" {
			valStr = FormatToDatetime(valStr)
			if valStr == "" {
				continue
			}
			value = valStr
		}

		fields = append(fields, tag)
		placeholders = append(placeholders, "?")
		args = append(args, value)
	}
	return fields, placeholders, args
}

func (m *ConDB) Update(field string, values ...interface{}) error {
	if m.parent == nil {
		m.trace("doesn't init ConDB")
		return errors.New("doesn't init ConDB")
	}
	if m.builder.table == "" {
		return errors.New("table not defined")
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("UPDATE ")
	sqlStr.WriteString(m.builder.table)
	sqlStr.WriteString(" SET ")
	sqlStr.WriteString(field)

	query, args := m.builder.Build()
	afterFrom := strings.SplitN(query, "FROM "+m.builder.table, 2)
	if len(afterFrom) == 2 {
		sqlStr.WriteString(afterFrom[1])
	}

	params := append(values, args...)
	m.trace(sqlStr.String(), params)

	var err error
	if m.tx == nil {
		m.Result, err = m.Db.Exec(sqlStr.String(), params...)
	} else {
		m.Result, err = m.tx.Exec(sqlStr.String(), params...)
	}
	if err != nil {
		m.Err = err
		return err
	}
	affected, err := m.Result.RowsAffected()
	if err != nil {
		m.trace("RowsAffected error:", err)
		return err
	}
	m.trace("RowsAffected num:", affected)
	return nil
}

func (db *ConDB) InsertId() int64 {

	insID, _ := db.Result.LastInsertId()
	return insID
}

// =================== ConDB 更新方法（map安全参数化） ===================

func (m *ConDB) UpdateMap(data map[string]interface{}) error {
	if m.parent == nil {
		m.trace("doesn't init ConDB")
		return errors.New("doesn't init ConDB")
	}
	if m.builder.table == "" {
		return errors.New("table not defined")
	}
	if len(data) == 0 {
		return errors.New("empty update data")
	}

	// 构建 SET 子句
	setParts := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data))
	for key, value := range data {
		setParts = append(setParts, fmt.Sprintf("%s = ?", key))
		args = append(args, value)
	}

	setClause := " SET " + strings.Join(setParts, ", ")

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("UPDATE ")
	sqlStr.WriteString(m.builder.table)
	sqlStr.WriteString(setClause)

	// 提取 WHERE 子句（只保留 FROM 之后部分）
	query, condArgs := m.builder.Build()
	afterFrom := strings.SplitN(query, "FROM "+m.builder.table, 2)
	if len(afterFrom) == 2 {
		sqlStr.WriteString(afterFrom[1])
	}

	params := append(args, condArgs...)
	m.trace(sqlStr.String(), params)

	var err error
	if m.tx == nil {
		m.Result, err = m.Db.Exec(sqlStr.String(), params...)
	} else {
		m.Result, err = m.tx.Exec(sqlStr.String(), params...)
	}
	if err != nil {
		m.Err = err
		return err
	}

	affected, err := m.Result.RowsAffected()
	if err != nil {
		m.trace("RowsAffected error:", err)
		return err
	}
	m.trace("RowsAffected num:", affected)
	return nil
}

func (m *ConDB) Exec(sql string, params ...interface{}) (sql.Result, error) {

	var db *ConDB
	if m.parent == nil {

		db = m.clone()
	} else {
		db = m
	}

	db.trace(sql, params...)

	if db.tx == nil {
		db.Result, db.Err = db.Db.Exec(sql, params...)

	} else {
		db.Result, db.Err = db.tx.Exec(sql, params...)

	}

	return db.Result, db.Err

}

// =================== ConDB 删除方法优化 ===================

func (m *ConDB) Delete(i ...interface{}) error {
	if len(i) == 0 {
		return m.delete()
	}

	obj := i[0]
	rv := reflect.ValueOf(obj).Elem()
	typ := rv.Type()
	found := false
	var idValue interface{}
	var dbField string

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("db")
		if tag == "" {
			tag = field.Tag.Get("gom")
		}
		if strings.ToLower(tag) == "id" {
			idValue = rv.Field(i).Interface()
			dbField = tag
			found = true
			break
		}
	}
	if !found {
		m.trace("No field with tag db:\"id\" found")
		return errors.New("missing id field with db tag")
	}

	return m.Model(obj).Where(fmt.Sprintf("%s = ?", dbField), idValue).delete()

}

func (m *ConDB) delete() error {
	if m.parent == nil {
		m.trace("ConDB not initialized, please use .Model().Where() chain")
		return errors.New("uninitialized ConDB")
	}
	if m.builder.table == "" {
		m.trace("no table specified")
		return errors.New("table not defined")
	}

	query, args := m.builder.Build()
	query = strings.Replace(query, m.builder.fields, "", 1) // 去掉 SELECT 字段部分，保留 FROM + WHERE
	query = strings.Replace(query, "SELECT", "DELETE", 1)

	m.trace(query, args)

	var err error
	if m.tx == nil {
		m.Result, err = m.Db.Exec(query, args...)
	} else {
		m.Result, err = m.tx.Exec(query, args...)
	}

	if err != nil {
		m.Err = err
		m.trace("Exec delete error:", err)
		return err
	}

	affected, err := m.Result.RowsAffected()
	if err != nil {
		m.trace("RowsAffected error:", err)
		return err
	}
	m.trace("RowsAffected:", affected)
	return nil
}

func FormatToDate(input string) string {
	t, err := time.Parse("2006-01-02", input)
	if err != nil {
		return ""
	}
	return t.Format("2006-01-02")
}
func FormatToDatetime(input string) string {
	t, err := time.Parse("2006-01-02 15:04:05", input)
	if err != nil {
		return ""
	}
	return t.Format("2006-01-02 15:04:05")
}
func parseString(value interface{}, args ...int) (s string) {
	switch v := value.(type) {
	case bool:
		s = strconv.FormatBool(v)
	case float32:
		s = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		s = strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		s = strconv.FormatInt(int64(v), 10)
	case int8:
		s = strconv.FormatInt(int64(v), 10)
	case int16:
		s = strconv.FormatInt(int64(v), 10)
	case int32:
		s = strconv.FormatInt(int64(v), 10)
	case int64:
		s = strconv.FormatInt(v, 10)
	case uint:
		s = strconv.FormatUint(uint64(v), 10)
	case uint8:
		s = strconv.FormatUint(uint64(v), 10)
	case uint16:
		s = strconv.FormatUint(uint64(v), 10)
	case uint32:
		s = strconv.FormatUint(uint64(v), 10)
	case uint64:
		s = strconv.FormatUint(v, 10)
	case string:
		s = v
	case time.Time:
		s = v.Format("2006-01-02 15:04:05")
	case []byte:
		s = string(v)
	default:
		s = fmt.Sprintf("%v", v)
	}
	return s
}

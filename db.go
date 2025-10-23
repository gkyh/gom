package gom

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type SqlExecutor interface {
	Model(class interface{}) *ConDB
	Table(name string) *ConDB
	StructModel(class interface{}) *ConDB
	Where(query string, values ...interface{}) *ConDB
	Maps(maps map[string]interface{}) *ConDB
	Or(query string, values ...interface{}) *ConDB
	In(key string, values []interface{}) *ConDB
	GroupBy(value string) *ConDB
	Count(agrs ...interface{}) int64
	Find(out interface{}) error

	//Select(out interface{}, sql string, values ...interface{}) error
	Raw(query string, args ...interface{}) *ConDB
	Scan(out interface{}) error 
	Sort(key, sort string) *ConDB
	Page(cur, count int32) *ConDB

	Update(field string, values ...interface{}) error
	UpdateMap(maps map[string]interface{}) error
	Delete(i ...interface{}) error
	Insert(i interface{}) error
	SelectInt(field string) int64
	SelectStr(field string) string
	Field(field string) *ConDB

	Get(out interface{}) error
	FindById(out, id interface{}) error
	IsExit() (bool, error)

	TxBegin() *ConDB
	Tx(tx *sql.Tx) *ConDB
	Commit() error
	Rollback() error
	GetForUpdate(out interface{}) error

	Exec(sql string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRows(query string, args ...interface{}) (*sql.Rows, error)

	QueryMap(query string, args ...interface{}) (map[string]interface{}, error)
	QueryMaps(query string, args ...interface{}) ([]map[string]interface{}, error)

	List() ([]map[string]interface{}, error)
	Query() (map[string]interface{}, error)
}

var _ SqlExecutor = &ConDB{}

var prefix string = "tb_"

type ConDB struct {
	Db     *sql.DB
	parent *ConDB
	tx     *sql.Tx

	Err     error
	Result  sql.Result
	builder *SQLBuilder

	rawSQL   string        //存放 Raw SQL
	rawArgs  []interface{} //存放参数
}

var logger SqlLogger
var logPrefix string

type SqlLogger interface {
	Printf(format string, v ...interface{})
}

func (m *ConDB) TraceOn(prefix string, log SqlLogger) {
	logger = log
	if prefix == "" {
		logPrefix = prefix
	} else {
		logPrefix = fmt.Sprintf("%s ", prefix)
	}
}

// TraceOff turns off tracing. It is idempotent.
func (m *ConDB) TraceOff() {
	logger = nil
	logPrefix = ""
}

func (m *ConDB) trace(query string, args ...interface{}) {
	if logger != nil {
		var margs = argsToStr(args...)
		logger.Printf("%s%s [%s]", logPrefix, query, margs)
	}
}

func (m *ConDB) clone() *ConDB {

	db := &ConDB{
		Db:      m.Db,
		parent:  m,
		tx:      nil,
		builder: NewSQLBuilder(),
	}
	return db
}

// ad dbMap new month
func (m *ConDB) Model(class interface{}) *ConDB {

	if m.parent == nil {

		db := m.clone()

		db.builder.From(getTable(class))
		return db
	} else {

		m.builder.From(getTable(class))
		return m
	}

}
func (m *ConDB) Table(name string) *ConDB {

	if m.parent == nil {
		db := m.clone()
		db.builder.From(name)
		return db
	} else {

		m.builder.From(name)
		return m
	}
}

func (m *ConDB) Where(query string, values ...interface{}) *ConDB {

	if m.parent == nil {
		db := m.clone()

		db.builder.Where(query, values...)
		return db
	} else {

		m.builder.Where(query, values...)
		return m
	}
}

// Raw 执行原生 SQL
func (m *ConDB) Raw(query string, args ...interface{}) *ConDB {
	newDB := m.clone()
	newDB.rawSQL = query
	newDB.rawArgs = args
	return newDB
}

// Scan 根据 out 类型执行不同的映射逻辑
func (m *ConDB) Scan(out interface{}) error {
	if m.rawSQL == "" {
		return errors.New("no raw SQL provided, use Raw() first")
	}

	rows, err := m.Db.Query(m.rawSQL, m.rawArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Ptr {
		return errors.New("out must be a pointer")
	}

	elem := rv.Elem() 

	switch elem.Kind() {
	case reflect.Slice:
		return RowsToList(rows, out)
	case reflect.Struct:
		return RowToStruct(rows, out)
	//case reflect.Map:
	//	return RowsToMap(rows)
	// 基础类型：单值查询（sum、count、avg 等）
	case reflect.Int, reflect.Int64, reflect.Float32, reflect.Float64, reflect.String, reflect.Bool:
		if !rows.Next() {
			return sql.ErrNoRows
		}
		var raw interface{}
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		// 使用 ConvertValueAuto 自动推断类型
		converted := ConvertValueAuto(raw)
		val := reflect.ValueOf(converted)

		if !val.IsValid() {
	        // 说明数据库返回了 NULL 值
	        // 可以选择设为零值或返回错误
	        elem.Set(reflect.Zero(elem.Type()))
	        return nil
	    }
		if val.Type().ConvertibleTo(elem.Type()) {
			elem.Set(val.Convert(elem.Type()))
		} else {
			// 无法直接转换时（例如 interface{} 类型）
			elem.Set(reflect.ValueOf(converted))
		}
		return nil

	default:
		return errors.New("unsupported out type, must be slice or struct")
	}
}

func (db *ConDB) Select(out interface{}, sql string, values ...interface{}) error {

	if sql == "" {
		return fmt.Errorf("sql is empty")
	}
	rows, err := db.Db.Query(sql, values...)
	if err != nil {

		return err
	}
	defer rows.Close()

	return RowsToList(rows, out)
}

func (m *ConDB) Field(field string) *ConDB {

	m.builder.fields = field
	return m
}

func (m *ConDB) Or(query string, args ...interface{}) *ConDB {

	if m.parent == nil {
		return nil
	}
	m.builder.Or(query, args...)
	return m
}

func (m *ConDB) In(key string, values []interface{}) *ConDB {

	if m.parent == nil {
		return nil
	}
	m.builder.In(key, values)
	return m
}

func (m *ConDB) IN(field string, value interface{}) *ConDB {
	if m.parent == nil {
		return nil
	}

	var list []interface{}

	switch v := value.(type) {
	case string:
		// 处理 "1,2,3" 格式
		if strings.Contains(v, ",") {
			parts := strings.Split(v, ",")
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					list = append(list, p)
				}
			}
		} else if v != "" {
			list = append(list, v)
		}
	case []string:
		for _, p := range v {
			list = append(list, p)
		}
	case []int:
		for _, p := range v {
			list = append(list, p)
		}
	case []interface{}:
		list = v
	default:
		list = append(list, v)
	}

	if len(list) == 0 {
		return m
	}

	m.builder.In(field, list) // 只调用，不接收返回值
	return m
}

func (m *ConDB) GroupBy(field string) *ConDB {

	if m.parent == nil {
		return nil
	}

	m.builder.GroupBy(field)
	return m
}

func (m *ConDB) OrderBy(field string) *ConDB {

	if m.parent == nil {
		return nil
	}

	m.builder.OrderBy(field)
	return m
}

func (db *ConDB) Sort(key, sort string) *ConDB {
	if db.parent == nil {
		return nil
	}
	db.builder.OrderBy(fmt.Sprintf("%s %s", key, sort))
	return db
}

func (db *ConDB) Page(cur, count int32) *ConDB {
	if db.parent == nil {
		return nil
	}
	start := (cur - 1) * count
	if start < 0 {
		start = 0
	}

	db.builder.Limit(start, count)
	return db
}

func (m *ConDB) Limit(offset, size int32) *ConDB {

	if m.parent == nil {
		return nil
	}

	m.builder.Limit(offset, size)
	return m
}

func m_type(v interface{}) string {
	return fmt.Sprintf("%T", v)
}

func (m *ConDB) Maps(filters map[string]interface{}) *ConDB {
	apply := func(target *ConDB) {
		for k, v := range filters {
			if m_type(v) == "string" && v == "" {
				continue
			}
			target.Where(k+" = ?", v)
		}
	}
	if m.parent == nil {
		db := m.clone()
		apply(db)
		return db
	} else {
		apply(m)
		return m
	}
}

func (m *ConDB) Count(args ...interface{}) int64 {
	if m.parent == nil {
		return 0
	}
	if m.builder.table == "" {
		if len(args) > 0 {
			table := getTable(args[0])
			m.builder.From(table)
		}
	}

	field := "*"
	if m.builder.fields != "" {
		field = m.builder.fields
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT COUNT(")
	sqlStr.WriteString(field)
	sqlStr.WriteString(") FROM ")
	sqlStr.WriteString(m.builder.table)

	sql, argsList := m.builder.Build()

	// 提取 WHERE 子句（避免重复 FROM）
	afterFrom := strings.SplitN(sql, "FROM "+m.builder.table, 2)
	if len(afterFrom) == 2 {
		sqlStr.WriteString(afterFrom[1])
	}

	m.trace(sqlStr.String(), argsList)

	var count int64 = 0
	err := m.Db.QueryRow(sqlStr.String(), argsList...).Scan(&count)
	if err != nil {
		m.Err = err
		return 0
	}

	return count
}

func (db *ConDB) Find(out interface{}) error {

	if db.parent == nil {

		return fmt.Errorf("Lack of ConDB objects")
	}
	if db.builder.table == "" {

		table := getTable(out)
		db.builder.From(table)
	}
	sqlStr, args := db.builder.Build()

	db.trace(sqlStr, args...)

	rows, err := db.Db.Query(sqlStr, args...)
	if err != nil {

		return err
	}
	defer rows.Close()

	return RowsToList(rows, out)
}

func (m *ConDB) FindAll(field string, limit, offset int64, out interface{}) *ConDB {
	if m.parent == nil {
		return nil
	}
	table := getTable(out)

	if field == "" {
		field = "*"
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(table)

	offsetCalc := (offset + 1) * limit
	sqlStr.WriteString(" WHERE id >= ((SELECT id FROM ")
	sqlStr.WriteString(table)
	sqlStr.WriteString(" ORDER BY id DESC LIMIT 1) - ")
	sqlStr.WriteString(strconv.FormatInt(offsetCalc, 10))
	sqlStr.WriteString(")")

	sqlStr.WriteString(" ORDER BY id DESC")
	sqlStr.WriteString(" LIMIT ")
	sqlStr.WriteString(strconv.FormatInt(limit, 10))

	m.trace(sqlStr.String(), nil)

	rows, err := m.Db.Query(sqlStr.String())
	if err != nil {
		m.Err = err
		return m
	}
	defer rows.Close()

	m.Err = RowsToList(rows, out)
	return m
}

func (db *ConDB) Query() (map[string]interface{}, error) {

	if db.parent == nil {
		return nil, errors.New("not found ConDB")
	}
	if db.builder.table == "" {

		return nil, errors.New("not found table")
	}

	sqlStr, args := db.builder.Build()
	sqlStr += " LIMIT 1" // ✅ 限制只取一条

	db.trace(sqlStr, args...)

	rows, err := db.Db.Query(sqlStr, args...)
	if err != nil {

		return nil, err
	}
	defer rows.Close()

	return RowsToMap(rows)
}

func (db *ConDB) List() ([]map[string]interface{}, error) {

	if db.parent == nil {
		return nil, errors.New("not found ConDB")
	}
	if db.builder.table == "" {

		return nil, errors.New("not found table")
	}

	sqlStr, params := db.builder.Build()

	db.trace(sqlStr, params...)

	rows, err := db.Db.Query(sqlStr, params...)
	if err != nil {

		return nil, err
	}
	defer rows.Close()

	return RowsToMaps(rows)
}

func (db *ConDB) SelectInt(field string) int64 {

	var out int64
	db.builder.Select(field)
	db_sql, params := db.builder.Build()

	db_sql += " limit 1"

	db.trace(db_sql, params...)

	db.Err = db.Db.QueryRow(db_sql, params...).Scan(&out)
	return out
}
func (db *ConDB) SelectStr(field string) string {

	var out string
	db.builder.Select(field)
	db_sql, params := db.builder.Build()

	db_sql += " limit 1"

	db.trace(db_sql, params...)

	db.Err = db.Db.QueryRow(db_sql, params...).Scan(&out)

	return out
}

func (m *ConDB) IsExit() (bool, error) {
	if m.parent == nil {
		return false, errors.New("no found model")
	}

	if m.builder.table == "" {
		return false, errors.New("no table defined")
	}

	// 构建 WHERE 子句（提取 FROM 后）
	sqlFull, args := m.builder.Build()
	afterFrom := strings.SplitN(sqlFull, "FROM "+m.builder.table, 2)
	whereClause := ""
	if len(afterFrom) == 2 {
		whereClause = afterFrom[1]
	}

	query := fmt.Sprintf("SELECT 1 FROM %s%s LIMIT 1", m.builder.table, whereClause)

	m.trace(query, args)

	var one int
	err := m.Db.QueryRow(query, args...).Scan(&one)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		m.Err = err
		return false, err
	}
	return true, nil

}

func (db *ConDB) FindById(out, id interface{}) error {

	DB := db
	if db.parent == nil {
		DB = db.clone()
	}
	if DB.builder.table == "" {

		DB.builder.From(getTable(out))
	}

	DB.builder.Where("id=?", id)
	query, args := DB.builder.Build()
	//DB.trace(sqlStr.String(), id)
	rows, err := db.Db.Query(query, args...)
	if err != nil {

		return err
	}
	defer rows.Close()

	return RowToStruct(rows, out)

}
func (db *ConDB) Get(out interface{}) error {

	if db.parent == nil {
		return nil
	}
	if db.builder.table == "" {

		db.builder.From(getTable(out))
	}

	query, args := db.builder.Build()
	query += " LIMIT 1" // ✅ 限制只取一条

	t := reflect.TypeOf(out)
	kind := t.Elem().Kind()

	if reflect.Struct == kind {

		rows, err := db.Db.Query(query, args...)
		if err != nil {

			return err
		}
		defer rows.Close()

		return RowToStruct(rows, out)

	}

	db.Err = db.Db.QueryRow(query, args...).Scan(out)
	return db.Err

}

func (m *ConDB) QueryRow(query string, args ...interface{}) *sql.Row {
	m.trace(query, args...)
	return m.Db.QueryRow(query, args...)
}

func (m *ConDB) QueryRows(query string, args ...interface{}) (*sql.Rows, error) {
	m.trace(query, args...)
	return m.Db.Query(query, args...)
}

func (db *ConDB) QueryMap(query string, args ...interface{}) (map[string]interface{}, error) {

	if db.parent == nil {

		rows, err := db.Db.Query(query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return RowsToMap(rows)
	}

	db.builder.Where(query, args...)
	sqlStr, params := db.builder.Build()
	sqlStr += " LIMIT 1"

	db.trace(sqlStr, params...)
	rows, err := db.Db.Query(sqlStr, params...)
	if err != nil {

		return nil, err
	}
	defer rows.Close()
	return RowsToMap(rows)
}

func (db *ConDB) QueryMaps(query string, args ...interface{}) ([]map[string]interface{}, error) {

	if db.parent == nil {

		rows, err := db.Db.Query(query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return RowsToMaps(rows)
	}

	db.builder.Where(query, args...)
	sqlStr, params := db.builder.Build()

	db.trace(sqlStr, params...)
	rows, err := db.Db.Query(sqlStr, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return RowsToMaps(rows)
}

func argsToStr(args ...interface{}) string {
	var margs string
	for i, a := range args {
		var v interface{} = a
		if x, ok := v.(driver.Valuer); ok {
			y, err := x.Value()
			if err == nil {
				v = y
			}
		}
		switch v.(type) {
		case string:
			v = fmt.Sprintf("%q", v)
		default:
			v = fmt.Sprintf("%v", v)
		}
		margs += fmt.Sprintf("%d:%s", i+1, v)
		if i+1 < len(args) {
			margs += " "
		}
	}
	return margs
}
func SliceClear(s *[]interface{}) {
	*s = (*s)[0:0]
}

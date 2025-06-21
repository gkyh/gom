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
	"time"
)

type SqlExecutor interface {
	Model(class interface{}) *ConDB
	Table(name string) *ConDB
	StructModel(class interface{}) *ConDB
	Where(query string, values ...interface{}) *ConDB
	Maps(maps map[string]interface{}) *ConDB
	Or(query string, values ...interface{}) *ConDB
	IN(key string, value string) *ConDB
	GroupBy(value string) *ConDB
	Count(agrs ...interface{}) int64
	PageSize(size int32) int32
	Find(out interface{}) *ConDB

	Select(args string) *ConDB
	Sort(key, sort string) *ConDB
	Page(cur, count int32) *ConDB

	Flush(c interface{}) error
	Update(field string, values ...interface{}) error
	UpdateMap(maps map[string]interface{}) error
	Delete(i ...interface{}) error
	Insert(i interface{}) error
	SelectInt(field string) int64
	SelectStr(field string) string
	QueryField(field string, out interface{}) error
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
	Pagination(currentPage, pageSize, totalPage int32) (page int32)
}

var _ SqlExecutor = &ConDB{}

var prefix string = "tb_"

type ConDB struct {
	Db          *sql.DB
	parent      *ConDB
	tx          *sql.Tx
	query       string
	inCondition string
	params      []interface{}
	Condition   []map[string]interface{}
	OrCondition []map[string]interface{}
	table       string
	field       string
	Offset      int32
	Limit       int32
	sort        string
	group       string
	Err         error
	Result      sql.Result
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

	db := &ConDB{Db: m.Db, parent: m, tx: nil, inCondition: "", query: "", table: "", Condition: nil, field: "*", Offset: 0, Limit: 0, sort: "", group: ""}
	return db
}

// ad dbMap new month
func (m *ConDB) Model(class interface{}) *ConDB {

	if m.parent == nil {

		db := m.clone()
		db.table = getTable(class)
		return db
	} else {

		if m.table == "" {
			m.table = getTable(class)
		}
		return m
	}

}
func (m *ConDB) Table(name string) *ConDB {

	if m.parent == nil {
		db := m.clone()
		db.table = name
		return db
	} else {

		m.table = name
		return m
	}
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
func (m *ConDB) Where(query string, values ...interface{}) *ConDB {

	if m.parent == nil {
		db := m.clone()
		db.Condition = append(db.Condition, map[string]interface{}{"query": query, "args": values})
		return db
	} else {

		m.Condition = append(m.Condition, map[string]interface{}{"query": query, "args": values})
		return m
	}
}
func (m *ConDB) Field(field string) *ConDB {

	m.field = field
	return m
}

func (m *ConDB) TxBegin() *ConDB {

	tx, _ := m.Db.Begin()

	if m.parent == nil {
		db := m.clone()
		db.tx = tx
		return db
	} else {

		m.tx = tx
		return m
	}
}
func (m *ConDB) Tx(tx *sql.Tx) *ConDB {

	if m.parent == nil {
		db := m.clone()
		db.tx = tx
		return db
	} else {

		m.tx = tx
		return m
	}
}

func (m *ConDB) Commit() error {

	return m.tx.Commit()
}

func (m *ConDB) Rollback() error {

	return m.tx.Rollback()
}

func (m *ConDB) Maps(maps map[string]interface{}) *ConDB {

	if m.parent == nil {
		db := m.clone()
		if maps != nil && len(maps) > 0 {

			for k, v := range maps {
				if m_type(v) == "string" && v == "" { //忽略空
					continue
				}
				query := k + " = ? "
				db.Where(query, v)
			}
		}
		return db
	} else {

		if maps != nil && len(maps) > 0 {

			for k, v := range maps {
				if m_type(v) == "string" && v == "" { //忽略空
					continue
				}
				query := k + " = ? "
				m.Where(query, v)

			}
		}
		return m
	}

}

func (db *ConDB) Select(args string) *ConDB {

	if db.parent == nil {
		return nil
	}
	db.field = args
	return db
}

func m_type(i interface{}) string {
	switch i.(type) {
	case string:
		return "string"
	case int:
		return "number"
	case int32:
		return "number"
	case int64:
		return "number"
	case float64:
		return "number"
	case uint32:
		return "number"
	case uint64:
		return "number"
	case []string:
		return "strings"
	default:
		return ""
	}

}
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

func (m *ConDB) Flush(c interface{}) error {

	var db *ConDB
	if m.parent == nil {

		db = m.clone()
	} else {
		db = m
	}

	s := bytes.Buffer{}

	s.WriteString("UPDATE ")

	if db.table == "" {

		s.WriteString(getTable(c))
	} else {

		s.WriteString(db.table)
	}

	s.WriteString(" set ")

	val := reflect.ValueOf(c)
	typ := reflect.TypeOf(c)

	_, ins := typ.MethodByName("PreUpdate")
	if ins {
		mv := val.MethodByName("PreUpdate")
		mv.Call(nil)
	}

	data := toMap(val, typ)
	buff := bytes.NewBuffer([]byte{})

	var id interface{}
	for k, v := range data {

		if k == "id" {
			id = v
			continue
		}
		buff.WriteString(",")
		buff.WriteString(k)
		buff.WriteString("= ? ")

		//buff.WriteString(parseString(v))
		db.params = append(db.params, v)

	}

	sql := strings.TrimLeft(buff.String(), `,`)

	s.WriteString(sql)
	s.WriteString(" where id = ?")

	//p := getKey(c, "Id")
	db.params = append(db.params, id)

	db.trace(s.String(), db.params...)

	if db.tx == nil {
		db.Result, db.Err = db.Db.Exec(s.String(), db.params...)

	} else {
		db.Result, db.Err = db.tx.Exec(s.String(), db.params...)

	}

	if db.Err != nil {

		return db.Err
	}

	aff_nums, err := db.Result.RowsAffected()
	if err == nil {
		db.trace("RowsAffected num:", aff_nums)
	} else {
		db.trace("RowsAffected error:%v", err)
	}

	return err

}
func (db *ConDB) UpdateMap(maps map[string]interface{}) error {

	if db.parent == nil {

		db.trace("doesn't init ConDB")
		return errors.New("doesn't init ConDB")
	}
	sql := buildUpdateSQL(maps)
	s := bytes.Buffer{}

	s.WriteString("UPDATE ")
	s.WriteString(db.table)
	s.WriteString(sql)
	s.WriteString(db.buildSql())

	db.trace(s.String(), db.params...)

	if db.tx == nil {
		db.Result, db.Err = db.Db.Exec(s.String(), db.params...)

	} else {
		db.Result, db.Err = db.tx.Exec(s.String(), db.params...)

	}

	if db.Err != nil {

		return db.Err
	}

	aff_nums, err := db.Result.RowsAffected()
	if err == nil {
		db.trace("RowsAffected num:", aff_nums)
		/*if aff_nums == 0 {

			return errors.New("RowsAffected rows is 0")
		}*/
	} else {
		db.trace("RowsAffected error:%v", err)
	}

	return err

}

func buildUpdateSQL(data map[string]interface{}) string {
	if len(data) == 0 {
		return ""
	}
	var setClauses []string

	for key, value := range data {
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", key, formatValue(value)))
	}

	return fmt.Sprintf(" SET %s", strings.Join(setClauses, ", "))
}

func formatValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")) // 转义单引号
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", v)
	}
}
func (db *ConDB) Update(field string, values ...interface{}) error {

	if db.parent == nil {

		db.trace("doesn't init ConDB")
		return errors.New("doesn't init ConDB")
	}
	s := bytes.Buffer{}

	s.WriteString("UPDATE ")
	s.WriteString(db.table)
	s.WriteString(" set ")
	s.WriteString(field)

	s.WriteString(db.buildSql())

	params := append(values, db.params...)

	db.trace(s.String(), db.params...)

	if db.tx == nil {
		db.Result, db.Err = db.Db.Exec(s.String(), params...)

	} else {
		db.Result, db.Err = db.tx.Exec(s.String(), params...)

	}

	if db.Err != nil {

		return db.Err
	}

	aff_nums, err := db.Result.RowsAffected()
	if err == nil {
		db.trace("RowsAffected num:", aff_nums)
		/*if aff_nums == 0 {

			return errors.New("RowsAffected rows is 0")
		}*/
	} else {
		db.trace("RowsAffected error:%v", err)
	}

	return err

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

func (db *ConDB) Delete(i ...interface{}) error {

	if len(i) > 0 {

		c := i[0]
		key := reflect.ValueOf(c).Elem().FieldByName("Id")

		if !key.IsValid() {

			db.trace("doesn't found key")
			return errors.New("doesn't found key")
		}
		//db1 := db.clone()
		id := fmt.Sprintf("%d", key)
		return db.Model(c).Where("id=?", id).delete()

	} else {

		return db.delete()
	}

}

func (db *ConDB) delete() error {

	if db.parent == nil {
		db.trace("doesn't init ConDB,need first get new ConDB")
		return errors.New("doesn't init ConDB,need first get new ConDB")
	}
	if db.table == "" {

		db.trace("no defined table name ")
		return errors.New("not defined table name")
	}
	s := bytes.Buffer{}

	s.WriteString("DELETE  FROM ")
	s.WriteString(db.table)

	s.WriteString(db.buildSql())

	db.trace(s.String(), db.params...)

	if db.tx == nil {
		db.Result, db.Err = db.Db.Exec(s.String(), db.params...)

	} else {
		db.Result, db.Err = db.tx.Exec(s.String(), db.params...)

	}

	if db.Err != nil {

		db.trace("error:%v", db.Err)
		return db.Err
	}

	aff_nums, err := db.Result.RowsAffected()
	if err == nil {
		db.trace("RowsAffected num:", aff_nums)
	} else {
		db.trace("RowsAffected error:%v", err)
	}

	return err
}

func into(field string) string {

	arry := strings.Split(field, ",")

	pty := strings.Repeat("?,", len(arry))

	vas := strings.TrimRight(pty, `,`)
	return vas
}

func sets(field string) string {

	return strings.Replace(field, ",", "=?,", -1) + "=?"
}

func (m *ConDB) Save(table, field string, key interface{}, args []interface{}) error {

	var db *ConDB
	if m.parent == nil {

		db = m.clone()
	} else {
		db = m
	}

	sql := `update ` + table + ` set ` + sets(field) + ` where id = ?`

	args = append(args, key)
	db.trace(sql, args...)
	if db.tx == nil {

		db.Result, db.Err = db.Db.Exec(sql, args...)
	} else {
		db.Result, db.Err = db.tx.Exec(sql, args...)
	}

	if db.Err != nil {

		return db.Err
	}

	aff_nums, err := db.Result.RowsAffected()
	if err == nil {
		db.trace("RowsAffected num:", aff_nums)
	} else {
		db.trace("RowsAffected error:%v", err)
	}

	return err

}

func (m *ConDB) Add(table, field string, args []interface{}) error {

	var db *ConDB
	if m.parent == nil {

		db = m.clone()
	} else {
		db = m
	}

	sql := `insert into ` + table + ` (` + field + `) values (` + into(field) + `)`

	db.trace(sql, args...)
	if db.tx == nil {

		db.Result, db.Err = db.Db.Exec(sql, args...)
	} else {
		db.Result, db.Err = db.tx.Exec(sql, args...)
	}

	if db.Err != nil {

		return db.Err
	}

	insID, err := db.Result.LastInsertId()
	if err == nil {
		db.trace("RowsAffected num:", insID)
	} else {
		db.trace("RowsAffected error:", err)
	}

	return err

}

func (m *ConDB) Insert(i interface{}) error {

	var db *ConDB
	if m.parent == nil {

		db = m.clone()
	} else {
		db = m
	}
	s := bytes.Buffer{}

	s.WriteString("INSERT INTO  ")

	if db.table == "" {

		s.WriteString(getTable(i))
	} else {

		s.WriteString(db.table)
	}

	s.WriteString(insertSql(i))

	db.trace(s.String())

	//var ret sql.Result
	//var err error
	if db.tx == nil {

		db.Result, db.Err = db.Db.Exec(s.String())
	} else {
		db.Result, db.Err = db.tx.Exec(s.String())
	}

	if db.Err != nil {

		return db.Err
	}

	insID, err := db.Result.LastInsertId()
	if err == nil {
		db.trace("RowsAffected num:", insID)

		key := reflect.ValueOf(i).Elem().FieldByName("Id")
		typ, _ := reflect.TypeOf(i).Elem().FieldByName("Id")

		tp := typ.Type.Name()

		if tp == "int" {

			newValue := reflect.ValueOf(int(insID))
			key.Set(newValue)
		} else if tp == "int32" {

			newValue := reflect.ValueOf(int32(insID))
			key.Set(newValue)
		} else if tp == "int64" {

			newValue := reflect.ValueOf(insID)
			key.Set(newValue)
		}

	} else {
		db.trace("RowsAffected error:", err)
	}

	return err
}

func (db *ConDB) InsertId() int64 {

	insID, _ := db.Result.LastInsertId()
	return insID
}

func (db *ConDB) Sort(key, sort string) *ConDB {
	if db.parent == nil {
		return nil
	}
	db.sort = fmt.Sprintf(" ORDER BY %s %s ", key, sort)
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
	db.Offset = start
	db.Limit = count
	return db
}

func (db *ConDB) Or(query string, values ...interface{}) *ConDB {
	if db.parent == nil {
		return nil
	}
	db.OrCondition = append(db.OrCondition, map[string]interface{}{"query": query, "args": values})
	return db
}

func (db *ConDB) IN(key string, value string) *ConDB {

	if db.parent == nil {
		return nil
	}
	db.inCondition = key + " IN (" + value + ") "

	return db
}

func (db *ConDB) GroupBy(value string) *ConDB {
	if db.parent == nil {
		return nil
	}
	db.group = " group by " + value
	return db
}
func (db *ConDB) PageSize(size int32) int32 {

	total := int32(db.Count())

	if total <= 0 {

		return 0
	}
	var page int32
	if total%size == 0 {
		page = total / size
	} else {
		page = (total + size) / size
	}

	return page

}
func (db *ConDB) Count(agrs ...interface{}) int64 {

	if db.parent == nil {
		return 0
	}
	if db.table == "" {
		if len(agrs) == 0 {
			return 0
		}
		db.table = getTable(agrs[0])
	}

	db_sql := bytes.Buffer{}
	db_sql.WriteString("SELECT count(")
	db_sql.WriteString(db.field)
	db_sql.WriteString(") FROM ")
	db_sql.WriteString(db.table)

	db_sql.WriteString(db.buildSql())

	if db.group != "" {

		db_sql.WriteString(db.group)
	}

	db.trace(db_sql.String(), db.params...)

	var count int64 = 0

	err := db.Db.QueryRow(db_sql.String(), db.params...).Scan(&count)
	if err != nil {

		db.Err = err

		return 0
	}
	//count, db.Err = db.dbmap.SelectInt(sql.String())
	return count

}
func (db *ConDB) Find(out interface{}) *ConDB {

	if db.parent == nil {
		return nil
	}
	if db.table == "" {

		db.table = getTable(out)
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(db.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(db.table)

	sqlStr.WriteString(db.buildSql())

	if db.group != "" {

		sqlStr.WriteString(db.group)
	}
	if db.sort != "" {

		sqlStr.WriteString(db.sort)
	}

	if db.Limit > 0 {

		ls := fmt.Sprintf(" limit %d,%d", db.Offset, db.Limit)
		sqlStr.WriteString(ls)
	}

	db.trace(sqlStr.String(), db.params...)

	rows, err := db.Db.Query(sqlStr.String(), db.params...)
	if err != nil {

		db.Err = err
		return db
	}
	defer rows.Close()

	db.Err = RowsToList(rows, out)

	//_, db.Err = db.dbmap.Select(out, sql.String())
	return db
}
func (db *ConDB) FindAll(out interface{}) *ConDB {

	if db.parent == nil {
		return nil
	}
	if db.table == "" {

		db.table = getTable(out)
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(db.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(db.table)

	offset := (db.Offset + 1) * db.Limit
	s := bytes.Buffer{}
	s.WriteString(" where id >= ((SELECT id  FROM ")
	s.WriteString(db.table)
	s.WriteString(" order by id desc limit 1)-")
	s.WriteString(strconv.FormatInt(int64(offset), 10))
	s.WriteString(") ")

	sqlStr.WriteString(s.String())

	sqlStr.WriteString(" order by id desc ")

	sqlStr.WriteString(" limit ")
	sqlStr.WriteString(strconv.FormatInt(int64(db.Limit), 10))

	db.trace(sqlStr.String(), nil)

	rows, err := db.Db.Query(sqlStr.String())
	if err != nil {

		db.Err = err
		return db
	}
	defer rows.Close()

	db.Err = RowsToList(rows, out)

	//_, db.Err = db.dbmap.Select(out, sql.String())
	return db
}
func (db *ConDB) Query() (map[string]interface{}, error) {

	if db.parent == nil {
		return nil, errors.New("not found ConDB")
	}
	if db.table == "" {

		return nil, errors.New("not found table")
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(db.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(db.table)

	sqlStr.WriteString(db.buildSql())

	if db.group != "" {

		sqlStr.WriteString(db.group)
	}

	sqlStr.WriteString(" limit 1")

	db.trace(sqlStr.String(), db.params...)

	rows, err := db.Db.Query(sqlStr.String(), db.params...)
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
	if db.table == "" {

		return nil, errors.New("not found table")
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(db.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(db.table)

	sqlStr.WriteString(db.buildSql())

	if db.group != "" {

		sqlStr.WriteString(db.group)
	}
	if db.sort != "" {

		sqlStr.WriteString(db.sort)
	}

	if db.Limit > 0 {

		ls := fmt.Sprintf(" limit %d,%d", db.Offset, db.Limit)
		sqlStr.WriteString(ls)
	}

	db.trace(sqlStr.String(), db.params...)

	rows, err := db.Db.Query(sqlStr.String(), db.params...)
	if err != nil {

		return nil, err
	}
	defer rows.Close()

	return RowsToMaps(rows)
}

func (db *ConDB) SelectInt(field string) int64 {

	var out int64
	db_sql := bytes.Buffer{}
	db_sql.WriteString("SELECT ")
	db_sql.WriteString(field)
	db_sql.WriteString(" FROM ")
	db_sql.WriteString(db.table)

	db_sql.WriteString(db.buildSql())

	db_sql.WriteString(" limit 1")

	db.trace(db_sql.String(), db.params...)

	db.Err = db.Db.QueryRow(db_sql.String(), db.params...).Scan(&out)
	return out
}
func (db *ConDB) SelectStr(field string) string {

	var out string
	db_sql := bytes.Buffer{}
	db_sql.WriteString("SELECT ")
	db_sql.WriteString(field)
	db_sql.WriteString(" FROM ")
	db_sql.WriteString(db.table)

	db_sql.WriteString(db.buildSql())

	db_sql.WriteString(" limit 1")

	db.trace(db_sql.String(), db.params...)

	db.Err = db.Db.QueryRow(db_sql.String(), db.params...).Scan(&out)
	return out
}
func (db *ConDB) QueryField(field string, out interface{}) error {

	db_sql := bytes.Buffer{}
	db_sql.WriteString("SELECT ")
	db_sql.WriteString(field)
	db_sql.WriteString(" FROM ")
	db_sql.WriteString(db.table)

	db_sql.WriteString(db.buildSql())

	db.trace(db_sql.String(), db.params...)

	rows, err := db.Db.Query(db_sql.String(), db.params...)
	if err != nil {

		//fmt.Errorf("gorp: cannot SELECT into this type: %v", err)
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	defer rows.Close()

	return rows.Scan(out)
}

func (db *ConDB) IsExit() (bool, error) {

	if db.parent == nil {
		return false, errors.New("no found model")
	}

	var out int64

	db_sql := bytes.Buffer{}
	db_sql.WriteString("SELECT 1  FROM ")
	db_sql.WriteString(db.table)

	db_sql.WriteString(db.buildSql())
	db_sql.WriteString(" LIMIT 1")

	db.trace(db_sql.String(), db.params...)

	db.Err = db.Db.QueryRow(db_sql.String(), db.params...).Scan(&out)

	if db.Err != nil && db.Err.Error() == "sql: no rows in result set" {

		return false, nil
	}
	return out > 0, db.Err

}
func (db *ConDB) FindById(out, id interface{}) error {

	DB := db
	if db.parent == nil {
		DB = db.clone()
	}
	if DB.table == "" {

		DB.table = getTable(out)
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(DB.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(DB.table)
	sqlStr.WriteString(" WHERE id=?")

	DB.trace(sqlStr.String(), id)
	rows, err := DB.Db.Query(sqlStr.String(), id)
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
	if db.table == "" {

		db.table = getTable(out)
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(db.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(db.table)

	sqlStr.WriteString(db.buildSql())

	if db.group != "" {

		sqlStr.WriteString(db.group)
	}

	sqlStr.WriteString(" limit 1")

	db.trace(sqlStr.String(), db.params...)

	t := reflect.TypeOf(out)
	kind := t.Elem().Kind()

	if reflect.Struct == kind {

		rows, err := db.Db.Query(sqlStr.String(), db.params...)
		if err != nil {

			return err
		}
		defer rows.Close()

		return RowToStruct(rows, out)

	}

	db.Err = db.Db.QueryRow(sqlStr.String(), db.params...).Scan(out)
	return db.Err

}

func (db *ConDB) GetForUpdate(out interface{}) error {

	if db.parent == nil {
		return nil
	}
	if db.table == "" {

		db.table = getTable(out)
	}

	sqlStr := bytes.Buffer{}
	sqlStr.WriteString("SELECT ")
	sqlStr.WriteString(db.field)
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(db.table)

	sqlStr.WriteString(db.buildSql())

	sqlStr.WriteString(" for update")

	db.trace(sqlStr.String())

	rows, err := db.tx.Query(sqlStr.String(), db.params...)

	if err != nil {

		return err
	}
	defer rows.Close()

	return RowToStruct(rows, out)

}

func (m *ConDB) QueryRow(query string, args ...interface{}) *sql.Row {
	m.trace(query, args...)
	return m.Db.QueryRow(query, args...)
}

func (m *ConDB) QueryRows(query string, args ...interface{}) (*sql.Rows, error) {
	m.trace(query, args...)
	return m.Db.Query(query, args...)
}

func (m *ConDB) QueryMap(query string, args ...interface{}) (map[string]interface{}, error) {

	m.trace(query, args...)
	rows, err := m.Db.Query(query, args...)
	if err != nil {

		return nil, err
	}
	defer rows.Close()
	return RowsToMap(rows)
}

func (m *ConDB) QueryMaps(query string, args ...interface{}) ([]map[string]interface{}, error) {

	m.trace(query, args...)
	rows, err := m.Db.Query(query, args...)
	if err != nil {

		return nil, err
	}
	defer rows.Close()
	return RowsToMaps(rows)
}

func (db *ConDB) Pagination(currentPage, pageSize, totalPage int32) (page int32) {

	if pageSize <= 0 {
		pageSize = 20
	}

	if currentPage <= 0 {

		currentPage = 1
	}
	page = 0
	if totalPage == 0 || currentPage == 1 {

		total := int32(db.Count())
		if total <= 0 {

			db.trace("没有查询到记录，获取记录数为0")
			//panic(`{"code":404, "msg": "没有查询到记录"}`)
			return
		}

		page = TotalPage(pageSize, total)

	} else {

		page = totalPage
	}

	db.Page(currentPage, pageSize)
	return
}

func TotalPage(pageSize, total int32) int32 {

	var page int32
	if total%pageSize == 0 {

		page = total / pageSize
	} else {
		page = (total + pageSize) / pageSize
	}
	return page
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

func (db *ConDB) buildSql() string {

	sql := bytes.Buffer{}
	SliceClear(&db.params)
	if len(db.Condition) > 0 {

		sql.WriteString(" WHERE ")

		i := 0
		for _, clause := range db.Condition {

			query := clause["query"].(string)
			values := clause["args"].([]interface{})
			if i > 0 {
				sql.WriteString(" AND ")
			}

			sql.WriteString(query)

			for _, vv := range values {

				db.params = append(db.params, vv)
			}
			i++
		}

	}
	if len(db.OrCondition) > 0 {

		sql.WriteString(" OR ")

		i := 0

		for _, clause := range db.OrCondition {

			query := clause["query"].(string)
			values := clause["args"].([]interface{})
			if i > 0 {
				sql.WriteString(" OR ")
			}

			sql.WriteString(query)

			for _, vv := range values {

				db.params = append(db.params, vv)
			}
			i++
		}

	}
	if db.inCondition != "" {

		if len(db.Condition) > 0 {

			sql.WriteString(" AND ")
		} else {
			sql.WriteString("  ")
		}
		sql.WriteString(db.inCondition)

	}
	return sql.String()
}

func insertSql(i interface{}) string {

	val := reflect.ValueOf(i)
	getType := reflect.TypeOf(i)

	_, ins := getType.MethodByName("PreInsert")
	if ins {
		mv := val.MethodByName("PreInsert")
		mv.Call(nil)
	}
	data := toMap(val, getType)
	buff := bytes.NewBuffer([]byte{})
	value := bytes.NewBuffer([]byte{})

	for k, v := range data {

		if k == "id" {

			id := parseString(v)
			if id == "" || id == "0" {
				continue
			}
		}
		buff.WriteString(",")
		buff.WriteString(k)

		if isNumber(v) {

			value.WriteString(fmt.Sprintf(",%v", v))
		} else {

			value.WriteString(fmt.Sprintf(",'%v'", v))

		}

	}

	key := strings.TrimLeft(buff.String(), `,`)
	vas := strings.TrimLeft(value.String(), `,`)

	sql := ` (` + key + `) values (` + vas + `)`
	return sql
}

func isNumber(val interface{}) bool {
	switch val.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return true
	default:
		return false
	}
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

func toMap(v reflect.Value, t reflect.Type) map[string]interface{} {

	m := make(map[string]interface{})
	vt := t.Elem()
	vv := v.Elem()

	for i := 0; i < vt.NumField(); i++ {

		obj := vt.Field(i)
		tag := obj.Tag.Get("db")

		value := vv.Field(i).Interface()
		if obj.Anonymous { // 输出匿名字段结构
			if obj.Name == "Common" {
				continue
			}
			for x := 0; x < obj.Type.NumField(); x++ {

				af := obj.Type.Field(x)
				tag := af.Tag.Get("db")
				if tag == "" {
					continue
				}
				vl := reflect.ValueOf(value).Field(x).Interface()
				m[tag] = vl

			}
		} else {

			if tag == "" {
				continue
			}
			tagType := obj.Tag.Get("type")
			if tagType == "date" {
				vv := FormatToDate(fmt.Sprintf("%v", value))
				if vv == "" {
					continue
				}
				m[tag] = vv

			} else if tagType == "datetime" {
				vv := FormatToDatetime(fmt.Sprintf("%v", value))
				if vv == "" {
					continue
				}
				m[tag] = vv
			} else {
				m[tag] = value
			}

		}
	}
	return m
}

func Int(f string) int {
	v, _ := strconv.ParseInt(f, 10, 0)
	return int(v)
}
func Int32(f string) int32 {
	v, _ := strconv.ParseInt(f, 10, 64)
	return int32(v)
}

func Int64(f string) int64 {
	v, _ := strconv.ParseInt(f, 10, 64)
	return int64(v)
}

func Float64(f string) float64 {
	v, _ := strconv.ParseFloat(f, 64)
	return float64(v)
}

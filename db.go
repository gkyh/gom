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

var prefix string = "tb_"

type MDB struct {
	Db          *sql.DB
	parent      *MDB
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

func (m *MDB) TraceOn(prefix string, log SqlLogger) {
	logger = log
	if prefix == "" {
		logPrefix = prefix
	} else {
		logPrefix = fmt.Sprintf("%s ", prefix)
	}
}

// TraceOff turns off tracing. It is idempotent.
func (m *MDB) TraceOff() {
	logger = nil
	logPrefix = ""
}

func (m *MDB) clone() *MDB {

	db := &MDB{Db: m.Db, parent: m, tx: nil, inCondition: "", query: "", table: "", Condition: nil, field: "*", Offset: 0, Limit: 0, sort: "", group: ""}
	return db
}

//ad dbMap new month
func (m *MDB) Model(class interface{}) *MDB {

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
func (m *MDB) Table(name string) *MDB {

	if m.parent == nil {
		db := m.clone()
		db.table = name
		return db
	} else {

		m.table = name
		return m
	}
}

func (m *MDB) Where(query string, values ...interface{}) *MDB {

	if m.parent == nil {
		db := m.clone()
		db.Condition = append(db.Condition, map[string]interface{}{"query": query, "args": values})
		return db
	} else {

		m.Condition = append(m.Condition, map[string]interface{}{"query": query, "args": values})
		return m
	}
}

func (m *MDB) TxBegin() *MDB {

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
func (m *MDB) Tx(tx *sql.Tx) *MDB {

	if m.parent == nil {
		db := m.clone()
		db.tx = tx
		return db
	} else {

		m.tx = tx
		return m
	}
}

func (m *MDB) Commit() error {

	return m.tx.Commit()
}

func (m *MDB) Rollback() error {

	return m.tx.Rollback()
}

func (m *MDB) Map(maps map[string]interface{}) *MDB {

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

func (db *MDB) Maps(maps map[string]interface{}) *MDB {

	if db.parent == nil {
		return nil
	}
	i := 0
	s := bytes.Buffer{}
	if maps != nil && len(maps) > 0 {

		for k, v := range maps {

			if m_type(v) == "string" && v == "" { //忽略空

				continue
			}
			if i > 0 {

				s.WriteString(" AND ")
			}
			if m_type(v) == "string" {

				s.WriteString(fmt.Sprintf(" %s='%s' ", k, v))
			} else {

				s.WriteString(fmt.Sprintf(" %s=%v ", k, v))
			}
			i++
		}
	}

	db.query = s.String()

	return db
}

func (db *MDB) Select(args string) *MDB {

	if db.parent == nil {
		return nil
	}
	db.field = args
	return db
}

func getName(class interface{}) string {

	t := reflect.TypeOf(class)
	str := fmt.Sprintf("%v", t)

	buff := bytes.NewBuffer([]byte{})

	for pos, char := range str {
		if str[pos] != '*' && str[pos] != '[' && str[pos] != ']' {

			buff.WriteRune(char)
		}
	}

	return buff.String()
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
	case []string:
		return "strings"
	default:
		return ""
	}

}
func getTable(class interface{}) string {

	var table string
	ts := reflect.TypeOf(class)
	se := fmt.Sprintf("%v", ts)

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

func (db *MDB) Update(field string, values ...interface{}) *MDB {

	if db.parent == nil {

		db.trace("doesn't init MDB")
		return nil
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

	aff_nums, err := db.Result.RowsAffected()
	if err == nil {
		db.trace("RowsAffected num:", aff_nums)
	} else {
		db.trace("RowsAffected error:%v", err)
	}

	return db

}

func (db *MDB) Delete(i ...interface{}) *MDB {

	if len(i) > 0 {

		class := i[0]
		key := getKey(class, "Id")
		if key == nil {

			db.trace("doesn't found key")
			return nil
		}
		db1 := db.clone()
		db1.Model(class).Where("id=?", key).delete()
		return nil
	} else {

		return db.delete()
	}

}

func (db *MDB) delete() *MDB {

	if db.parent == nil {
		db.trace("doesn't init MDB,need first get new mdb")
		return nil
	}
	if db.table == "" {

		db.trace("no defined table name ")
		db.Err = errors.New("not defined table name")
		return db
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

	aff_nums, err := db.Result.RowsAffected()
	if err == nil {
		db.trace("RowsAffected num:", aff_nums)
	} else {
		db.trace("RowsAffected error:%v", err)
	}

	return db
}

func (m *MDB) Insert(i interface{}) *MDB {

	var db *MDB
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

	insID, err := db.Result.LastInsertId()
	if err == nil {
		db.trace("RowsAffected num:", insID)
	} else {
		db.trace("RowsAffected error:", err)
	}

	return db
}

func (db *MDB) InsertId() int64 {

	insID, _ := db.Result.LastInsertId()
	return insID
}

func (db *MDB) Field(field string) *MDB {
	if db.parent == nil {
		return nil
	}
	db.field = field
	return db
}

func (db *MDB) Sort(key, sort string) *MDB {
	if db.parent == nil {
		return nil
	}
	db.sort = fmt.Sprintf(" ORDER BY %s %s ", key, sort)
	return db
}
func (db *MDB) Page(cur, count int32) *MDB {
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

func (db *MDB) Or(query string, values ...interface{}) *MDB {
	if db.parent == nil {
		return nil
	}
	db.OrCondition = append(db.OrCondition, map[string]interface{}{"query": query, "args": values})
	return db
}

func (db *MDB) IN(key string, value string) *MDB {

	if db.parent == nil {
		return nil
	}
	db.inCondition = key + " IN (" + value + ") "

	return db
}

func (db *MDB) GroupBy(value string) *MDB {
	if db.parent == nil {
		return nil
	}
	db.group = " group by " + value
	return db
}

func (db *MDB) Count(agrs ...interface{}) int32 {

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

		if err == sql.ErrNoRows {
			// there were no rows, but otherwise no error occurred
			//no found record
		}

		db.Err = err

		return 0
	}
	//count, db.Err = db.dbmap.SelectInt(sql.String())
	return int32(count)

}
func (db *MDB) Find(out interface{}) *MDB {

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

	db.Err = rowsToList(rows, out)

	//_, db.Err = db.dbmap.Select(out, sql.String())
	return db
}
func (db *MDB) SelectInt(field string) int64 {

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
func (db *MDB) SelectStr(field string) string {

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
func (db *MDB) QueryField(field string, out interface{}) error {

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

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	defer rows.Close()

	return rows.Scan(out)
}

func (db *MDB) IsExit() (bool, error) {

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
func (db *MDB) FindById(out, id interface{}) error {

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
	sqlStr.WriteString(" WHERE id=?")

	db.trace(sqlStr.String(), id)
	rows, err := db.Db.Query(sqlStr.String(), id)
	if err != nil {

		return err
	}

	maps, err := rowsToMap(rows)
	if err != nil {
		return err
	}

	mapToStruct(maps, out)
	return nil
	//return db.dbmap.SelectOne(out, sql.String(), id)
}
func (db *MDB) Get(out interface{}) error {

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

	rows, err := db.Db.Query(sqlStr.String(), db.params...)
	if err != nil {

		return err
	}

	maps, err := rowsToMap(rows)
	if err != nil {
		return err
	}

	mapToStruct(maps, out)

	return nil

}

func (db *MDB) GetForUpdate(out interface{}, id interface{}) error {

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

	sqlStr.WriteString(" WHERE id=? for update")
	fmt.Println(sqlStr.String())

	db.trace(sqlStr.String())

	rows, err := db.Db.Query(sqlStr.String(), id)

	if err != nil {

		return err
	}

	maps, err := rowsToMap(rows)
	if err != nil {
		return err
	}

	mapToStruct(maps, out)
	return nil

}

func (m *MDB) QueryRow(query string, args ...interface{}) *sql.Row {
	m.trace(query, args...)
	return m.Db.QueryRow(query, args...)
}

func (m *MDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	m.trace(query, args...)
	return m.Db.Query(query, args...)
}

func (m *MDB) trace(query string, args ...interface{}) {
	if logger != nil {
		var margs = argsToStr(args...)
		logger.Printf("%s%s [%s]", logPrefix, query, margs)
	}
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

func (db *MDB) buildSql() string {

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

func (db *MDB) createSql() string {

	sql := bytes.Buffer{}
	if db.query != "" {

		sql.WriteString(" WHERE ")
		sql.WriteString(db.query)

	}

	if len(db.Condition) > 0 {

		if db.query != "" {

			sql.WriteString(" AND ")
		} else {

			sql.WriteString(" WHERE ")
		}

		sql.WriteString(buildCondition(db.Condition))

	}
	if len(db.OrCondition) > 0 {

		sql.WriteString(" OR ")

		sql.WriteString(buildOrCondition(db.OrCondition))

	}
	return sql.String()
}

func buildCondition(w []map[string]interface{}) string {

	buff := bytes.NewBuffer([]byte{})
	i := 0

	for _, clause := range w {
		if sql := buildSelectQuery(clause); sql != "" {

			if i > 0 {
				buff.WriteString(" AND ")
			}
			buff.WriteString(sql)
			i++
		}

	}
	return buff.String()
}

func buildOrCondition(w []map[string]interface{}) string {

	buff := bytes.NewBuffer([]byte{})
	i := 0

	for _, clause := range w {
		if sql := buildSelectQuery(clause); sql != "" {

			if i > 0 {
				buff.WriteString(" Or ")
			}
			buff.WriteString(sql)
			i++
		}

	}
	return buff.String()
}

func buildSelectQuery(clause map[string]interface{}) (str string) {
	switch value := clause["query"].(type) {
	case string:
		str = value
	case []string:
		str = strings.Join(value, ", ")
	}

	args := clause["args"].([]interface{})

	buff := bytes.NewBuffer([]byte{})
	i := 0
	for pos, char := range str {
		if str[pos] == '?' {

			if m_type(args[i]) == "string" {
				buff.WriteString("'")
				buff.WriteString(args[i].(string))
				buff.WriteString("'")
			} else {
				buff.WriteString(fmt.Sprintf("%v", args[i]))
			}
			i++
		} else {
			buff.WriteRune(char)
		}
	}

	str = buff.String()

	return
}

func insertSql(i interface{}) string {

	val := reflect.ValueOf(i)

	getType := reflect.TypeOf(i)

	for i := 0; i < getType.NumMethod(); i++ {

		m := getType.Method(i)
		fmt.Printf("%s\n", m.Name)

		if m.Name == "PreInsert" {

			mv := val.MethodByName("PreInsert")
			mv.Call(nil)
		}
	}
	fmt.Println(i)

	data := structToMap(i)
	key := ""
	value := ""
	for k, v := range data {

		key += `,` + k
		value += `,'` + parseString(v) + `'`

	}

	key = strings.TrimLeft(key, `,`)
	value = strings.TrimLeft(value, `,`)

	sql := ` (` + key + `) values (` + value + `)`
	return sql
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
	case []byte:
		s = string(v)
	default:
		s = fmt.Sprintf("%v", v)
	}
	return s
}

func structToMap(i interface{}) map[string]interface{} {

	m := make(map[string]interface{})
	vt := reflect.TypeOf(i).Elem()
	vv := reflect.ValueOf(i).Elem()

	for i := 0; i < vt.NumField(); i++ {

		key := vt.Field(i)
		tag := key.Tag.Get("db")
		mk := key.Tag.Get("key")

		obj := vt.Field(i)

		value := vv.Field(i).Interface()

		if obj.Anonymous { // 输出匿名字段结构

			for x := 0; x < obj.Type.NumField(); x++ {

				af := obj.Type.Field(x)
				//key := af.Name
				//ktype := af.Type
				tag := af.Tag.Get("db")

				vv := reflect.ValueOf(value)

				vl := vv.Field(x).Interface()

				m[tag] = vl
			}
		} else if mk == "auto" {
			continue
		} else {

			//fmt.Printf("%q => %q, ", chKey, vv.FieldByName(key.Name).String())
			//fmt.Printf("第%d个字段是：%s:%v = %v \n", i+1, key.Name, key.Type, value)
			m[tag] = value
		}
	}
	return m
}

func mapToStruct(data map[string]string, c interface{}) {

	pv := reflect.ValueOf(c).Elem()
	pt := reflect.TypeOf(c).Elem()

	for i := 0; i < pv.NumField(); i++ {

		obj := pt.Field(i)

		key := pt.Field(i).Name
		ktype := pt.Field(i).Type
		col := pt.Field(i).Tag.Get("db")
		value := data[col]

		val := reflect.ValueOf(value)
		vtype := reflect.TypeOf(value)

		if ktype != vtype {

			val, _ = conversionType(value, ktype.Name())
		}

		if obj.Anonymous { // 输出匿名字段结构

			for x := 0; x < obj.Type.NumField(); x++ {

				af := obj.Type.Field(x)

				k := af.Name
				t := af.Type
				d := af.Tag.Get("db")

				vl := data[d]

				av := reflect.ValueOf(vl)
				at := reflect.TypeOf(vl)

				//fmt.Println(" ", k, ":", t, ":", d, " vl:", vl)

				if t != at {
					av, _ = conversionType(vl, t.Name())
				}
				pv.FieldByName(k).Set(av)

			}
		} else {

			pv.FieldByName(key).Set(val)
		}
	}
}
func conversionType(value string, ktype string) (reflect.Value, error) {

	if ktype == "string" {

		return reflect.ValueOf(ktype), nil
	} else if ktype == "int64" {

		buf, err := strconv.ParseInt(value, 10, 64)
		return reflect.ValueOf(buf), err
	} else if ktype == "int32" {

		buf, err := strconv.ParseInt(value, 10, 64)
		return reflect.ValueOf(int32(buf)), err
	} else if ktype == "int8" {

		buf, err := strconv.ParseInt(value, 10, 64)
		return reflect.ValueOf(int8(buf)), err
	} else if ktype == "int" {

		buf, err := strconv.Atoi(value)
		return reflect.ValueOf(buf), err
	} else if ktype == "float32" {

		buf, err := strconv.ParseFloat(value, 64)
		return reflect.ValueOf(float32(buf)), err
	} else if ktype == "float64" {

		buf, err := strconv.ParseFloat(value, 64)
		return reflect.ValueOf(buf), err
	} else if ktype == "time.Time" {

		buf, err := time.ParseInLocation("2006-01-02 15:04:05", value, time.Local)
		return reflect.ValueOf(buf), err
	} else if ktype == "Time" {

		buf, err := time.ParseInLocation("2006-01-02 15:04:05", value, time.Local)
		return reflect.ValueOf(buf), err
	} else {
		return reflect.ValueOf(ktype), nil
	}
}

func mapReflect(m map[string]string, v reflect.Value) error {

	t := v.Type()
	val := v.Elem()
	typ := t.Elem()

	if !val.IsValid() {
		return errors.New("数据类型不正确")
	}

	for i := 0; i < val.NumField(); i++ {

		obj := typ.Field(i)

		if obj.Anonymous { // 输出匿名字段结构

			value := val.Field(i)
			for x := 0; x < obj.Type.NumField(); x++ {

				af := obj.Type.Field(x)

				key := af.Name
				ktype := af.Type
				tag := af.Tag.Get("db")

				meta := m[tag]

				vl := reflect.ValueOf(meta)
				vt := reflect.TypeOf(meta)

				if ktype != vt {
					vl, _ = conversionType(meta, ktype.Name())
				}
				value.FieldByName(key).Set(vl)

			}
		}

		key := obj.Name
		ktype := obj.Type

		col := obj.Tag.Get("db")
		if len(col) == 0 {
			continue
		}
		meta, ok := m[col]
		if !ok {
			continue
		}

		vl := reflect.ValueOf(meta)
		vt := reflect.TypeOf(meta)

		if ktype != vt {
			vl, _ = conversionType(meta, ktype.Name())
		}

		val.FieldByName(key).Set(vl)
		/*

			value := val.Field(i)
			kind := value.Kind()
			tag := typ.Field(i).Tag.Get("db")
			if !value.CanSet() {
				return errors.New("结构体字段没有读写权限")
			}

			if len(meta) == 0 {
				continue
			}
			meta, ok := m[tag]
			if !ok {
				continue
			}

			if kind == reflect.Struct {

				fmt.Println(kind)

			} else if kind == reflect.String {
				value.SetString(meta)
			} else if kind == reflect.Float32 {
				f, err := strconv.ParseFloat(meta, 32)
				if err != nil {
					return err
				}
				value.SetFloat(f)
			} else if kind == reflect.Float64 {
				f, err := strconv.ParseFloat(meta, 64)
				if err != nil {
					return err
				}
				value.SetFloat(f)
			} else if kind == reflect.Int64 {
				integer64, err := strconv.ParseInt(meta, 10, 64)
				if err != nil {
					return err
				}
				value.SetInt(integer64)
			} else if kind == reflect.Int {
				integer, err := strconv.Atoi(meta)
				if err != nil {
					return err
				}
				value.SetInt(int64(integer))
			} else if kind == reflect.Int32 {
				integer, err := strconv.ParseInt(meta, 10, 64)
				if err != nil {
					return err
				}
				value.SetInt(integer)
			} else if kind == reflect.Bool {
				b, err := strconv.ParseBool(meta)
				if err != nil {
					return err
				}
				value.SetBool(b)
			} else if kind == reflect.Int8 {
				integer, err := strconv.ParseInt(meta, 10, 64)
				if err != nil {
					return err
				}
				value.SetInt(integer)
			} else {
				fmt.Println(kind)
				return errors.New("数据库映射存在不识别的数据类型")
			}
		*/
	}
	return nil
}

func rowsToList(rows *sql.Rows, in interface{}) error {

	d, err := rowsToMaps(rows)
	if err != nil {
		fmt.Println(err)
	}

	length := len(d)

	if length > 0 {
		v := reflect.ValueOf(in).Elem()

		newv := reflect.MakeSlice(v.Type(), 0, length)
		v.Set(newv)
		v.SetLen(length)

		index := 0
		for i := 0; i < length; i++ {

			k := v.Type().Elem()

			newObj := reflect.New(k)
			err := mapReflect(d[i], newObj)
			if err != nil {
				return err
			}

			v.Index(index).Set(newObj.Elem())
			index++
		}
		v.SetLen(index)
	}
	return nil
}

func rowsToMap(rows *sql.Rows) (map[string]string, error) {

	column, err := rows.Columns() //读出查询出的列字段名
	if err != nil {
		//logger.Error(err)
		return nil, err
	}

	values := make([][]byte, len(column))     //values是每个列的值，这里获取到byte里
	scans := make([]interface{}, len(column)) //因为每次查询出来的列是不定长的，用len(column)定住当次查询的长度

	for i := range values {

		scans[i] = &values[i]
	}

	for rows.Next() {

		if err := rows.Scan(scans...); err != nil {
			//query.Scan查询出来的不定长值放到scans[i] = &values[i],也就是每行都放在values里
			//logger.Error(err)
			return nil, err
		}

		row := make(map[string]string) //每行数据
		for k, v := range values {
			//每行数据是放在values里面，现在把它挪到row里
			key := column[k]
			row[key] = string(v)
		}
		return row, nil
	}

	return nil, errors.New("empty")
}

func rowsToMaps(rows *sql.Rows) ([]map[string]string, error) {

	column, err := rows.Columns() //读出查询出的列字段名
	if err != nil {
		//logger.Error(err)
		return nil, err
	}

	values := make([][]byte, len(column))     //values是每个列的值，这里获取到byte里
	scans := make([]interface{}, len(column)) //因为每次查询出来的列是不定长的，用len(column)定住当次查询的长度

	for i := range values {

		scans[i] = &values[i]
	}

	results := make([]map[string]string, 0) //最后得到的map
	for rows.Next() {

		if err := rows.Scan(scans...); err != nil {
			//query.Scan查询出来的不定长值放到scans[i] = &values[i],也就是每行都放在values里
			//logger.Error(err)
			return nil, err
		}

		row := make(map[string]string) //每行数据
		for k, v := range values {
			//每行数据是放在values里面，现在把它挪到row里
			key := column[k]
			row[key] = string(v)
		}
		results = append(results, row)
	}

	return results, nil
}

func getKey(c interface{}, keyName string) interface{} {

	pt := reflect.TypeOf(c)
	pv := reflect.ValueOf(c)
	if pt.Kind() == reflect.Ptr {

		pt = pt.Elem()
		pv = pv.Elem()
	}

	for i := 0; i < pv.NumField(); i++ {

		obj := pt.Field(i)

		key := pt.Field(i).Name
		//ktype := pt.Field(i).Type
		value := pv.Field(i).Interface()
		//fmt.Println("key:", key, " key Type:", ktype.Name(), " kind:", ktype.Kind(), "value:", value)
		if key == keyName {

			return value
		}

		if obj.Anonymous { // 输出匿名字段结构
			for x := 0; x < obj.Type.NumField(); x++ {

				vv := reflect.ValueOf(value)
				af := obj.Type.Field(x)
				vl := vv.Field(x).Interface()
				//fmt.Println(" ", af.Name, af.Type, vl)

				if af.Name == keyName {

					return vl
				}

			}
		}
	}
	return nil
}

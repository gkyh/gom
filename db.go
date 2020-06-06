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

var prefix string = "tb_"

type gomDB struct {
	Db          *sql.DB
	parent      *gomDB
	tx          *sql.Tx
	query       string
	Condition   []map[string]interface{}
	OrCondition []map[string]interface{}
	table       string
	field       string
	Offset      int32
	Limit       int32
	sort        string
	group       string
	Err         error
}

func (m *gomDB) SetPrefix(s string) {

	prefix = s
}

func (m *gomDB) clone() *gomDB {

	db := &gomDB{Db: m.Db, parent: m, tx: nil, query: "", table: "", Condition: nil, field: "*", Offset: 0, Limit: 0, sort: "", group: ""}
	return db
}

//ad dbMap new month
func (m *gomDB) Model(class interface{}) *gomDB {

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
func (m *gomDB) Table(name string) *gomDB {

	if m.parent == nil {
		db := m.clone()
		db.table = name
		return db
	} else {

		m.table = name
		return m
	}
}

func (m *gomDB) Where(query string, values ...interface{}) *gomDB {

	if m.parent == nil {
		db := m.clone()
		db.Condition = append(db.Condition, map[string]interface{}{"query": query, "args": values})
		return db
	} else {

		m.Condition = append(m.Condition, map[string]interface{}{"query": query, "args": values})
		return m
	}
}

func (m *gomDB) TxBegin() *gomDB {

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
func (m *gomDB) Tx(tx *sql.Tx) *gomDB {

	if m.parent == nil {
		db := m.clone()
		db.tx = tx
		return db
	} else {

		m.tx = tx
		return m
	}
}

func (m *gomDB) Commit() error {

	return m.tx.Commit()
}

func (m *gomDB) Rollback() error {

	return m.tx.Rollback()
}

func (db *gomDB) Maps(maps map[string]interface{}) *gomDB {

	if db.parent == nil {
		return nil
	}
	i := 0
	s := bytes.Buffer{}
	if maps != nil && len(maps) > 0 {

		for k, v := range maps {

			if m_type(v) == "string" && v == "" {

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

func (db *gomDB) Select(args string) *gomDB {

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

func (db *gomDB) Update(field string, values ...interface{}) error {

	if db.parent == nil {
		return errors.New("doesn't init gomDB")
	}
	s := bytes.Buffer{}

	s.WriteString("UPDATE ")
	s.WriteString(db.table)
	s.WriteString(" set ")
	s.WriteString(field)

	s.WriteString(db.BuildSql())

	var ret sql.Result
	if db.tx == nil {
		ret, db.Err = db.Db.Exec(s.String(), values...)

	} else {
		ret, db.Err = db.tx.Exec(s.String(), values...)

	}

	aff_nums, _ := ret.RowsAffected()
	fmt.Println("RowsAffected num:", aff_nums)

	return db.Err

}

func (db *gomDB) Delete(class interface{}) error {

	if db.parent == nil {
		return errors.New("doesn't init gomDB")
	}
	if db.table == "" {
		db.table = getTable(class)
	}
	s := bytes.Buffer{}

	s.WriteString("DELETE  FROM ")
	s.WriteString(db.table)

	s.WriteString(db.BuildSql())

	if db.tx == nil {

		_, db.Err = db.Db.Exec(s.String())
	} else {
		_, db.Err = db.tx.Exec(s.String())
	}

	return db.Err
}

func (db *gomDB) Insert(i interface{}) error {

	s := bytes.Buffer{}

	s.WriteString("INSERT INTO  ")

	if db.table == "" {

		s.WriteString(getTable(i))
	} else {

		s.WriteString(db.table)
	}

	s.WriteString(insertSql(i))

	fmt.Println(s.String())
	var ret sql.Result
	var err error
	if db.tx == nil {

		ret, err = db.Db.Exec(s.String())
	} else {
		ret, err = db.tx.Exec(s.String())
	}

	//insID, _ := ret.LastInsertId()
	fmt.Println(ret)

	return err
}

func (db *gomDB) Field(field string) *gomDB {
	if db.parent == nil {
		return nil
	}
	db.field = field
	return db
}

func (db *gomDB) Sort(key, sort string) *gomDB {
	if db.parent == nil {
		return nil
	}
	db.sort = fmt.Sprintf(" ORDER BY %s %s ", key, sort)
	return db
}
func (db *gomDB) Page(cur, count int32) *gomDB {
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

func (db *gomDB) Or(query string, values ...interface{}) *gomDB {
	if db.parent == nil {
		return nil
	}
	db.OrCondition = append(db.OrCondition, map[string]interface{}{"query": query, "args": values})
	return db
}

func (db *gomDB) IN(key string, value string) *gomDB {
	if db.parent == nil {
		return nil
	}
	in := key + " IN (" + value + ") "
	if db.query != "" {

		db.query = " AND " + in
	} else {

		db.query = in
	}
	return db
}

func (db *gomDB) GroupBy(value string) *gomDB {
	if db.parent == nil {
		return nil
	}
	db.group = " group by " + value
	return db
}

func (db *gomDB) BuildSql() string {

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

		sql.WriteString(buildOrCondition(db.Condition))

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

func (db *gomDB) Count(agrs ...interface{}) int32 {

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

	db_sql.WriteString(db.BuildSql())

	if db.group != "" {

		db_sql.WriteString(db.group)
	}

	var count int64 = 0

	err := db.Db.QueryRow(db_sql.String()).Scan(&count)
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
func (db *gomDB) Find(out interface{}) *gomDB {

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

	sqlStr.WriteString(db.BuildSql())

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

	rows, err := db.Db.Query(sqlStr.String())
	if err != nil {

		db.Err = err
		return db
	}

	db.Err = rowsToList(rows, out)

	//_, db.Err = db.dbmap.Select(out, sql.String())
	return db
}
func (db *gomDB) QueryField(field string, out interface{}) error {

	db_sql := bytes.Buffer{}
	db_sql.WriteString("SELECT ")
	db_sql.WriteString(field)
	db_sql.WriteString(" FROM ")
	db_sql.WriteString(db.table)

	db_sql.WriteString(db.BuildSql())

	rows, err := db.Db.Query(db_sql.String())
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
func (db *gomDB) FindById(out, id interface{}) error {

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
func (db *gomDB) Get(out interface{}) error {

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

	sqlStr.WriteString(db.BuildSql())

	if db.group != "" {

		sqlStr.WriteString(db.group)
	}

	sqlStr.WriteString(" limit 1")

	rows, err := db.Db.Query(sqlStr.String())
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

func insertSql(i interface{}) string {

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
	vt := reflect.TypeOf(i)
	vv := reflect.ValueOf(i)

	for i := 0; i < vt.NumField(); i++ {

		key := vt.Field(i)
		chKey := key.Tag.Get("db")
		mk := key.Tag.Get("key")
		if mk == "auto" {
			continue
		} else {
			//fmt.Printf("%q => %q, ", chKey, vv.FieldByName(key.Name).String())
			//fmt.Printf("第%d个字段是：%s:%v = %v \n", i+1, key.Name, key.Type, value)
			value := vv.Field(i).Interface()
			m[chKey] = value
		}
	}
	return m
}

func mapToStruct(data map[string]string, c interface{}) {

	pv := reflect.ValueOf(c).Elem()
	pt := reflect.TypeOf(c).Elem()

	for i := 0; i < pv.NumField(); i++ {

		key := pt.Field(i).Name

		ktype := pt.Field(i).Type

		col := pt.Field(i).Tag.Get("db")
		//fmt.Println("key:", key, " ktype:", ktype.Name(), " tag:", col)

		value := data[col]
		//fmt.Println("value:", value)

		val := reflect.ValueOf(value)
		vtype := reflect.TypeOf(value)

		if ktype != vtype {

			val, _ = conversionType(value, ktype.Name())

		}

		pv.FieldByName(key).Set(val)
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
		return reflect.ValueOf(buf), err
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

		value := val.Field(i)
		kind := value.Kind()
		tag := typ.Field(i).Tag.Get("db")

		if len(tag) > 0 {
			meta, ok := m[tag]
			if !ok {
				continue
			}

			if !value.CanSet() {
				return errors.New("结构体字段没有读写权限")
			}

			if len(meta) == 0 {
				continue
			}

			if kind == reflect.String {
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
			} else {
				return errors.New("数据库映射存在不识别的数据类型")
			}
		}
	}
	return nil
}

func rowsToList(rows *sql.Rows, in interface{}) error {

	d, _ := rowsToMaps(rows)

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

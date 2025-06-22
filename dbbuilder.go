package gom

import (
	"bytes"
	"fmt"
	"strings"
)

type SQLBuilder struct {
	op      string
	fields  string
	table   string
	where   []string
	or      []string
	in      string
	groupBy string
	orderBy string
	limit   string
	args    []interface{}
}

func NewSQLBuilder() *SQLBuilder {
	return &SQLBuilder{
		where: []string{},
		or:    []string{},
		args:  []interface{}{},
	}
}

func (b *SQLBuilder) Select(fields string) *SQLBuilder {
	b.op = "SELECT"
	b.fields = fields
	return b
}

func (b *SQLBuilder) From(table string) *SQLBuilder {
	b.table = table
	return b
}

func (b *SQLBuilder) Where(clause string, args ...interface{}) *SQLBuilder {
	b.where = append(b.where, clause)
	b.args = append(b.args, args...)
	return b
}

func (b *SQLBuilder) Or(clause string, args ...interface{}) *SQLBuilder {
	b.or = append(b.or, clause)
	b.args = append(b.args, args...)
	return b
}

func (b *SQLBuilder) In(field string, values []interface{}) *SQLBuilder {
	placeholders := strings.Repeat("?,", len(values))
	placeholders = placeholders[:len(placeholders)-1]
	b.in = fmt.Sprintf("%s IN (%s)", field, placeholders)
	b.args = append(b.args, values...)
	return b
}

func (b *SQLBuilder) GroupBy(group string) *SQLBuilder {
	b.groupBy = group
	return b
}

func (b *SQLBuilder) OrderBy(order string) *SQLBuilder {
	b.orderBy = order
	return b
}

func (b *SQLBuilder) Limit(offset, count int32) *SQLBuilder {
	b.limit = fmt.Sprintf("LIMIT %d OFFSET %d", count, offset)
	return b
}

func (b *SQLBuilder) Build() (string, []interface{}) {
	var sql bytes.Buffer

	sql.WriteString("SELECT ")
	if b.fields == "" {
		sql.WriteString("*")
	} else {
		sql.WriteString(b.fields)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(b.table)

	if len(b.where) > 0 {
		sql.WriteString(" WHERE ")
		sql.WriteString(strings.Join(b.where, " AND "))
	}
	if len(b.or) > 0 {
		if len(b.where) == 0 {
			sql.WriteString(" WHERE ")
		} else {
			sql.WriteString(" OR ")
		}
		sql.WriteString(strings.Join(b.or, " OR "))
	}
	if b.in != "" {
		if len(b.where) == 0 && len(b.or) == 0 {
			sql.WriteString(" WHERE ")
		} else {
			sql.WriteString(" AND ")
		}
		sql.WriteString(b.in)
	}
	if b.groupBy != "" {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(b.groupBy)
	}
	if b.orderBy != "" {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(b.orderBy)
	}
	if b.limit != "" {
		sql.WriteString(" ")
		sql.WriteString(b.limit)
	}

	return sql.String(), b.args
}

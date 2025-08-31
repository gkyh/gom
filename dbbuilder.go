package gom

import (
	"bytes"
	"fmt"
	"strings"
)

type clause struct {
	kind string // "where", "or", "in"
	expr string
	args []interface{}
}
type SQLBuilder struct {
	fields  string
	table   string
	//where   []string
	//or      []string
	//in      string
	clauses []clause
	groupBy string
	orderBy string
	limit   string
	//args    []interface{}
}

func NewSQLBuilder() *SQLBuilder {
	return &SQLBuilder{}
}

func (b *SQLBuilder) Select(fields string) *SQLBuilder {
	
	b.fields = fields
	return b
}

func (b *SQLBuilder) From(table string) *SQLBuilder {
	b.table = table
	return b
}

func (b *SQLBuilder) Where(clause string, args ...interface{}) *SQLBuilder {
	b.clauses = append(b.clauses, clause{"where", expr, args})
	return b
}

func (b *SQLBuilder) Or(clause string, args ...interface{}) *SQLBuilder {
	b.clauses = append(b.clauses, clause{"or", expr, args})
	return b
}

func (b *SQLBuilder) In(field string, values []interface{}) *SQLBuilder {
	if len(values) == 0 {
		return b
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(values)), ",")
	expr := fmt.Sprintf("%s IN (%s)", field, placeholders)
	b.clauses = append(b.clauses, clause{"in", expr, values})
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
	var buf strings.Builder
	args := []interface{}{}

	buf.WriteString("SELECT ")
	if b.fields == "" {
		buf.WriteString("*")
	} else {
		buf.WriteString(b.fields)
	}
	buf.WriteString(" FROM ")
	buf.WriteString(b.table)

	first := true
	for _, c := range b.clauses {
		switch c.kind {
		case "where":
			if first {
				buf.WriteString(" WHERE ")
				first = false
			} else {
				buf.WriteString(" AND ")
			}
			buf.WriteString(c.expr)
		case "or":
			if first {
				buf.WriteString(" WHERE ")
				first = false
			} else {
				buf.WriteString(" OR ")
			}
			buf.WriteString(c.expr)
		case "in":
			if first {
				buf.WriteString(" WHERE ")
				first = false
			} else {
				buf.WriteString(" AND ")
			}
			buf.WriteString(c.expr)
		}
		args = append(args, c.args...)
	}

	if b.groupBy != "" {
		buf.WriteString(" GROUP BY ")
		buf.WriteString(b.groupBy)
	}
	if b.orderBy != "" {
		buf.WriteString(" ORDER BY ")
		buf.WriteString(b.orderBy)
	}
	if b.limit != "" {
		buf.WriteString(" ")
		buf.WriteString(b.limit)
	}

	return buf.String(), args
}

package gom

import "database/sql"

func (db *ConDB) GetForUpdate(out interface{}) error {

	if db.parent == nil {
		return nil
	}
	if db.builder.table == "" {

		db.builder.From(getTable(out))
	}

	sqlStr, args := db.builder.Build()

	sqlStr += " for update"

	db.trace(sqlStr)

	rows, err := db.tx.Query(sqlStr, args...)
	if err != nil {

		return err
	}
	defer rows.Close()

	return RowToStruct(rows, out)

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

package main

import (
	// inner
	"database/sql"
	"fmt"
	"log"
	"os"

	// mine
	"dbMiner"

	// outer
	_ "github.com/go-sql-driver/mysql" //mySql driver
)

type MySQLMiner struct {
	Host string
	DB   sql.DB
}

func New(host string) (*MySQLMiner, error) {
	m := MySQLMiner{Host: host}

	err := m.connect()

	if err != nil {
		log.Printf("Func New error while connectiong to host %s:\n", host)
		return nil, err
	}

	return &m, nil
}

func (m *MySQLMiner) connect() error {
	db, err := sql.Open("mysql", fmt.Sprintf("root:p@55w0rd!@tcp(%s:3306)/information_schema", m.Host))
	if err != nil {
		log.Panicln(err)
	}
	m.DB = *db
	return nil
}

func (m *MySQLMiner) GetSchema() (*dbMiner.Schema, error) {
	var s = new(dbMiner.Schema)

	sql := `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM columns WHERE TABLE_SCHEMA NOT IN
	('mysql','information_schema','performance_schema','sys')
	ORDER BY TABLE_SCHEMA, TABLE_NAME`
	schemarows, err := m.DB.Query(sql)
	if err != nil {
		return nil, err
	}
	defer schemarows.Close()

	var (
		prevschema, prevtable string
		db                    dbMiner.Database
		table                 dbMiner.Table
	)
	for schemarows.Next() {
		var currschema, currtable, currcol string
		if err := schemarows.Scan(&currschema, &currtable, &currcol); err != nil {
			return nil, err
		}
		if currschema != prevschema {
			if prevschema != "" {
				db.Tables = append(db.Tables, table)
				s.Databases = append(s.Databases, db)
			}

			db = dbMiner.Database{Name: currschema, Tables: []dbMiner.Table{}}
			prevschema = currschema
			prevtable = ""
		}

		if currtable != prevtable {
			if prevtable != "" {
				db.Tables = append(db.Tables, table)
			}
			table = dbMiner.Table{Name: currtable}
			prevtable = currtable
		}
		table.Columns = append(table.Columns, currcol)
	}
	db.Tables = append(db.Tables, table)
	s.Databases = append(s.Databases, db)
	if err := schemarows.Err(); err != nil {
		return nil, err
	}

	return s, nil
}

func main() {
	mm, err := New(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer mm.DB.Close()
	if err := dbMiner.Search(mm); err != nil {
		panic(err)
	}
}

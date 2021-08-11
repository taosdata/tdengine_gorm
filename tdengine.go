package taosgorm

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/taosdata/driver-go/taosSql"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// DriverName is the default driver name for TDengine.
const DriverName = "taosSql"

type Dialect struct {
	DriverName string
	DSN        string
	Conn       gorm.ConnPool
}

func Open(dsn string) gorm.Dialector {
	return &Dialect{DSN: dsn}
}

func (dialect Dialect) Name() string {
	return "tdengine"
}

func (dialect Dialect) Initialize(db *gorm.DB) (err error) {
	if dialect.DriverName == "" {
		dialect.DriverName = DriverName
	}
	db.SkipDefaultTransaction = true
	db.DisableNestedTransaction = true
	db.DisableAutomaticPing = true
	db.DisableForeignKeyConstraintWhenMigrating = true
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		LastInsertIDReversed: true,
		QueryClauses:         []string{"SELECT", "FROM", "WHERE", "WINDOW", "FILL", "GROUP BY", "ORDER BY", "SLIMIT", "LIMIT"},
		CreateClauses:        []string{"CREATE TABLE", "INSERT", "USING", "VALUES", "ON CONFLICT"},
	})
	if dialect.Conn != nil {
		db.ConnPool = dialect.Conn
	} else {
		db.ConnPool, err = sql.Open(dialect.DriverName, dialect.DSN)
		if err != nil {
			return err
		}
	}
	for k, v := range dialect.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}
	return
}

func (dialect Dialect) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		"INSERT": func(c clause.Clause, builder clause.Builder) {
			if _, ok := c.Expression.(clause.Insert); ok {
				if stmt, ok := builder.(*gorm.Statement); ok {
					_, containsCreateTable := stmt.Clauses["CREATE TABLE"]
					if containsCreateTable {
						return
					}
				}
			}
			c.Build(builder)
		},
		"FOR": func(c clause.Clause, builder clause.Builder) {
			if _, ok := c.Expression.(clause.Locking); ok {
				return
			}
			c.Build(builder)
		},
		"VALUES": func(c clause.Clause, builder clause.Builder) {
			if _, ok := c.Expression.(clause.Values); ok {
				if stmt, ok := builder.(*gorm.Statement); ok {
					_, containsCreateTable := stmt.Clauses["CREATE TABLE"]
					if containsCreateTable {
						return
					}
				}
			}
			c.Build(builder)
		},
	}
}
func (dialect Dialect) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "NULL"}
}

func (dialect Dialect) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:                          db,
		Dialector:                   dialect,
		CreateIndexAfterCreateTable: false,
	}}, dialect}
}

func (dialect Dialect) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	switch v.(type) {
	case string:
		writer.WriteString("'?'")
	default:
		writer.WriteByte('?')
	}
}

func (dialect Dialect) QuoteTo(writer clause.Writer, str string) {
	writer.WriteString(str)
	return
}

func (dialect Dialect) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, "'", vars...)
}

func (dialect Dialect) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "bool"
	case schema.Int, schema.Uint:
		sqlType := "bigint"
		switch {
		case field.Size <= 8:
			sqlType = "tinyint"
		case field.Size <= 16:
			sqlType = "smallint"
		case field.Size <= 32:
			sqlType = "int"
		}
		return sqlType
	case schema.Float:
		if field.Size <= 32 {
			return "float"
		}
		return "double"
	case schema.String:
		size := field.Size
		if size == 0 {
			size = 64
		}
		return fmt.Sprintf("NCHAR(%d)", size)
	case schema.Time:
		return "TIMESTAMP"
	case schema.Bytes:
		size := field.Size
		if size == 0 {
			size = 64
		}
		return fmt.Sprintf("BINARY(%d)", size)
	}

	return string(field.DataType)
}

func (dialect Dialect) SavePoint(tx *gorm.DB, name string) error {
	return errors.New("not support")
}

func (dialect Dialect) RollbackTo(tx *gorm.DB, name string) error {
	return errors.New("not support")
}

package tests

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"gorm.io/gorm/utils/tests"
	"reflect"
	"strings"
	"sync"
	"testing"
)

var db, _ = gorm.Open(DummyDialector{}, nil)

func CheckBuildClauses(t *testing.T, clauses []clause.Interface, results []string, vars [][][]interface{}) {
	var (
		buildNames    []string
		buildNamesMap = map[string]bool{}
		user, _       = schema.Parse(&tests.User{}, &sync.Map{}, db.NamingStrategy)
		stmt          = gorm.Statement{DB: db, Table: user.Table, Schema: user, Clauses: map[string]clause.Clause{}}
	)

	for _, c := range clauses {
		if _, ok := buildNamesMap[c.Name()]; !ok {
			buildNames = append(buildNames, c.Name())
			buildNamesMap[c.Name()] = true
		}

		stmt.AddClause(c)
	}

	stmt.Build(buildNames...)
	sql := strings.TrimSpace(stmt.SQL.String())
	matched := false
	for i, result := range results {
		if sql == result {
			matched = true
			matchVars := false
			if len(stmt.Vars) > 0 {
				if len(vars) > i {
					for _, varItem := range vars[i] {
						if reflect.DeepEqual(stmt.Vars, varItem) {
							matchVars = true
							break
						}
					}
				}
			} else {
				matchVars = true
			}
			if !matchVars {
				t.Errorf("Vars expects %+v got %v", stmt.Vars, vars[i])
			}
			break
		}
	}
	if !matched {
		t.Errorf("SQL expects in %v got %v", results, sql)
	}
}

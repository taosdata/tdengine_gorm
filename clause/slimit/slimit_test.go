package slimit_test

import (
	"fmt"
	"github.com/taosdata/tdengine_gorm/clause/slimit"
	"github.com/taosdata/tdengine_gorm/clause/tests"
	"testing"

	"gorm.io/gorm/clause"
)

func TestSLimit(t *testing.T) {
	results := []struct {
		Clauses []clause.Interface
		Result  string
		Vars    []interface{}
	}{
		{
			[]clause.Interface{clause.Select{}, clause.From{}, slimit.SLimit{
				Limit:  10,
				Offset: 20,
			}},
			"SELECT * FROM users SLIMIT 10 SOFFSET 20", nil,
		},
		{
			[]clause.Interface{clause.Select{}, clause.From{}, slimit.SLimit{Offset: 20}},
			"SELECT * FROM users SOFFSET 20", nil,
		},
		{
			[]clause.Interface{clause.Select{}, clause.From{}, slimit.SLimit{Offset: 20}, slimit.SLimit{Offset: 30}},
			"SELECT * FROM users SOFFSET 30", nil,
		},
		{
			[]clause.Interface{clause.Select{}, clause.From{}, slimit.SLimit{Offset: 20}, slimit.SLimit{Limit: 10}},
			"SELECT * FROM users SLIMIT 10 SOFFSET 20", nil,
		},
		{
			[]clause.Interface{clause.Select{}, clause.From{}, slimit.SLimit{Limit: 10, Offset: 20}, slimit.SLimit{Offset: 30}},
			"SELECT * FROM users SLIMIT 10 SOFFSET 30", nil,
		},
		{
			[]clause.Interface{clause.Select{}, clause.From{}, slimit.SLimit{Limit: 10, Offset: 20}, slimit.SLimit{Offset: 30}, slimit.SLimit{Offset: -10}},
			"SELECT * FROM users SLIMIT 10", nil,
		},
		{
			[]clause.Interface{clause.Select{}, clause.From{}, slimit.SLimit{Limit: 10, Offset: 20}, slimit.SLimit{Offset: 30}, slimit.SLimit{Limit: -10}},
			"SELECT * FROM users SOFFSET 30", nil,
		},
		{
			[]clause.Interface{clause.Select{}, clause.From{}, slimit.SLimit{Limit: 10, Offset: 20}, slimit.SLimit{Offset: 30}, slimit.SLimit{Limit: 50}},
			"SELECT * FROM users SLIMIT 50 SOFFSET 30", nil,
		},
		{
			[]clause.Interface{clause.Select{}, clause.From{}, slimit.SetSLimit(10, 20), slimit.SetSLimit(0, 30), slimit.SetSLimit(50, 0)},
			"SELECT * FROM users SLIMIT 50 SOFFSET 30", nil,
		},
	}

	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			tests.CheckBuildClauses(t, result.Clauses, []string{result.Result}, [][][]interface{}{{result.Vars}})
		})
	}
}

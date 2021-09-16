package fill_test

import (
	"fmt"
	"github.com/taosdata/tdengine_gorm/clause/fill"
	"github.com/taosdata/tdengine_gorm/clause/tests"
	"github.com/taosdata/tdengine_gorm/clause/window"
	"testing"

	"gorm.io/gorm/clause"
)

func TestSetValue(t *testing.T) {
	var (
		results = []struct {
			Clauses []clause.Interface
			Result  []string
			Vars    [][][]interface{}
		}{
			{
				Clauses: []clause.Interface{
					clause.Select{Columns: []clause.Column{{Table: "t_1", Name: "avg(value)"}}},
					clause.From{Tables: []clause.Table{{Name: "t_1"}}},
					window.SetInterval(window.Duration{Value: 10, Unit: window.Minute}),
					fill.SetFill(fill.FillValue).SetValue(12),
				},
				Result: []string{"SELECT t_1.avg(value) FROM t_1 INTERVAL(10m) FILL (VALUE,12)"},
				Vars:   nil,
			},
		}
	)
	for idx, result := range results {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			tests.CheckBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}

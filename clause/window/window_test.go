package window_test

import (
	"fmt"
	"github.com/taosdata/tdengine_gorm/clause/tests"
	"github.com/taosdata/tdengine_gorm/clause/window"
	"testing"
	"time"

	"gorm.io/gorm/clause"
)

func TestSetInterval(t *testing.T) {
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
				},
				Result: []string{"SELECT t_1.avg(value) FROM t_1 INTERVAL(10m)"},
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

func TestSetStateWindow(t *testing.T) {
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
					window.SetStateWindow("state"),
				},
				Result: []string{"SELECT t_1.avg(value) FROM t_1 STATE_WINDOW(state)"},
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

func TestSetSessionWindow(t *testing.T) {
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
					window.SetSessionWindow("ts", window.Duration{
						Value: 10,
						Unit:  window.Minute,
					}),
				},
				Result: []string{"SELECT t_1.avg(value) FROM t_1 SESSION(ts,10m)"},
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

func TestSetOffset(t *testing.T) {
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
					window.SetInterval(window.Duration{Value: 10, Unit: window.Minute}).SetOffset(window.Duration{
						Value: 5,
						Unit:  window.Minute,
					}),
				},
				Result: []string{"SELECT t_1.avg(value) FROM t_1 INTERVAL(10m,5m)"},
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

func TestSetSliding(t *testing.T) {
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
					window.SetInterval(window.Duration{Value: 10, Unit: window.Minute}).SetOffset(window.Duration{
						Value: 5,
						Unit:  window.Minute,
					}).SetSliding(window.Duration{
						Value: 2,
						Unit:  window.Minute,
					}),
				},
				Result: []string{"SELECT t_1.avg(value) FROM t_1 INTERVAL(10m,5m) SLIDING(2m)"},
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

func TestNewDurationFromTimeDuration(t *testing.T) {
	duration5Min, err := window.NewDurationFromTimeDuration(time.Minute * 5)
	if err != nil {
		t.Errorf("NewDurationFromTimeDuration error : %s", err.Error())
		return
	}
	_, err = window.NewDurationFromTimeDuration(-time.Second)
	if err == nil {
		t.Errorf("Need error")
		return
	}
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
					window.SetInterval(*duration5Min),
				},
				Result: []string{"SELECT t_1.avg(value) FROM t_1 INTERVAL(300000000u)"},
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

//ParseDuration
func TestParseDuration(t *testing.T) {
	duration5Min, err := window.ParseDuration("5m")
	if err != nil {
		t.Errorf("ParseDuration error : %s", err.Error())
		return
	}
	_, err = window.ParseDuration("1")
	if err == nil {
		t.Errorf("need error")
		return
	}
	_, err = window.ParseDuration("1K")
	if err == nil {
		t.Errorf("need error")
		return
	}
	_, err = window.ParseDuration("mm")
	if err == nil {
		t.Errorf("need error")
		return
	}
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
					window.SetInterval(*duration5Min),
				},
				Result: []string{"SELECT t_1.avg(value) FROM t_1 INTERVAL(5m)"},
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

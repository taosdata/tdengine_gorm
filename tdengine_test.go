package tdengine_gorm

import (
	"database/sql"
	"fmt"
	"github.com/taosdata/tdengine_gorm/clause/create"
	"github.com/taosdata/tdengine_gorm/clause/fill"
	"github.com/taosdata/tdengine_gorm/clause/using"
	"github.com/taosdata/tdengine_gorm/clause/window"
	"gorm.io/gorm"
	"math/rand"
	"testing"
	"time"
)

func TestDialect(t *testing.T) {
	dsn := "root:taosdata@/cfg/"

	rows := []struct {
		description  string
		dialect      *Dialect
		openSuccess  bool
		query        string
		querySuccess bool
	}{
		{
			description: "Default driver",
			dialect: &Dialect{
				DSN: dsn,
			},
			openSuccess:  true,
			query:        "SELECT 1",
			querySuccess: true,
		},
		{
			description: "create db",
			dialect: &Dialect{
				DriverName: DriverName,
				DSN:        dsn,
			},
			openSuccess:  true,
			query:        "create database if not exists gorm_test",
			querySuccess: true,
		},
		{
			description: "create table",
			dialect: &Dialect{
				DriverName: DriverName,
				DSN:        dsn,
			},
			openSuccess:  true,
			query:        "create table if not exists gorm_test.test (ts timestamp, value double)",
			querySuccess: true,
		},
		{
			description: "insert data",
			dialect: &Dialect{
				DriverName: DriverName,
				DSN:        dsn,
			},
			openSuccess:  true,
			query:        "insert into gorm_test.test values (now,12)",
			querySuccess: true,
		},
		{
			description: "query data",
			dialect: &Dialect{
				DriverName: DriverName,
				DSN:        dsn,
			},
			openSuccess:  true,
			query:        "select * from gorm_test.test limit 1",
			querySuccess: true,
		},
		{
			description: "syntax error",
			dialect: &Dialect{
				DriverName: DriverName,
				DSN:        dsn,
			},
			openSuccess:  true,
			query:        "select * rfom gorm_test.test limit 1",
			querySuccess: false,
		},
	}
	for rowIndex, row := range rows {
		t.Run(fmt.Sprintf("%d/%s", rowIndex, row.description), func(t *testing.T) {
			db, err := gorm.Open(row.dialect, &gorm.Config{})
			if !row.openSuccess {
				if err == nil {
					t.Errorf("Expected Open to fail.")
				}
				return
			}
			if err != nil {
				t.Errorf("Expected Open to succeed; got error: %v", err)
				return
			}
			if db == nil {
				t.Errorf("Expected db to be non-nil.")
				return
			}
			if row.query != "" {
				err = db.Exec(row.query).Error
				if !row.querySuccess {
					if err == nil {
						t.Errorf("Expected query to fail.")
					}
					return
				}

				if err != nil {
					t.Errorf("Expected query to succeed; got error: %v", err)
				}
			}
		})
	}
}

func TestClause(t *testing.T) {
	//create db
	dsnWithoutDB := "root:taosdata@/cfg/?loc=Local"
	nativeDB, err := sql.Open(DriverName, dsnWithoutDB)
	if err != nil {
		t.Errorf("connect db error:%v", err)
		return
	}
	_, err = nativeDB.Exec("create database if not exists gorm_test")
	if err != nil {
		t.Errorf("create database error %v", err)
		return
	}
	nativeDB.Close()
	dsn := "root:taosdata@/cfg/gorm_test?loc=Local"
	db, err := gorm.Open(Open(dsn))
	if err != nil {
		t.Errorf("unexpected error:%v", err)
		return
	}
	db = db.Debug()
	t.Run(fmt.Sprintf("create stable"), func(t *testing.T) {
		//create stable
		stable := create.NewSTable("stb_1", true, []*create.Column{{
			Name:       "ts",
			ColumnType: create.TimestampType,
		}, {
			Name:       "value",
			ColumnType: create.DoubleType,
		}}, []*create.Column{
			{
				Name:       "tbn",
				ColumnType: create.BinaryType,
				Length:     64,
			},
		})
		err = db.Table("stb_1").Clauses(create.NewCreateTableClause([]*create.Table{stable})).Create(map[string]interface{}{}).Error
		if err != nil {
			t.Errorf("create sTable error %v", err)
			return
		}
	})
	t.Run(fmt.Sprintf("create table using sTable"), func(t *testing.T) {
		//create table
		table := create.NewTable("tb_1", true, nil, "stb_1", map[string]interface{}{
			"tbn": "tb_1",
		})
		err = db.Table("tb_1").Clauses(create.NewCreateTableClause([]*create.Table{table})).Create(map[string]interface{}{}).Error
		if err != nil {
			t.Errorf("create table error %v", err)
			return
		}
	})
	now := time.Now()
	randValue := rand.Float64()
	t.Run(fmt.Sprintf("insert data"), func(t *testing.T) {
		//insert data
		err = db.Table("tb_1").Create(map[string]interface{}{
			"ts":    now,
			"value": randValue,
		}).Error
		if err != nil {
			t.Errorf("insert data error %v", err)
			return
		}
	})
	t1 := now.Add(time.Second)
	tRandValue := rand.Float64()
	t.Run(fmt.Sprintf("create table when insert data"), func(t *testing.T) {
		//create table when insert data
		err = db.Table("tb_2").Clauses(using.SetUsing("stb_1", map[string]interface{}{
			"tbn": "tb_2",
		})).Create(map[string]interface{}{
			"ts":    t1,
			"value": tRandValue,
		}).Error
		if err != nil {
			t.Errorf("create table when insert data error %v", err)
			return
		}
	})
	type Data struct {
		TS    time.Time
		Value float64
	}
	t.Run(fmt.Sprintf("find tb_1 data"), func(t *testing.T) {
		//find tb_1 data
		var d Data
		err = db.Table("tb_1").Where("ts = ?", now).Find(&d).Error
		if err != nil {
			t.Errorf("find data error %v", err)
			return
		}
		if d.Value != randValue {
			t.Errorf("expect value %v got %v", randValue, d.Value)
			return
		}
	})
	t.Run(fmt.Sprintf("find tb_2 data"), func(t *testing.T) {
		//find tb_2 data
		var d2 Data
		err = db.Table("tb_2").Where("ts = ?", t1).Find(&d2).Error
		if err != nil {
			t.Errorf("find data error %v", err)
			return
		}
		if d2.Value != tRandValue {
			t.Errorf("expect value %v got %v", tRandValue, d2.Value)
			return
		}
	})
	t.Run(fmt.Sprintf("find by sTable"), func(t *testing.T) {
		//find by sTable
		var d3 Data
		err = db.Table("stb_1").Where("ts = ?", now).Find(&d3).Error
		if err != nil {
			t.Errorf("find data by sTable error %v", err)
			return
		}
		if d3.Value != randValue {
			t.Errorf("expect value %v got %v", randValue, d3.Value)
			return
		}
	})
	t2 := now.Add(time.Second * 2)
	t3 := now.Add(time.Second * 3)
	v1 := 11
	v2 := 12
	v3 := 13
	//aggregate query
	t.Run(fmt.Sprintf("aggregate insert data"), func(t *testing.T) {
		err = db.Table("tb_aggregate").Clauses(using.SetUsing("stb_1", map[string]interface{}{
			"tbn": "tb_aggregate",
		})).Create([]map[string]interface{}{
			{
				"ts":    t1,
				"value": v1,
			}, {
				"ts":    t2,
				"value": v2,
			}, {
				"ts":    t3,
				"value": v3,
			},
		}).Error
		if err != nil {
			t.Errorf("create table when insert data error %v", err)
			return
		}
	})
	t.Run(fmt.Sprintf("aggregate query: avg"), func(t *testing.T) {
		var result []map[string]interface{}
		err = db.Table("tb_aggregate").Select("avg(value) as v").Where("ts >= ? and ts <= ?", now.Add(time.Second), now.Add(time.Second*3)).Find(&result).Error
		if err != nil {
			t.Errorf("aggregate query error %v", err)
			return
		}
		expectR1 := []map[string]interface{}{
			{
				"v": float64(12),
			},
		}
		if !resultMapEqual(expectR1, result) {
			t.Errorf("expect %v got %v", expectR1, result)
			return
		}
	})
	t.Run(fmt.Sprintf("aggregate query: time window"), func(t *testing.T) {
		var result2 []map[string]interface{}
		windowD, err := window.NewDurationFromTimeDuration(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		err = db.Table("tb_aggregate").
			Select("max(value) as v").
			Where("ts >= ? and ts <= ?", now.Add(time.Second), now.Add(time.Second*4)).
			Clauses(
				window.SetInterval(*windowD),
				fill.SetFill(fill.FillNull),
			).
			Find(&result2).Error
		if err != nil {
			t.Errorf("aggregate query error %v", err)
			return
		}
		expectR2 := []map[string]interface{}{
			{
				"ts": now.Add(time.Second),
				"v":  float64(11),
			},
			{
				"ts": now.Add(time.Second * 2),
				"v":  float64(12),
			},
			{
				"ts": now.Add(time.Second * 3),
				"v":  float64(13),
			},
			{
				"ts": now.Add(time.Second * 4),
				"v":  nil,
			},
		}
		if !resultMapEqual(result2, expectR2) {
			t.Errorf("aggregate query expect %v got %v", result2, expectR2)
			return
		}
	})
}

func resultMapEqual(m1, m2 []map[string]interface{}) bool {
	if len(m1) != len(m2) {
		return false
	}
	for i := range m1 {
		if len(m1[i]) != len(m2[i]) {
			return false
		}

	}
	for i, m := range m1 {
		for s, v := range m {
			_, ok := m2[i][s].(time.Time)
			if ok {
				continue
			}
			if m2[i][s] != v {
				return false
			}
		}
	}
	return true
}

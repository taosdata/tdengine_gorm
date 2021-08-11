package slimit

import (
	"gorm.io/gorm/clause"
	"strconv"
)

// SLimit limit clause
type SLimit struct {
	Limit  int
	Offset int
}

// Name SLIMIT clause name
func (limit SLimit) Name() string {
	return "SLIMIT"
}

// Build SLIMIT clause
func (limit SLimit) Build(builder clause.Builder) {
	if limit.Limit > 0 {
		builder.WriteString("SLIMIT ")
		builder.WriteString(strconv.Itoa(limit.Limit))
	}
	if limit.Offset > 0 {
		if limit.Limit > 0 {
			builder.WriteString(" ")
		}
		builder.WriteString("SOFFSET ")
		builder.WriteString(strconv.Itoa(limit.Offset))
	}
}

// MergeClause merge SLIMIT by clauses
func (limit SLimit) MergeClause(clause *clause.Clause) {
	clause.Name = ""
	if v, ok := clause.Expression.(SLimit); ok {
		if limit.Limit == 0 && v.Limit != 0 {
			limit.Limit = v.Limit
		}

		if limit.Offset == 0 && v.Offset > 0 {
			limit.Offset = v.Offset
		} else if limit.Offset < 0 {
			limit.Offset = 0
		}
	}
	clause.Expression = limit
}

//SetSLimit SLimit clause
func SetSLimit(limit, offset int) SLimit {
	return SLimit{
		Limit:  limit,
		Offset: offset,
	}
}

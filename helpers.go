package psqlfmt

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func sprintf(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}

// quoteIdent quotes an identifier if it needs quoting.
func quoteIdent(name string) string {
	if name == "" {
		return name
	}

	// Check if quoting is needed
	needsQuote := false
	for i, c := range name {
		if i == 0 {
			if (c < 'a' || c > 'z') && c != '_' {
				needsQuote = true
				break
			}
		} else {
			if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
				needsQuote = true
				break
			}
		}
	}

	// Check if it's a reserved keyword
	if !needsQuote && isReservedKeyword(name) {
		needsQuote = true
	}

	if needsQuote {
		return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
	}

	return name
}

func isReservedKeyword(name string) bool {
	switch strings.ToUpper(name) {
	case "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC",
		"ASYMMETRIC", "BOTH", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN",
		"CONSTRAINT", "CREATE", "CURRENT_CATALOG", "CURRENT_DATE",
		"CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
		"DEFAULT", "DEFERRABLE", "DESC", "DISTINCT", "DO", "ELSE", "END",
		"EXCEPT", "FALSE", "FETCH", "FOR", "FOREIGN", "FROM", "GRANT",
		"GROUP", "HAVING", "IN", "INITIALLY", "INTERSECT", "INTO",
		"LATERAL", "LEADING", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "NOT",
		"NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "PLACING",
		"PRIMARY", "REFERENCES", "RETURNING", "SELECT", "SESSION_USER",
		"SOME", "SYMMETRIC", "TABLE", "THEN", "TO", "TRAILING", "TRUE",
		"UNION", "UNIQUE", "USER", "USING", "VARIADIC", "WHEN", "WHERE",
		"WINDOW", "WITH":
		return true
	}
	return false
}

// formatFallback uses Deparse for nodes we don't explicitly handle.
func (f *fmter) formatFallback(node *pg_query.Node) {
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{Stmt: node},
		},
	}

	sql, err := pg_query.Deparse(result)
	if err != nil {
		f.write("/* unsupported node */")
		return
	}

	f.write(sql)
}

func (f *fmter) formatCommaSeparated(nodes []*pg_query.Node, depth int) {
	for i, n := range nodes {
		if i > 0 {
			f.write(",")
			f.newline(depth)
		}
		f.formatNode(n, depth)
	}
}

func (f *fmter) formatCommaSeparatedInline(nodes []*pg_query.Node, depth int) {
	for i, n := range nodes {
		if i > 0 {
			f.write(", ")
		}
		f.formatNode(n, depth)
	}
}

func typeName(tn *pg_query.TypeName) string {
	if tn == nil {
		return ""
	}

	var parts []string
	for _, n := range tn.Names {
		if s, ok := n.Node.(*pg_query.Node_String_); ok {
			name := s.String_.Sval
			// Skip pg_catalog schema prefix
			if name == "pg_catalog" {
				continue
			}
			parts = append(parts, name)
		}
	}

	name := strings.Join(parts, ".")

	// Map internal type names to SQL standard names
	name = mapTypeName(name, tn)

	return name
}

func mapTypeName(name string, tn *pg_query.TypeName) string {
	typmods := tn.Typmods

	switch name {
	case "int2":
		return "SMALLINT"
	case "int4":
		return "INTEGER"
	case "int8":
		return "BIGINT"
	case "float4":
		return "REAL"
	case "float8":
		return "DOUBLE PRECISION"
	case "bool":
		return "BOOLEAN"
	case "varchar":
		if len(typmods) > 0 {
			return sprintf("VARCHAR(%s)", typemodStr(typmods))
		}
		return "VARCHAR"
	case "bpchar":
		if len(typmods) > 0 {
			return sprintf("CHAR(%s)", typemodStr(typmods))
		}
		return "CHAR"
	case "numeric":
		if len(typmods) > 0 {
			return sprintf("NUMERIC(%s)", typemodStr(typmods))
		}
		return "NUMERIC"
	case "text":
		return "TEXT"
	case "timestamp":
		return "TIMESTAMP"
	case "timestamptz":
		return "TIMESTAMPTZ"
	case "date":
		return "DATE"
	case "time":
		return "TIME"
	case "timetz":
		return "TIMETZ"
	case "interval":
		return "INTERVAL"
	case "json":
		return "JSON"
	case "jsonb":
		return "JSONB"
	case "uuid":
		return "UUID"
	case "bytea":
		return "BYTEA"
	case "inet":
		return "INET"
	case "cidr":
		return "CIDR"
	case "macaddr":
		return "MACADDR"
	case "xml":
		return "XML"
	case "money":
		return "MONEY"
	case "bit":
		if len(typmods) > 0 {
			return sprintf("BIT(%s)", typemodStr(typmods))
		}
		return "BIT"
	case "varbit":
		if len(typmods) > 0 {
			return sprintf("BIT VARYING(%s)", typemodStr(typmods))
		}
		return "BIT VARYING"
	}

	if tn.ArrayBounds != nil {
		return strings.ToUpper(name) + "[]"
	}

	return strings.ToUpper(name)
}

func typemodStr(typmods []*pg_query.Node) string {
	var parts []string
	for _, m := range typmods {
		switch v := m.Node.(type) {
		case *pg_query.Node_Integer:
			// For varchar/char, the typmod includes the header size (4 bytes)
			parts = append(parts, sprintf("%d", v.Integer.Ival))
		case *pg_query.Node_AConst:
			switch val := v.AConst.Val.(type) {
			case *pg_query.A_Const_Ival:
				parts = append(parts, sprintf("%d", val.Ival.Ival))
			case *pg_query.A_Const_Fval:
				parts = append(parts, val.Fval.Fval)
			case *pg_query.A_Const_Sval:
				parts = append(parts, val.Sval.Sval)
			}
		}
	}
	return strings.Join(parts, ", ")
}

func (f *fmter) formatTypeName(tn *pg_query.TypeName, _ int) {
	name := typeName(tn)

	if tn.ArrayBounds != nil && !strings.HasSuffix(name, "[]") {
		name += "[]"
	}

	f.write(name)
}

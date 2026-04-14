package psqlfmt

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

const defaultIndent = "  "

// Format parses and formats a SQL string.
func Format(sql string) (string, error) {
	stmts, err := pg_query.SplitWithParser(sql, true)
	if err != nil {
		return "", err
	}

	var results []string

	for _, stmt := range stmts {
		if stmt == "" {
			continue
		}

		result, err := pg_query.Parse(stmt)
		if err != nil {
			return "", err
		}

		for _, rawStmt := range result.Stmts {
			f := &fmter{}
			f.formatNode(rawStmt.Stmt, 0)
			results = append(results, f.String())
		}
	}

	return strings.Join(results, ";\n\n") + ";\n", nil
}

type fmter struct {
	buf strings.Builder
}

func (f *fmter) String() string {
	return f.buf.String()
}

func (f *fmter) write(s string) {
	f.buf.WriteString(s)
}

func (f *fmter) writef(format string, args ...any) {
	f.buf.WriteString(strings.TrimRight(sprintf(format, args...), " "))
}

func (f *fmter) newline(depth int) {
	f.buf.WriteByte('\n')
	for i := 0; i < depth; i++ {
		f.buf.WriteString(defaultIndent)
	}
}

func (f *fmter) formatNode(node *pg_query.Node, depth int) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		f.formatSelect(n.SelectStmt, depth)
	case *pg_query.Node_InsertStmt:
		f.formatInsert(n.InsertStmt, depth)
	case *pg_query.Node_UpdateStmt:
		f.formatUpdate(n.UpdateStmt, depth)
	case *pg_query.Node_DeleteStmt:
		f.formatDelete(n.DeleteStmt, depth)
	case *pg_query.Node_CreateStmt:
		f.formatCreateTable(n.CreateStmt, depth)
	case *pg_query.Node_IndexStmt:
		f.formatCreateIndex(n.IndexStmt, depth)
	case *pg_query.Node_DropStmt:
		f.formatDrop(n.DropStmt, depth)
	case *pg_query.Node_AlterTableStmt:
		f.formatAlterTable(n.AlterTableStmt, depth)
	case *pg_query.Node_CommentStmt:
		f.formatComment(n.CommentStmt, depth)
	case *pg_query.Node_CreateFunctionStmt:
		f.formatCreateFunction(n.CreateFunctionStmt, depth)
	case *pg_query.Node_ColumnRef:
		f.formatColumnRef(n.ColumnRef)
	case *pg_query.Node_AConst:
		f.formatAConst(n.AConst)
	case *pg_query.Node_AExpr:
		f.formatAExpr(n.AExpr, depth)
	case *pg_query.Node_BoolExpr:
		f.formatBoolExpr(n.BoolExpr, depth)
	case *pg_query.Node_FuncCall:
		f.formatFuncCall(n.FuncCall, depth)
	case *pg_query.Node_TypeCast:
		f.formatTypeCast(n.TypeCast, depth)
	case *pg_query.Node_SubLink:
		f.formatSubLink(n.SubLink, depth)
	case *pg_query.Node_ResTarget:
		f.formatResTarget(n.ResTarget, depth)
	case *pg_query.Node_RangeVar:
		f.formatRangeVar(n.RangeVar)
	case *pg_query.Node_JoinExpr:
		f.formatJoinExpr(n.JoinExpr, depth)
	case *pg_query.Node_SortBy:
		f.formatSortBy(n.SortBy, depth)
	case *pg_query.Node_RangeSubselect:
		f.formatRangeSubselect(n.RangeSubselect, depth)
	case *pg_query.Node_NullTest:
		f.formatNullTest(n.NullTest, depth)
	case *pg_query.Node_CaseExpr:
		f.formatCaseExpr(n.CaseExpr, depth)
	case *pg_query.Node_CaseWhen:
		f.formatCaseWhen(n.CaseWhen, depth)
	case *pg_query.Node_CoalesceExpr:
		f.formatCoalesceExpr(n.CoalesceExpr, depth)
	case *pg_query.Node_ParamRef:
		f.formatParamRef(n.ParamRef)
	case *pg_query.Node_CollateClause:
		f.formatCollateClause(n.CollateClause, depth)
	case *pg_query.Node_String_:
		f.write(quoteIdent(n.String_.Sval))
	case *pg_query.Node_Integer:
		f.writef("%d", n.Integer.Ival)
	case *pg_query.Node_Float:
		f.write(n.Float.Fval)
	case *pg_query.Node_Boolean:
		if n.Boolean.Boolval {
			f.write("TRUE")
		} else {
			f.write("FALSE")
		}
	case *pg_query.Node_List:
		f.formatList(n.List, depth)
	case *pg_query.Node_RangeFunction:
		f.formatRangeFunction(n.RangeFunction, depth)
	case *pg_query.Node_WithClause:
		f.formatWithClause(n.WithClause, depth)
	case *pg_query.Node_CommonTableExpr:
		f.formatCTE(n.CommonTableExpr, depth)
	case *pg_query.Node_WindowDef:
		f.formatWindowDef(n.WindowDef, depth)
	case *pg_query.Node_AArrayExpr:
		f.formatArrayExpr(n.AArrayExpr, depth)
	case *pg_query.Node_RowExpr:
		f.formatRowExpr(n.RowExpr, depth)
	case *pg_query.Node_AStar:
		f.write("*")
	case *pg_query.Node_AIndirection:
		f.formatAIndirection(n.AIndirection, depth)
	case *pg_query.Node_BitString:
		f.write(n.BitString.Bsval)
	case *pg_query.Node_BooleanTest:
		f.formatBooleanTest(n.BooleanTest, depth)
	case *pg_query.Node_SetToDefault:
		f.write("DEFAULT")
	case *pg_query.Node_TruncateStmt:
		f.formatTruncate(n.TruncateStmt, depth)
	default:
		// Fallback: use Deparse for unsupported nodes
		f.formatFallback(node)
	}
}

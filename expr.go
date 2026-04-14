package psqlfmt

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func (f *fmter) formatColumnRef(cr *pg_query.ColumnRef) {
	for i, field := range cr.Fields {
		if i > 0 {
			f.write(".")
		}
		switch n := field.Node.(type) {
		case *pg_query.Node_String_:
			f.write(quoteIdent(n.String_.Sval))
		case *pg_query.Node_AStar:
			f.write("*")
		}
	}
}

func (f *fmter) formatAConst(ac *pg_query.A_Const) {
	switch v := ac.Val.(type) {
	case *pg_query.A_Const_Ival:
		f.writef("%d", v.Ival.Ival)
	case *pg_query.A_Const_Fval:
		f.write(v.Fval.Fval)
	case *pg_query.A_Const_Sval:
		f.formatStringConst(v.Sval.Sval)
	case *pg_query.A_Const_Boolval:
		if v.Boolval.Boolval {
			f.write("TRUE")
		} else {
			f.write("FALSE")
		}
	case *pg_query.A_Const_Bsval:
		f.formatBitString(v.Bsval.Bsval)
	}

	if ac.Isnull {
		f.write("NULL")
	}
}

// formatStringConst outputs a string constant, using E'...' syntax when
// the string contains characters that need backslash escaping.
func (f *fmter) formatStringConst(s string) {
	if needsEscape(s) {
		f.write("E'")
		for _, c := range s {
			switch c {
			case '\'':
				f.write("\\'")
			case '\\':
				f.write("\\\\")
			case '\n':
				f.write("\\n")
			case '\r':
				f.write("\\r")
			case '\t':
				f.write("\\t")
			case '\b':
				f.write("\\b")
			case '\f':
				f.write("\\f")
			default:
				f.buf.WriteRune(c)
			}
		}
		f.write("'")
	} else {
		f.write("'" + strings.ReplaceAll(s, "'", "''") + "'")
	}
}

func needsEscape(s string) bool {
	for _, c := range s {
		switch c {
		case '\n', '\r', '\t', '\b', '\f', '\\':
			return true
		}
	}
	return false
}

// formatBitString outputs B'...' or X'...' bit-string constants.
// pg_query stores them as "b1001" or "x1FF".
func (f *fmter) formatBitString(bsval string) {
	if len(bsval) == 0 {
		return
	}

	prefix := bsval[0]
	body := bsval[1:]

	switch prefix {
	case 'b':
		f.write("B'" + body + "'")
	case 'x':
		f.write("X'" + body + "'")
	default:
		f.write("'" + body + "'")
	}
}

func (f *fmter) formatAExpr(expr *pg_query.A_Expr, depth int) {
	switch expr.Kind {
	case pg_query.A_Expr_Kind_AEXPR_OP:
		if expr.Lexpr == nil {
			// Unary prefix operator (e.g. ~1, -1)
			if len(expr.Name) > 0 {
				f.write(nodeStr(expr.Name[0]))
			}
			f.formatNode(expr.Rexpr, depth)
		} else {
			f.formatNode(expr.Lexpr, depth)
			if len(expr.Name) > 0 {
				op := nodeStr(expr.Name[0])
				f.write(" " + op + " ")
			}
			f.formatNode(expr.Rexpr, depth)
		}
	case pg_query.A_Expr_Kind_AEXPR_OP_ANY:
		f.formatNode(expr.Lexpr, depth)
		if len(expr.Name) > 0 {
			op := nodeStr(expr.Name[0])
			f.write(" " + op + " ANY (")
		}
		f.formatNode(expr.Rexpr, depth)
		f.write(")")
	case pg_query.A_Expr_Kind_AEXPR_OP_ALL:
		f.formatNode(expr.Lexpr, depth)
		if len(expr.Name) > 0 {
			op := nodeStr(expr.Name[0])
			f.write(" " + op + " ALL (")
		}
		f.formatNode(expr.Rexpr, depth)
		f.write(")")
	case pg_query.A_Expr_Kind_AEXPR_IN:
		f.formatNode(expr.Lexpr, depth)
		if len(expr.Name) > 0 {
			op := nodeStr(expr.Name[0])
			if op == "=" {
				f.write(" IN (")
			} else {
				f.write(" NOT IN (")
			}
		}
		if list, ok := expr.Rexpr.Node.(*pg_query.Node_List); ok {
			f.formatCommaSeparatedInline(list.List.Items, depth)
		} else {
			f.formatNode(expr.Rexpr, depth)
		}
		f.write(")")
	case pg_query.A_Expr_Kind_AEXPR_LIKE:
		f.formatNode(expr.Lexpr, depth)
		if len(expr.Name) > 0 {
			op := nodeStr(expr.Name[0])
			if op == "~~" {
				f.write(" LIKE ")
			} else {
				f.write(" NOT LIKE ")
			}
		}
		f.formatNode(expr.Rexpr, depth)
	case pg_query.A_Expr_Kind_AEXPR_ILIKE:
		f.formatNode(expr.Lexpr, depth)
		if len(expr.Name) > 0 {
			op := nodeStr(expr.Name[0])
			if op == "~~*" {
				f.write(" ILIKE ")
			} else {
				f.write(" NOT ILIKE ")
			}
		}
		f.formatNode(expr.Rexpr, depth)
	case pg_query.A_Expr_Kind_AEXPR_BETWEEN:
		f.formatNode(expr.Lexpr, depth)
		f.write(" BETWEEN ")
		if list, ok := expr.Rexpr.Node.(*pg_query.Node_List); ok && len(list.List.Items) == 2 {
			f.formatNode(list.List.Items[0], depth)
			f.write(" AND ")
			f.formatNode(list.List.Items[1], depth)
		}
	case pg_query.A_Expr_Kind_AEXPR_NOT_BETWEEN:
		f.formatNode(expr.Lexpr, depth)
		f.write(" NOT BETWEEN ")
		if list, ok := expr.Rexpr.Node.(*pg_query.Node_List); ok && len(list.List.Items) == 2 {
			f.formatNode(list.List.Items[0], depth)
			f.write(" AND ")
			f.formatNode(list.List.Items[1], depth)
		}
	case pg_query.A_Expr_Kind_AEXPR_SIMILAR:
		f.formatNode(expr.Lexpr, depth)
		f.write(" SIMILAR TO ")
		f.formatNode(expr.Rexpr, depth)
	case pg_query.A_Expr_Kind_AEXPR_NULLIF:
		f.write("NULLIF(")
		f.formatNode(expr.Lexpr, depth)
		f.write(", ")
		f.formatNode(expr.Rexpr, depth)
		f.write(")")
	case pg_query.A_Expr_Kind_AEXPR_DISTINCT:
		f.formatNode(expr.Lexpr, depth)
		f.write(" IS DISTINCT FROM ")
		f.formatNode(expr.Rexpr, depth)
	case pg_query.A_Expr_Kind_AEXPR_NOT_DISTINCT:
		f.formatNode(expr.Lexpr, depth)
		f.write(" IS NOT DISTINCT FROM ")
		f.formatNode(expr.Rexpr, depth)
	default:
		// Fallback for unknown expression kinds
		f.formatNode(expr.Lexpr, depth)
		if len(expr.Name) > 0 {
			f.write(" " + nodeStr(expr.Name[0]) + " ")
		}
		f.formatNode(expr.Rexpr, depth)
	}
}

func nodeStr(node *pg_query.Node) string {
	if s, ok := node.Node.(*pg_query.Node_String_); ok {
		return s.String_.Sval
	}
	return ""
}

func (f *fmter) formatBoolExpr(expr *pg_query.BoolExpr, depth int) {
	switch expr.Boolop {
	case pg_query.BoolExprType_AND_EXPR:
		for i, arg := range expr.Args {
			if i > 0 {
				f.newline(depth)
				f.write("AND ")
			}
			f.formatBoolArg(arg, depth)
		}
	case pg_query.BoolExprType_OR_EXPR:
		for i, arg := range expr.Args {
			if i > 0 {
				f.newline(depth)
				f.write("OR ")
			}
			f.formatBoolArg(arg, depth)
		}
	case pg_query.BoolExprType_NOT_EXPR:
		f.write("NOT ")
		if len(expr.Args) > 0 {
			f.formatBoolArg(expr.Args[0], depth)
		}
	}
}

func (f *fmter) formatBoolArg(node *pg_query.Node, depth int) {
	// Wrap OR inside AND with parentheses for clarity
	if be, ok := node.Node.(*pg_query.Node_BoolExpr); ok {
		if be.BoolExpr.Boolop == pg_query.BoolExprType_OR_EXPR {
			f.write("(")
			f.formatNode(node, depth+1)
			f.write(")")
			return
		}
	}
	f.formatNode(node, depth)
}

func (f *fmter) formatFuncCall(fc *pg_query.FuncCall, depth int) {
	// Build function name
	var parts []string
	for _, n := range fc.Funcname {
		if s, ok := n.Node.(*pg_query.Node_String_); ok {
			parts = append(parts, s.String_.Sval)
		}
	}
	funcName := strings.Join(parts, ".")

	// Handle aggregate functions and common functions with uppercase
	f.write(strings.ToUpper(funcName))
	f.write("(")

	if fc.AggDistinct {
		f.write("DISTINCT ")
	}

	if fc.AggStar {
		f.write("*")
	} else {
		f.formatCommaSeparatedInline(fc.Args, depth)
	}

	if len(fc.AggOrder) > 0 {
		f.write(" ORDER BY ")
		f.formatCommaSeparatedInline(fc.AggOrder, depth)
	}

	f.write(")")

	if fc.AggFilter != nil {
		f.write(" FILTER (WHERE ")
		f.formatNode(fc.AggFilter, depth)
		f.write(")")
	}

	if fc.Over != nil {
		f.write(" OVER ")
		f.formatWindowDef(fc.Over, depth)
	}
}

func (f *fmter) formatTypeCast(tc *pg_query.TypeCast, depth int) {
	// Check for special CAST syntax
	tn := typeName(tc.TypeName)

	f.write("CAST(")
	f.formatNode(tc.Arg, depth)
	f.write(" AS ")
	f.write(tn)

	if tc.TypeName.ArrayBounds != nil && !strings.HasSuffix(tn, "[]") {
		f.write("[]")
	}

	f.write(")")
}

func (f *fmter) formatSubLink(sl *pg_query.SubLink, depth int) {
	switch sl.SubLinkType {
	case pg_query.SubLinkType_EXISTS_SUBLINK:
		f.write("EXISTS (")
		f.newline(depth + 1)
		f.formatNode(sl.Subselect, depth+1)
		f.newline(depth)
		f.write(")")
	case pg_query.SubLinkType_ANY_SUBLINK:
		f.formatNode(sl.Testexpr, depth)
		f.write(" IN (")
		f.newline(depth + 1)
		f.formatNode(sl.Subselect, depth+1)
		f.newline(depth)
		f.write(")")
	case pg_query.SubLinkType_ALL_SUBLINK:
		f.formatNode(sl.Testexpr, depth)
		if len(sl.OperName) > 0 {
			f.write(" " + nodeStr(sl.OperName[0]) + " ALL (")
		} else {
			f.write(" ALL (")
		}
		f.newline(depth + 1)
		f.formatNode(sl.Subselect, depth+1)
		f.newline(depth)
		f.write(")")
	case pg_query.SubLinkType_EXPR_SUBLINK:
		f.write("(")
		f.newline(depth + 1)
		f.formatNode(sl.Subselect, depth+1)
		f.newline(depth)
		f.write(")")
	default:
		f.write("(")
		f.formatNode(sl.Subselect, depth)
		f.write(")")
	}
}

func (f *fmter) formatResTarget(rt *pg_query.ResTarget, depth int) {
	if rt.Val != nil {
		f.formatNode(rt.Val, depth)
	}

	if rt.Name != "" {
		f.write(" AS ")
		f.write(quoteIdent(rt.Name))
	}
}

func (f *fmter) formatRangeVar(rv *pg_query.RangeVar) {
	if rv.Schemaname != "" {
		f.write(quoteIdent(rv.Schemaname))
		f.write(".")
	}

	f.write(quoteIdent(rv.Relname))

	if rv.Alias != nil {
		f.write(" AS ")
		f.write(quoteIdent(rv.Alias.Aliasname))
	}
}

func (f *fmter) formatJoinExpr(je *pg_query.JoinExpr, depth int) {
	f.formatNode(je.Larg, depth)
	f.newline(depth)

	switch je.Jointype {
	case pg_query.JoinType_JOIN_INNER:
		if je.IsNatural {
			f.write("NATURAL JOIN ")
		} else if je.Quals != nil {
			f.write("JOIN ")
		} else if len(je.UsingClause) > 0 {
			f.write("JOIN ")
		} else {
			f.write("CROSS JOIN ")
		}
	case pg_query.JoinType_JOIN_LEFT:
		f.write("LEFT JOIN ")
	case pg_query.JoinType_JOIN_FULL:
		f.write("FULL JOIN ")
	case pg_query.JoinType_JOIN_RIGHT:
		f.write("RIGHT JOIN ")
	}

	f.formatNode(je.Rarg, depth)

	if je.Quals != nil {
		f.newline(depth + 1)
		f.write("ON ")
		f.formatNode(je.Quals, depth+1)
	}

	if len(je.UsingClause) > 0 {
		f.write(" USING (")
		f.formatCommaSeparatedInline(je.UsingClause, depth)
		f.write(")")
	}
}

func (f *fmter) formatSortBy(sb *pg_query.SortBy, depth int) {
	f.formatNode(sb.Node, depth)

	switch sb.SortbyDir {
	case pg_query.SortByDir_SORTBY_ASC:
		f.write(" ASC")
	case pg_query.SortByDir_SORTBY_DESC:
		f.write(" DESC")
	}

	switch sb.SortbyNulls {
	case pg_query.SortByNulls_SORTBY_NULLS_FIRST:
		f.write(" NULLS FIRST")
	case pg_query.SortByNulls_SORTBY_NULLS_LAST:
		f.write(" NULLS LAST")
	}
}

func (f *fmter) formatRangeSubselect(rs *pg_query.RangeSubselect, depth int) {
	if rs.Lateral {
		f.write("LATERAL ")
	}

	f.write("(")
	f.newline(depth + 1)
	f.formatNode(rs.Subquery, depth+1)
	f.newline(depth)
	f.write(")")

	if rs.Alias != nil {
		f.write(" AS ")
		f.write(quoteIdent(rs.Alias.Aliasname))
	}
}

func (f *fmter) formatNullTest(nt *pg_query.NullTest, depth int) {
	f.formatNode(nt.Arg, depth)
	switch nt.Nulltesttype {
	case pg_query.NullTestType_IS_NULL:
		f.write(" IS NULL")
	case pg_query.NullTestType_IS_NOT_NULL:
		f.write(" IS NOT NULL")
	}
}

func (f *fmter) formatCaseExpr(ce *pg_query.CaseExpr, depth int) {
	f.write("CASE")

	if ce.Arg != nil {
		f.write(" ")
		f.formatNode(ce.Arg, depth)
	}

	for _, arg := range ce.Args {
		f.newline(depth + 1)
		f.formatNode(arg, depth+1)
	}

	if ce.Defresult != nil {
		f.newline(depth + 1)
		f.write("ELSE ")
		f.formatNode(ce.Defresult, depth+1)
	}

	f.newline(depth)
	f.write("END")
}

func (f *fmter) formatCaseWhen(cw *pg_query.CaseWhen, depth int) {
	f.write("WHEN ")
	f.formatNode(cw.Expr, depth)
	f.write(" THEN ")
	f.formatNode(cw.Result, depth)
}

func (f *fmter) formatCoalesceExpr(ce *pg_query.CoalesceExpr, depth int) {
	f.write("COALESCE(")
	f.formatCommaSeparatedInline(ce.Args, depth)
	f.write(")")
}

func (f *fmter) formatParamRef(pr *pg_query.ParamRef) {
	if pr.Number > 0 {
		f.writef("$%d", pr.Number)
	} else {
		f.write("$0")
	}
}

func (f *fmter) formatList(list *pg_query.List, depth int) {
	for i, item := range list.Items {
		if i > 0 {
			f.write(".")
		}
		f.formatNode(item, depth)
	}
}

func (f *fmter) formatRangeFunction(rf *pg_query.RangeFunction, depth int) {
	if rf.Lateral {
		f.write("LATERAL ")
	}

	for _, fn := range rf.Functions {
		if list, ok := fn.Node.(*pg_query.Node_List); ok {
			for _, item := range list.List.Items {
				if item.Node == nil {
					continue
				}
				f.formatNode(item, depth)
			}
		} else {
			f.formatNode(fn, depth)
		}
	}

	if rf.Alias != nil {
		f.write(" AS ")
		f.write(quoteIdent(rf.Alias.Aliasname))
		if len(rf.Alias.Colnames) > 0 {
			f.write("(")
			for i, col := range rf.Alias.Colnames {
				if i > 0 {
					f.write(", ")
				}
				f.formatNode(col, depth)
			}
			f.write(")")
		}
	}
}

func (f *fmter) formatWithClause(wc *pg_query.WithClause, depth int) {
	f.write("WITH")

	if wc.Recursive {
		f.write(" RECURSIVE")
	}

	for i, cte := range wc.Ctes {
		if i > 0 {
			f.write(",")
		}
		f.newline(depth + 1)
		f.formatNode(cte, depth+1)
	}
}

func (f *fmter) formatCTE(cte *pg_query.CommonTableExpr, depth int) {
	f.write(quoteIdent(cte.Ctename))

	if len(cte.Aliascolnames) > 0 {
		f.write(" (")
		for i, col := range cte.Aliascolnames {
			if i > 0 {
				f.write(", ")
			}
			f.formatNode(col, depth)
		}
		f.write(")")
	}

	f.write(" AS (")
	f.newline(depth + 1)
	f.formatNode(cte.Ctequery, depth+1)
	f.newline(depth)
	f.write(")")
}

func (f *fmter) formatWindowDef(wd *pg_query.WindowDef, depth int) {
	if wd.Name != "" {
		f.write(quoteIdent(wd.Name))
		f.write(" AS ")
	}

	f.write("(")

	needSpace := false

	if wd.Refname != "" {
		f.write(quoteIdent(wd.Refname))
		needSpace = true
	}

	if len(wd.PartitionClause) > 0 {
		if needSpace {
			f.write(" ")
		}
		f.write("PARTITION BY ")
		f.formatCommaSeparatedInline(wd.PartitionClause, depth)
		needSpace = true
	}

	if len(wd.OrderClause) > 0 {
		if needSpace {
			f.write(" ")
		}
		f.write("ORDER BY ")
		f.formatCommaSeparatedInline(wd.OrderClause, depth)
	}

	f.write(")")
}

func (f *fmter) formatArrayExpr(ae *pg_query.A_ArrayExpr, depth int) {
	f.write("ARRAY[")
	f.formatCommaSeparatedInline(ae.Elements, depth)
	f.write("]")
}

func (f *fmter) formatRowExpr(re *pg_query.RowExpr, depth int) {
	f.write("ROW(")
	f.formatCommaSeparatedInline(re.Args, depth)
	f.write(")")
}

func (f *fmter) formatAIndirection(ai *pg_query.A_Indirection, depth int) {
	f.formatNode(ai.Arg, depth)
	for _, ind := range ai.Indirection {
		switch n := ind.Node.(type) {
		case *pg_query.Node_String_:
			f.write(".")
			f.write(quoteIdent(n.String_.Sval))
		case *pg_query.Node_AIndices:
			f.write("[")
			if n.AIndices.IsSlice {
				if n.AIndices.Lidx != nil {
					f.formatNode(n.AIndices.Lidx, depth)
				}
				f.write(":")
				if n.AIndices.Uidx != nil {
					f.formatNode(n.AIndices.Uidx, depth)
				}
			} else if n.AIndices.Uidx != nil {
				f.formatNode(n.AIndices.Uidx, depth)
			}
			f.write("]")
		case *pg_query.Node_AStar:
			f.write(".*")
		}
	}
}

func (f *fmter) formatBooleanTest(bt *pg_query.BooleanTest, depth int) {
	f.formatNode(bt.Arg, depth)
	switch bt.Booltesttype {
	case pg_query.BoolTestType_IS_TRUE:
		f.write(" IS TRUE")
	case pg_query.BoolTestType_IS_NOT_TRUE:
		f.write(" IS NOT TRUE")
	case pg_query.BoolTestType_IS_FALSE:
		f.write(" IS FALSE")
	case pg_query.BoolTestType_IS_NOT_FALSE:
		f.write(" IS NOT FALSE")
	case pg_query.BoolTestType_IS_UNKNOWN:
		f.write(" IS UNKNOWN")
	case pg_query.BoolTestType_IS_NOT_UNKNOWN:
		f.write(" IS NOT UNKNOWN")
	}
}

func (f *fmter) formatCollateClause(cc *pg_query.CollateClause, depth int) {
	f.formatNode(cc.Arg, depth)
	f.write(" COLLATE ")

	for i, n := range cc.Collname {
		if i > 0 {
			f.write(".")
		}
		if s, ok := n.Node.(*pg_query.Node_String_); ok {
			f.write("\"" + s.String_.Sval + "\"")
		}
	}
}

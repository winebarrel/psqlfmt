package psqlfmt

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func (f *fmter) formatSelect(stmt *pg_query.SelectStmt, depth int) {
	// Handle set operations (UNION, INTERSECT, EXCEPT)
	if stmt.Op != pg_query.SetOperation_SETOP_NONE {
		f.formatSetOperation(stmt, depth)
		return
	}

	// VALUES clause
	if len(stmt.ValuesLists) > 0 {
		f.formatValuesList(stmt.ValuesLists, depth)
		return
	}

	// WITH clause
	if stmt.WithClause != nil {
		f.formatWithClause(stmt.WithClause, depth)
		f.newline(depth)
	}

	// SELECT
	f.write("SELECT")

	if stmt.DistinctClause != nil {
		f.write(" DISTINCT")
	}

	// Target list
	if len(stmt.TargetList) > 0 {
		f.newline(depth + 1)
		f.formatCommaSeparated(stmt.TargetList, depth+1)
	}

	// FROM
	if len(stmt.FromClause) > 0 {
		f.newline(depth)
		f.write("FROM")
		f.newline(depth + 1)
		for i, from := range stmt.FromClause {
			if i > 0 {
				f.write(",")
				f.newline(depth + 1)
			}
			f.formatNode(from, depth+1)
		}
	}

	// WHERE
	if stmt.WhereClause != nil {
		f.newline(depth)
		f.write("WHERE")
		f.newline(depth + 1)
		f.formatNode(stmt.WhereClause, depth+1)
	}

	// GROUP BY
	if len(stmt.GroupClause) > 0 {
		f.newline(depth)
		f.write("GROUP BY")
		f.newline(depth + 1)
		f.formatCommaSeparated(stmt.GroupClause, depth+1)
	}

	// HAVING
	if stmt.HavingClause != nil {
		f.newline(depth)
		f.write("HAVING")
		f.newline(depth + 1)
		f.formatNode(stmt.HavingClause, depth+1)
	}

	// WINDOW
	if len(stmt.WindowClause) > 0 {
		f.newline(depth)
		f.write("WINDOW")
		f.newline(depth + 1)
		f.formatCommaSeparated(stmt.WindowClause, depth+1)
	}

	// ORDER BY
	if len(stmt.SortClause) > 0 {
		f.newline(depth)
		f.write("ORDER BY")
		f.newline(depth + 1)
		f.formatCommaSeparated(stmt.SortClause, depth+1)
	}

	// LIMIT
	if stmt.LimitCount != nil {
		f.newline(depth)
		f.write("LIMIT ")
		f.formatNode(stmt.LimitCount, depth)
	}

	// OFFSET
	if stmt.LimitOffset != nil {
		f.newline(depth)
		f.write("OFFSET ")
		f.formatNode(stmt.LimitOffset, depth)
	}

	// FOR UPDATE/SHARE
	if len(stmt.LockingClause) > 0 {
		for _, lock := range stmt.LockingClause {
			if lc, ok := lock.Node.(*pg_query.Node_LockingClause); ok {
				f.newline(depth)
				switch lc.LockingClause.Strength {
				case pg_query.LockClauseStrength_LCS_FORUPDATE:
					f.write("FOR UPDATE")
				case pg_query.LockClauseStrength_LCS_FORSHARE:
					f.write("FOR SHARE")
				case pg_query.LockClauseStrength_LCS_FORNOKEYUPDATE:
					f.write("FOR NO KEY UPDATE")
				case pg_query.LockClauseStrength_LCS_FORKEYSHARE:
					f.write("FOR KEY SHARE")
				}
			}
		}
	}
}

func (f *fmter) formatSetOperation(stmt *pg_query.SelectStmt, depth int) {
	f.formatSelect(stmt.Larg, depth)
	f.newline(depth)

	switch stmt.Op {
	case pg_query.SetOperation_SETOP_UNION:
		f.write("UNION")
	case pg_query.SetOperation_SETOP_INTERSECT:
		f.write("INTERSECT")
	case pg_query.SetOperation_SETOP_EXCEPT:
		f.write("EXCEPT")
	}

	if stmt.All {
		f.write(" ALL")
	}

	f.newline(depth)
	f.formatSelect(stmt.Rarg, depth)
}

func (f *fmter) formatValuesList(valuesLists []*pg_query.Node, depth int) {
	f.write("VALUES")
	for i, vl := range valuesLists {
		if i > 0 {
			f.write(",")
		}
		f.newline(depth + 1)
		f.write("(")
		if list, ok := vl.Node.(*pg_query.Node_List); ok {
			f.formatCommaSeparatedInline(list.List.Items, depth+1)
		}
		f.write(")")
	}
}

func (f *fmter) formatInsert(stmt *pg_query.InsertStmt, depth int) {
	// WITH clause
	if stmt.WithClause != nil {
		f.formatWithClause(stmt.WithClause, depth)
		f.newline(depth)
	}

	f.write("INSERT INTO ")
	f.formatRangeVar(stmt.Relation)

	// Column list
	if len(stmt.Cols) > 0 {
		f.write(" (")
		for i, col := range stmt.Cols {
			if i > 0 {
				f.write(", ")
			}
			if rt, ok := col.Node.(*pg_query.Node_ResTarget); ok {
				f.write(quoteIdent(rt.ResTarget.Name))
			}
		}
		f.write(")")
	}

	// SELECT or VALUES
	if stmt.SelectStmt != nil {
		f.newline(depth)
		f.formatNode(stmt.SelectStmt, depth)
	}

	// ON CONFLICT
	if stmt.OnConflictClause != nil {
		f.formatOnConflict(stmt.OnConflictClause, depth)
	}

	// RETURNING
	if len(stmt.ReturningList) > 0 {
		f.newline(depth)
		f.write("RETURNING")
		f.newline(depth + 1)
		f.formatCommaSeparated(stmt.ReturningList, depth+1)
	}
}

func (f *fmter) formatOnConflict(oc *pg_query.OnConflictClause, depth int) {
	f.newline(depth)
	f.write("ON CONFLICT")

	if oc.Infer != nil && len(oc.Infer.IndexElems) > 0 {
		f.write(" (")
		for i, elem := range oc.Infer.IndexElems {
			if i > 0 {
				f.write(", ")
			}
			if ie, ok := elem.Node.(*pg_query.Node_IndexElem); ok {
				f.write(quoteIdent(ie.IndexElem.Name))
			}
		}
		f.write(")")
	}

	switch oc.Action {
	case pg_query.OnConflictAction_ONCONFLICT_NOTHING:
		f.write(" DO NOTHING")
	case pg_query.OnConflictAction_ONCONFLICT_UPDATE:
		f.write(" DO UPDATE SET")
		f.newline(depth + 1)
		for i, target := range oc.TargetList {
			if i > 0 {
				f.write(",")
				f.newline(depth + 1)
			}
			if rt, ok := target.Node.(*pg_query.Node_ResTarget); ok {
				f.write(quoteIdent(rt.ResTarget.Name))
				f.write(" = ")
				f.formatNode(rt.ResTarget.Val, depth+1)
			}
		}

		if oc.WhereClause != nil {
			f.newline(depth)
			f.write("WHERE")
			f.newline(depth + 1)
			f.formatNode(oc.WhereClause, depth+1)
		}
	}
}

func (f *fmter) formatUpdate(stmt *pg_query.UpdateStmt, depth int) {
	// WITH clause
	if stmt.WithClause != nil {
		f.formatWithClause(stmt.WithClause, depth)
		f.newline(depth)
	}

	f.write("UPDATE ")
	f.formatRangeVar(stmt.Relation)

	// SET
	f.newline(depth)
	f.write("SET")
	f.newline(depth + 1)
	for i, target := range stmt.TargetList {
		if i > 0 {
			f.write(",")
			f.newline(depth + 1)
		}
		if rt, ok := target.Node.(*pg_query.Node_ResTarget); ok {
			f.write(quoteIdent(rt.ResTarget.Name))
			f.write(" = ")
			f.formatNode(rt.ResTarget.Val, depth+1)
		}
	}

	// FROM
	if len(stmt.FromClause) > 0 {
		f.newline(depth)
		f.write("FROM")
		f.newline(depth + 1)
		for i, from := range stmt.FromClause {
			if i > 0 {
				f.write(",")
				f.newline(depth + 1)
			}
			f.formatNode(from, depth+1)
		}
	}

	// WHERE
	if stmt.WhereClause != nil {
		f.newline(depth)
		f.write("WHERE")
		f.newline(depth + 1)
		f.formatNode(stmt.WhereClause, depth+1)
	}

	// RETURNING
	if len(stmt.ReturningList) > 0 {
		f.newline(depth)
		f.write("RETURNING")
		f.newline(depth + 1)
		f.formatCommaSeparated(stmt.ReturningList, depth+1)
	}
}

func (f *fmter) formatDelete(stmt *pg_query.DeleteStmt, depth int) {
	// WITH clause
	if stmt.WithClause != nil {
		f.formatWithClause(stmt.WithClause, depth)
		f.newline(depth)
	}

	f.write("DELETE FROM ")
	f.formatRangeVar(stmt.Relation)

	// USING
	if len(stmt.UsingClause) > 0 {
		f.newline(depth)
		f.write("USING")
		f.newline(depth + 1)
		for i, u := range stmt.UsingClause {
			if i > 0 {
				f.write(",")
				f.newline(depth + 1)
			}
			f.formatNode(u, depth+1)
		}
	}

	// WHERE
	if stmt.WhereClause != nil {
		f.newline(depth)
		f.write("WHERE")
		f.newline(depth + 1)
		f.formatNode(stmt.WhereClause, depth+1)
	}

	// RETURNING
	if len(stmt.ReturningList) > 0 {
		f.newline(depth)
		f.write("RETURNING")
		f.newline(depth + 1)
		f.formatCommaSeparated(stmt.ReturningList, depth+1)
	}
}

func (f *fmter) formatCreateTable(stmt *pg_query.CreateStmt, depth int) {
	f.write("CREATE")

	if stmt.Relation.Relpersistence == "t" {
		f.write(" TEMPORARY")
	} else if stmt.Relation.Relpersistence == "u" {
		f.write(" UNLOGGED")
	}

	f.write(" TABLE")

	if stmt.IfNotExists {
		f.write(" IF NOT EXISTS")
	}

	f.write(" ")
	f.formatRangeVar(stmt.Relation)

	if len(stmt.TableElts) > 0 {
		f.write(" (")
		for i, elt := range stmt.TableElts {
			if i > 0 {
				f.write(",")
			}
			f.newline(depth + 1)
			f.formatTableElement(elt, depth+1)
		}
		f.newline(depth)
		f.write(")")
	}

	// PARTITION OF ... FOR VALUES or INHERITS
	if len(stmt.InhRelations) > 0 && stmt.Partbound != nil {
		f.write(" PARTITION OF ")
		f.formatNode(stmt.InhRelations[0], depth)
		f.formatPartitionBound(stmt.Partbound, depth)
	} else if len(stmt.InhRelations) > 0 {
		f.write(" INHERITS (")
		for i, rel := range stmt.InhRelations {
			if i > 0 {
				f.write(", ")
			}
			f.formatNode(rel, depth)
		}
		f.write(")")
	}

	// PARTITION BY
	if stmt.Partspec != nil {
		f.formatPartitionSpec(stmt.Partspec, depth)
	}
}

func (f *fmter) formatPartitionSpec(spec *pg_query.PartitionSpec, depth int) {
	f.write(" PARTITION BY ")

	switch spec.Strategy {
	case pg_query.PartitionStrategy_PARTITION_STRATEGY_LIST:
		f.write("LIST")
	case pg_query.PartitionStrategy_PARTITION_STRATEGY_RANGE:
		f.write("RANGE")
	case pg_query.PartitionStrategy_PARTITION_STRATEGY_HASH:
		f.write("HASH")
	}

	f.write(" (")
	for i, param := range spec.PartParams {
		if i > 0 {
			f.write(", ")
		}
		if pe, ok := param.Node.(*pg_query.Node_PartitionElem); ok {
			if pe.PartitionElem.Name != "" {
				f.write(quoteIdent(pe.PartitionElem.Name))
			} else if pe.PartitionElem.Expr != nil {
				f.formatNode(pe.PartitionElem.Expr, depth)
			}
		}
	}
	f.write(")")
}

func (f *fmter) formatPartitionBound(pb *pg_query.PartitionBoundSpec, depth int) {
	if pb.IsDefault {
		f.write(" DEFAULT")
		return
	}

	f.write(" FOR VALUES")

	switch pb.Strategy {
	case "r": // RANGE
		f.write(" FROM (")
		f.formatCommaSeparatedInline(pb.Lowerdatums, depth)
		f.write(") TO (")
		f.formatCommaSeparatedInline(pb.Upperdatums, depth)
		f.write(")")
	case "l": // LIST
		f.write(" IN (")
		f.formatCommaSeparatedInline(pb.Listdatums, depth)
		f.write(")")
	case "h": // HASH
		f.writef(" WITH (MODULUS %d, REMAINDER %d)", pb.Modulus, pb.Remainder)
	}
}

func (f *fmter) formatTableElement(node *pg_query.Node, depth int) {
	switch n := node.Node.(type) {
	case *pg_query.Node_ColumnDef:
		f.formatColumnDef(n.ColumnDef, depth)
	case *pg_query.Node_Constraint:
		f.formatConstraint(n.Constraint, depth)
	default:
		f.formatNode(node, depth)
	}
}

func (f *fmter) formatColumnDef(cd *pg_query.ColumnDef, depth int) {
	f.write(quoteIdent(cd.Colname))
	f.write(" ")
	f.formatTypeName(cd.TypeName, depth)

	for _, constraint := range cd.Constraints {
		if c, ok := constraint.Node.(*pg_query.Node_Constraint); ok {
			f.write(" ")
			f.formatInlineConstraint(c.Constraint, depth)
		}
	}
}

func (f *fmter) formatInlineConstraint(c *pg_query.Constraint, depth int) {
	if c.Conname != "" {
		f.write("CONSTRAINT ")
		f.write(quoteIdent(c.Conname))
		f.write(" ")
	}

	switch c.Contype {
	case pg_query.ConstrType_CONSTR_NULL:
		f.write("NULL")
	case pg_query.ConstrType_CONSTR_NOTNULL:
		f.write("NOT NULL")
	case pg_query.ConstrType_CONSTR_DEFAULT:
		f.write("DEFAULT ")
		f.formatNode(c.RawExpr, depth)
	case pg_query.ConstrType_CONSTR_PRIMARY:
		f.write("PRIMARY KEY")
	case pg_query.ConstrType_CONSTR_UNIQUE:
		f.write("UNIQUE")
	case pg_query.ConstrType_CONSTR_CHECK:
		f.write("CHECK (")
		f.formatNode(c.RawExpr, depth)
		f.write(")")
	case pg_query.ConstrType_CONSTR_FOREIGN:
		f.write("REFERENCES ")
		f.formatRangeVar(c.Pktable)
		if len(c.PkAttrs) > 0 {
			f.write(" (")
			for i, attr := range c.PkAttrs {
				if i > 0 {
					f.write(", ")
				}
				f.formatNode(attr, depth)
			}
			f.write(")")
		}
	}
}

func (f *fmter) formatConstraint(c *pg_query.Constraint, depth int) {
	if c.Conname != "" {
		f.write("CONSTRAINT ")
		f.write(quoteIdent(c.Conname))
		f.write(" ")
	}

	switch c.Contype {
	case pg_query.ConstrType_CONSTR_PRIMARY:
		f.write("PRIMARY KEY (")
		for i, key := range c.Keys {
			if i > 0 {
				f.write(", ")
			}
			f.formatNode(key, depth)
		}
		f.write(")")
	case pg_query.ConstrType_CONSTR_UNIQUE:
		f.write("UNIQUE (")
		for i, key := range c.Keys {
			if i > 0 {
				f.write(", ")
			}
			f.formatNode(key, depth)
		}
		f.write(")")
	case pg_query.ConstrType_CONSTR_CHECK:
		f.write("CHECK (")
		f.formatNode(c.RawExpr, depth)
		f.write(")")
	case pg_query.ConstrType_CONSTR_FOREIGN:
		f.write("FOREIGN KEY (")
		for i, key := range c.FkAttrs {
			if i > 0 {
				f.write(", ")
			}
			f.formatNode(key, depth)
		}
		f.write(") REFERENCES ")
		f.formatRangeVar(c.Pktable)
		if len(c.PkAttrs) > 0 {
			f.write(" (")
			for i, attr := range c.PkAttrs {
				if i > 0 {
					f.write(", ")
				}
				f.formatNode(attr, depth)
			}
			f.write(")")
		}
	}
}

func (f *fmter) formatCreateIndex(stmt *pg_query.IndexStmt, depth int) {
	f.write("CREATE")

	if stmt.Unique {
		f.write(" UNIQUE")
	}

	f.write(" INDEX")

	if stmt.Concurrent {
		f.write(" CONCURRENTLY")
	}

	if stmt.IfNotExists {
		f.write(" IF NOT EXISTS")
	}

	if stmt.Idxname != "" {
		f.write(" ")
		f.write(quoteIdent(stmt.Idxname))
	}

	f.write(" ON ")
	f.formatRangeVar(stmt.Relation)

	if stmt.AccessMethod != "" && stmt.AccessMethod != "btree" {
		f.write(" USING ")
		f.write(stmt.AccessMethod)
	}

	f.write(" (")
	for i, param := range stmt.IndexParams {
		if i > 0 {
			f.write(", ")
		}
		if ie, ok := param.Node.(*pg_query.Node_IndexElem); ok {
			if ie.IndexElem.Name != "" {
				f.write(quoteIdent(ie.IndexElem.Name))
			} else if ie.IndexElem.Expr != nil {
				f.formatNode(ie.IndexElem.Expr, depth)
			}

			if ie.IndexElem.Ordering == pg_query.SortByDir_SORTBY_DESC {
				f.write(" DESC")
			}

			if ie.IndexElem.NullsOrdering == pg_query.SortByNulls_SORTBY_NULLS_FIRST {
				f.write(" NULLS FIRST")
			} else if ie.IndexElem.NullsOrdering == pg_query.SortByNulls_SORTBY_NULLS_LAST {
				f.write(" NULLS LAST")
			}
		}
	}
	f.write(")")

	if stmt.WhereClause != nil {
		f.newline(depth)
		f.write("WHERE")
		f.newline(depth + 1)
		f.formatNode(stmt.WhereClause, depth+1)
	}
}

func (f *fmter) formatDrop(stmt *pg_query.DropStmt, depth int) {
	f.write("DROP")

	switch stmt.RemoveType {
	case pg_query.ObjectType_OBJECT_TABLE:
		f.write(" TABLE")
	case pg_query.ObjectType_OBJECT_INDEX:
		f.write(" INDEX")
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		f.write(" SEQUENCE")
	case pg_query.ObjectType_OBJECT_VIEW:
		f.write(" VIEW")
	case pg_query.ObjectType_OBJECT_MATVIEW:
		f.write(" MATERIALIZED VIEW")
	case pg_query.ObjectType_OBJECT_SCHEMA:
		f.write(" SCHEMA")
	case pg_query.ObjectType_OBJECT_TYPE:
		f.write(" TYPE")
	case pg_query.ObjectType_OBJECT_FUNCTION:
		f.write(" FUNCTION")
	default:
		f.write(" " + strings.TrimPrefix(stmt.RemoveType.String(), "OBJECT_"))
	}

	if stmt.MissingOk {
		f.write(" IF EXISTS")
	}

	for i, obj := range stmt.Objects {
		if i > 0 {
			f.write(",")
		}
		f.write(" ")
		f.formatNode(obj, depth)
	}

	if stmt.Behavior == pg_query.DropBehavior_DROP_CASCADE {
		f.write(" CASCADE")
	}
}

func (f *fmter) formatAlterTable(stmt *pg_query.AlterTableStmt, depth int) {
	f.write("ALTER TABLE")

	if stmt.MissingOk {
		f.write(" IF EXISTS")
	}

	f.write(" ")
	f.formatRangeVar(stmt.Relation)

	for i, cmd := range stmt.Cmds {
		if i > 0 {
			f.write(",")
		}
		f.newline(depth + 1)
		if atCmd, ok := cmd.Node.(*pg_query.Node_AlterTableCmd); ok {
			f.formatAlterTableCmd(atCmd.AlterTableCmd, depth+1)
		}
	}
}

func (f *fmter) formatAlterTableCmd(cmd *pg_query.AlterTableCmd, depth int) {
	switch cmd.Subtype {
	case pg_query.AlterTableType_AT_AddColumn:
		f.write("ADD COLUMN ")
		if cd, ok := cmd.Def.Node.(*pg_query.Node_ColumnDef); ok {
			f.formatColumnDef(cd.ColumnDef, depth)
		}
	case pg_query.AlterTableType_AT_DropColumn:
		f.write("DROP COLUMN ")
		if cmd.MissingOk {
			f.write("IF EXISTS ")
		}
		f.write(quoteIdent(cmd.Name))
	case pg_query.AlterTableType_AT_AlterColumnType:
		f.write("ALTER COLUMN ")
		f.write(quoteIdent(cmd.Name))
		f.write(" TYPE ")
		if cd, ok := cmd.Def.Node.(*pg_query.Node_ColumnDef); ok {
			f.formatTypeName(cd.ColumnDef.TypeName, depth)
		}
	case pg_query.AlterTableType_AT_SetNotNull:
		f.write("ALTER COLUMN ")
		f.write(quoteIdent(cmd.Name))
		f.write(" SET NOT NULL")
	case pg_query.AlterTableType_AT_DropNotNull:
		f.write("ALTER COLUMN ")
		f.write(quoteIdent(cmd.Name))
		f.write(" DROP NOT NULL")
	case pg_query.AlterTableType_AT_ColumnDefault:
		f.write("ALTER COLUMN ")
		f.write(quoteIdent(cmd.Name))
		if cmd.Def != nil {
			f.write(" SET DEFAULT ")
			f.formatNode(cmd.Def, depth)
		} else {
			f.write(" DROP DEFAULT")
		}
	case pg_query.AlterTableType_AT_AddConstraint:
		f.write("ADD ")
		if c, ok := cmd.Def.Node.(*pg_query.Node_Constraint); ok {
			f.formatConstraint(c.Constraint, depth)
		}
	case pg_query.AlterTableType_AT_DropConstraint:
		f.write("DROP CONSTRAINT ")
		if cmd.MissingOk {
			f.write("IF EXISTS ")
		}
		f.write(quoteIdent(cmd.Name))
	default:
		f.write("/* unsupported ALTER TABLE command */")
	}

	if cmd.Behavior == pg_query.DropBehavior_DROP_CASCADE {
		f.write(" CASCADE")
	}
}

func (f *fmter) formatTruncate(stmt *pg_query.TruncateStmt, depth int) {
	f.write("TRUNCATE TABLE")

	for i, rel := range stmt.Relations {
		if i > 0 {
			f.write(",")
		}
		f.write(" ")
		f.formatNode(rel, depth)
	}

	if stmt.Behavior == pg_query.DropBehavior_DROP_CASCADE {
		f.write(" CASCADE")
	} else if stmt.Behavior == pg_query.DropBehavior_DROP_RESTRICT {
		f.write(" RESTRICT")
	}
}

func (f *fmter) formatComment(stmt *pg_query.CommentStmt, depth int) {
	f.write("COMMENT ON ")

	switch stmt.Objtype {
	case pg_query.ObjectType_OBJECT_TABLE:
		f.write("TABLE ")
	case pg_query.ObjectType_OBJECT_COLUMN:
		f.write("COLUMN ")
	case pg_query.ObjectType_OBJECT_INDEX:
		f.write("INDEX ")
	case pg_query.ObjectType_OBJECT_SCHEMA:
		f.write("SCHEMA ")
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		f.write("SEQUENCE ")
	case pg_query.ObjectType_OBJECT_VIEW:
		f.write("VIEW ")
	case pg_query.ObjectType_OBJECT_FUNCTION:
		f.write("FUNCTION ")
	case pg_query.ObjectType_OBJECT_TYPE:
		f.write("TYPE ")
	case pg_query.ObjectType_OBJECT_DATABASE:
		f.write("DATABASE ")
	case pg_query.ObjectType_OBJECT_ROLE:
		f.write("ROLE ")
	case pg_query.ObjectType_OBJECT_EXTENSION:
		f.write("EXTENSION ")
	default:
		f.write(strings.TrimPrefix(stmt.Objtype.String(), "OBJECT_") + " ")
	}

	f.formatCommentObject(stmt.Object, depth)

	if stmt.Comment != "" {
		f.write(" IS '" + strings.ReplaceAll(stmt.Comment, "'", "''") + "'")
	} else {
		f.write(" IS NULL")
	}
}

func (f *fmter) formatCommentObject(node *pg_query.Node, depth int) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_List:
		for i, item := range n.List.Items {
			if i > 0 {
				f.write(".")
			}
			if s, ok := item.Node.(*pg_query.Node_String_); ok {
				f.write(quoteIdent(s.String_.Sval))
			} else {
				f.formatNode(item, depth)
			}
		}
	default:
		f.formatNode(node, depth)
	}
}

func (f *fmter) formatCreateFunction(stmt *pg_query.CreateFunctionStmt, depth int) {
	f.write("CREATE")

	if stmt.Replace {
		f.write(" OR REPLACE")
	}

	if stmt.IsProcedure {
		f.write(" PROCEDURE ")
	} else {
		f.write(" FUNCTION ")
	}

	// Function name
	for i, n := range stmt.Funcname {
		if i > 0 {
			f.write(".")
		}
		if s, ok := n.Node.(*pg_query.Node_String_); ok {
			f.write(quoteIdent(s.String_.Sval))
		}
	}

	// Parameters
	f.write("(")
	paramIdx := 0
	for _, p := range stmt.Parameters {
		if fp, ok := p.Node.(*pg_query.Node_FunctionParameter); ok {
			// Skip TABLE output parameters in the parameter list
			if fp.FunctionParameter.Mode == pg_query.FunctionParameterMode_FUNC_PARAM_TABLE {
				continue
			}
			if paramIdx > 0 {
				f.write(", ")
			}
			f.formatFunctionParameter(fp.FunctionParameter, depth)
			paramIdx++
		}
	}
	f.write(")")

	// RETURNS
	if !stmt.IsProcedure {
		// Check for RETURNS TABLE
		var tableCols []*pg_query.FunctionParameter
		for _, p := range stmt.Parameters {
			if fp, ok := p.Node.(*pg_query.Node_FunctionParameter); ok {
				if fp.FunctionParameter.Mode == pg_query.FunctionParameterMode_FUNC_PARAM_TABLE {
					tableCols = append(tableCols, fp.FunctionParameter)
				}
			}
		}

		if len(tableCols) > 0 {
			f.newline(depth)
			f.write("RETURNS TABLE (")
			for i, col := range tableCols {
				if i > 0 {
					f.write(", ")
				}
				f.write(quoteIdent(col.Name))
				f.write(" ")
				f.formatTypeName(col.ArgType, depth)
			}
			f.write(")")
		} else if stmt.ReturnType != nil {
			f.newline(depth)
			f.write("RETURNS ")
			f.formatTypeName(stmt.ReturnType, depth)
		}
	}

	// Options (LANGUAGE, AS, etc.)
	var language string
	var body string

	for _, opt := range stmt.Options {
		if de, ok := opt.Node.(*pg_query.Node_DefElem); ok {
			switch de.DefElem.Defname {
			case "language":
				if s, ok := de.DefElem.Arg.Node.(*pg_query.Node_String_); ok {
					language = s.String_.Sval
				}
			case "as":
				if list, ok := de.DefElem.Arg.Node.(*pg_query.Node_List); ok {
					if len(list.List.Items) > 0 {
						if s, ok := list.List.Items[0].Node.(*pg_query.Node_String_); ok {
							body = s.String_.Sval
						}
					}
				}
			}
		}
	}

	if language != "" {
		f.newline(depth)
		f.write("LANGUAGE ")
		f.write(language)
	}

	if body != "" {
		f.newline(depth)
		f.write("AS $$")
		f.write(body)
		f.write("$$")
	}
}

func (f *fmter) formatFunctionParameter(fp *pg_query.FunctionParameter, depth int) {
	switch fp.Mode {
	case pg_query.FunctionParameterMode_FUNC_PARAM_IN:
		f.write("IN ")
	case pg_query.FunctionParameterMode_FUNC_PARAM_OUT:
		f.write("OUT ")
	case pg_query.FunctionParameterMode_FUNC_PARAM_INOUT:
		f.write("INOUT ")
	case pg_query.FunctionParameterMode_FUNC_PARAM_VARIADIC:
		f.write("VARIADIC ")
	}

	if fp.Name != "" {
		f.write(quoteIdent(fp.Name))
		f.write(" ")
	}

	f.formatTypeName(fp.ArgType, depth)

	if fp.Defexpr != nil {
		f.write(" DEFAULT ")
		f.formatNode(fp.Defexpr, depth)
	}
}

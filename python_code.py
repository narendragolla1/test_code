import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import ParseError

def extract_and_validate_tables(
    sql_query: str, 
    # allowed_tables: set, 
    dialect: str = "postgres"
) -> set:
    try:
        parsed = sqlglot.parse_one(sql_query, read=dialect, error_level="EXCEPTION")
    except ParseError as e:
        raise ValueError(f"Invalid SQL syntax for {dialect}: {e}")
    disallowed_expressions = (
        exp.Create,
        exp.Alter,
        exp.Update,
        exp.Insert,
        exp.Delete,
        exp.Drop,
        exp.Merge,
        exp.Grant,
        exp.Set,
    )
    for node in parsed.walk():
        if isinstance(node, disallowed_expressions):
            raise ValueError(
                f"The query must be a SELECT (or UNION of SELECTs). "
                f"Found disallowed statement: {node.key.upper()}."
            )

    # 2. Extract all table names, excluding CTE names
    cte_names = set(cte.alias_or_name for cte in parsed.find_all(exp.CTE))
    table_names = set()

    def visit(node: exp.Expression):
        if isinstance(node, exp.Table):
            if node.name not in cte_names:
                table_names.add(node.name)
        for arg in node.args.values():
            if isinstance(arg, exp.Expression):
                visit(arg)
            elif isinstance(arg, list):
                for item in arg:
                    if isinstance(item, exp.Expression):
                        visit(item)

    visit(parsed)

    # 3. Validate extracted tables against allowed tables
    # disallowed_tables = table_names - allowed_tables
    # if disallowed_tables:
    #     raise ValueError(f"Disallowed tables found: {disallowed_tables}")

    return table_names

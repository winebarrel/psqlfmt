# psqlfmt

`psqlfmt` is a PostgreSQL SQL formatter. It parses SQL with `pg_query_go` and rewrites `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and various DDL statements into a consistent multi-line format.

It can be used both as a CLI and as a Go package.

## Features

- Formats SQL from the PostgreSQL parse tree
- Handles multiple statements in a single input
- Normalizes keywords, indentation, and line breaks
- Exposes `psqlfmt.Format` for use from Go code

## Installation

### CLI

```bash
go install github.com/winebarrel/psqlfmt/cmd/psqlfmt@latest
```

### Library

```bash
go get github.com/winebarrel/psqlfmt
```

## Usage

### CLI

`psqlfmt` reads SQL from standard input and writes the formatted result to standard output.

```bash
echo "select id,name from users where id=1" | psqlfmt
```

Output:

```sql
SELECT
  id,
  name
FROM
  users
WHERE
  id = 1;
```

You can also format a file by piping it into the command.

```bash
cat schema.sql | psqlfmt
```

### Go

```go
package main

import (
	"fmt"
	"log"

	"github.com/winebarrel/psqlfmt"
)

func main() {
	sql := "select id,name from users where active=true"

	formatted, err := psqlfmt.Format(sql)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(formatted)
}
```

## Example

Input:

```sql
with cte as (select id from users where active=true) select * from cte;
insert into users (name,email) values ('a','a@example.com') on conflict (email) do update set name = excluded.name returning *;
```

Output:

```sql
WITH
  cte AS (
    SELECT
      id
    FROM
      users
    WHERE
      active = TRUE
  )
SELECT
  *
FROM
  cte;

INSERT INTO users (name, email)
VALUES
  ('a', 'a@example.com')
ON CONFLICT (email) DO UPDATE SET
  name = excluded.name
RETURNING
  *;
```

## Supported Syntax

- `SELECT`
- `JOIN`
- `WITH` / CTE
- `UNION` / `INTERSECT` / `EXCEPT`
- `CASE`
- `GROUP BY` / `HAVING` / `ORDER BY`
- Subqueries
- Window functions
- `INSERT`
  - `VALUES`
  - `INSERT ... SELECT`
  - `ON CONFLICT`
  - `RETURNING`
- `UPDATE`
  - `FROM`
  - `RETURNING`
- `DELETE`
  - `USING`
  - `RETURNING`
- DDL
  - `CREATE TABLE`
  - `CREATE INDEX`
  - `DROP`
  - `TRUNCATE`
  - `ALTER TABLE`
  - `COMMENT`
  - `CREATE FUNCTION`
  - `CREATE PROCEDURE`
  - `PARTITION BY` / `PARTITION OF`

## Formatting Rules

- Uses two-space indentation
- Uppercases SQL keywords
- Separates multiple statements with a blank line
- Always appends a trailing semicolon and newline
- Falls back to deparsed output for unsupported nodes when possible

## Development

```bash
make test
make vet
make build
```

Test cases are stored as YAML files under [`testdata/`](./testdata).

## License

[MIT](./LICENSE)

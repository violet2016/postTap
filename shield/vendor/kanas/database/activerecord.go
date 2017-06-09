package database

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

// ActiveRecord represents a database connection and a sql string.
type ActiveRecord struct {
	DB     *sql.DB
	Tokens []string
	Args   []interface{}
}

const comma = ","
const holder = "?"

func NewActiveRecord() *ActiveRecord {
	return &ActiveRecord{}
}

// Connect connect to dbtype database by provided url.
func (ar *ActiveRecord) Connect(dbtype string, url string) (err error) {
	if ar.DB != nil {
		ar.Close()
	}

	if ar.DB, err = sql.Open(dbtype, url); err != nil {
		return err
	} else {
		return nil
	}
}

// Close close the connection.
func (ar *ActiveRecord) Close() {
	if ar.DB != nil {
		ar.DB.Close()
	}
	ar.DB = nil
}

// Exec will execute the sql string represented by ActiveRecord.
func (ar *ActiveRecord) Exec() (sql.Result, error) {
	return ar.DB.Exec(ar.ExecString(), ar.Args...)
}

// ExecSQL execute the sql string in argument.
func (ar *ActiveRecord) ExecSQL(sql string, args ...interface{}) (sql.Result, error) {
	return ar.DB.Exec(sql, args...)
}

// GetCount get the row count of the result of ActiveRecord sql.
func (ar *ActiveRecord) GetCount(args ...interface{}) (count int, err error) {
	if err = ar.DB.QueryRow(ar.ExecString(), ar.Args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// GetRows return the full execution result of ActiveRecord sql, all of the values are in interface{} type.
func (ar *ActiveRecord) GetRows() (result []map[string]interface{}, err error) {
	rows, err := ar.DB.Query(ar.ExecString(), ar.Args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	dest := make([]interface{}, len(columns))
	fields := make([]interface{}, len(columns))
	for i := range fields {
		dest[i] = &fields[i]
	}

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		r := make(map[string]interface{})
		for i, v := range fields {
			if str, ok := v.(string); ok {
				r[columns[i]] = str
			} else {
				switch v.(type) {
				case time.Time:
					t := v.(time.Time)
					r[columns[i]] = t.String()[:19]
				case []uint8:
					r[columns[i]] = string(v.([]uint8))
				default:
					r[columns[i]] = v
				}
			}
		}

		result = append(result, r)
	}

	if err := rows.Err(); err != nil {
		if err == driver.ErrBadConn {
			return result, nil
		}
		return nil, err

	}
	return result, nil
}

// get rows as map[string]interface{}
func (ar *ActiveRecord) GetRowsI(query string, args ...interface{}) (result []map[string]interface{}, err error) {

	rows, err := ar.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	dest := make([]interface{}, len(columns))
	fields := make([]interface{}, len(columns))
	for i := range fields {
		dest[i] = &fields[i]
	}

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		r := make(map[string]interface{})
		for i, value := range fields {
			switch v := value.(type) {
			case sql.NullBool:
				if v.Valid {
					r[columns[i]] = v.Bool
				} else {
					r[columns[i]] = nil
				}
			case sql.NullFloat64:
				if v.Valid {
					r[columns[i]] = v.Float64
				} else {
					r[columns[i]] = nil
				}
			case sql.NullInt64:
				if v.Valid {
					r[columns[i]] = v.Int64
				} else {
					r[columns[i]] = nil
				}
			case sql.NullString:
				if v.Valid {
					r[columns[i]] = v.String
				} else {
					r[columns[i]] = nil
				}
			case pq.NullTime:
				if v.Valid {
					r[columns[i]] = v.Time
				} else {
					r[columns[i]] = nil
				}
			case sql.RawBytes:
				r[columns[i]] = string(v)
			case nil:
				r[columns[i]] = nil
			case []uint8:
				r[columns[i]] = string(v)
			case time.Time:
				r[columns[i]] = v
			case int64:
				r[columns[i]] = v
			default:
				r[columns[i]] = v
			}
		}

		result = append(result, r)
	}

	if err := rows.Err(); err != nil {
		if err == driver.ErrBadConn {
			return result, nil
		}
		return nil, err
	}
	return result, nil
}

// GetRow return the first row of execution result of ActiveRecord sql, all of the values are in interface{} type.
func (ar *ActiveRecord) GetRow() (map[string]interface{}, error) {
	rows, err := ar.GetRows()
	if err != nil || len(rows) == 0 {
		return nil, err
	}

	return rows[0], nil
}

func (ar *ActiveRecord) Select(fields ...string) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "SELECT", strings.Join(fields, comma))
	return ar
}

func (ar *ActiveRecord) SelectDistinct(fields ...string) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "SELECT DISTINCT", strings.Join(fields, comma))
	return ar
}

func (ar *ActiveRecord) From(tables ...string) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "FROM", strings.Join(tables, comma))
	return ar
}

func (ar *ActiveRecord) Join(table string) *ActiveRecord {
	if len(table) > 0 {
		ar.Tokens = append(ar.Tokens, "JOIN", table)
	}
	return ar
}

func (ar *ActiveRecord) InnerJoin(table string) *ActiveRecord {
	if len(table) > 0 {
		ar.Tokens = append(ar.Tokens, "INNER JOIN", table)
	}
	return ar
}

func (ar *ActiveRecord) LeftJoin(table string) *ActiveRecord {
	if len(table) > 0 {
		ar.Tokens = append(ar.Tokens, "LEFT JOIN", table)
	}
	return ar
}

func (ar *ActiveRecord) RightJoin(table string) *ActiveRecord {
	if len(table) > 0 {
		ar.Tokens = append(ar.Tokens, "RIGHT JOIN", table)
	}
	return ar
}

func (ar *ActiveRecord) LeftOuterJoin(table string) *ActiveRecord {
	if len(table) > 0 {
		ar.Tokens = append(ar.Tokens, "LEFT OUTER JOIN", table)
	}
	return ar
}

func (ar *ActiveRecord) RightOuterJoin(table string) *ActiveRecord {
	if len(table) > 0 {
		ar.Tokens = append(ar.Tokens, "RIGHT OUTER JOIN", table)
	}
	return ar
}

func (ar *ActiveRecord) appendArgs(args ...interface{}) {
	ar.Args = append(ar.Args, args...)
}

func (ar *ActiveRecord) On(cond string, args ...interface{}) *ActiveRecord {
	if len(cond) > 0 {
		ar.Tokens = append(ar.Tokens, "ON", cond)
		ar.appendArgs(args...)
	}
	return ar
}

func (ar *ActiveRecord) Where(cond string, args ...interface{}) *ActiveRecord {
	if len(cond) > 0 {
		ar.Tokens = append(ar.Tokens, "WHERE", cond)
		ar.appendArgs(args...)
	}
	return ar
}

func (ar *ActiveRecord) And(cond string, args ...interface{}) *ActiveRecord {
	if len(cond) > 0 {
		ar.Tokens = append(ar.Tokens, "AND", cond)
		ar.appendArgs(args...)
	}
	return ar
}
func (ar *ActiveRecord) WhereAnd(conds []string, args ...interface{}) *ActiveRecord {
	firstNotEmptyFound := false
	ar.appendArgs(args...)
	for _, cond := range conds {
		if len(cond) > 0 {
			if !firstNotEmptyFound {
				ar.Tokens = append(ar.Tokens, "WHERE", cond)
				firstNotEmptyFound = true
				continue
			} else {
				ar.Tokens = append(ar.Tokens, "AND", cond)
			}
		}
	}
	return ar
}
func (ar *ActiveRecord) Or(cond string, args ...interface{}) *ActiveRecord {
	if len(cond) > 0 {
		ar.Tokens = append(ar.Tokens, "OR", cond)
		ar.appendArgs(args...)
	}
	return ar
}

func (ar *ActiveRecord) In(vals []string, args ...interface{}) *ActiveRecord {
	cond := strings.Join(vals, comma)
	ar.Tokens = append(ar.Tokens, "IN", "(", cond, ")")
	ar.appendArgs(args...)
	return ar
}

func (ar *ActiveRecord) InAddQuotes(vals []string, args ...interface{}) *ActiveRecord {
	qvals := []string{}
	for _, val := range vals {
		qvals = append(qvals, fmt.Sprintf("'%s'", val))
	}
	return ar.In(qvals)
}

func (ar *ActiveRecord) OrderBy(fields ...string) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "ORDER BY", strings.Join(fields, comma))
	return ar
}

func (ar *ActiveRecord) Asc() *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "ASC")
	return ar
}

func (ar *ActiveRecord) Desc() *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "DESC")
	return ar
}

func (ar *ActiveRecord) Limit(limit int) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "LIMIT", strconv.Itoa(limit))
	return ar
}

func (ar *ActiveRecord) Offset(offset int) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "OFFSET", strconv.Itoa(offset))
	return ar
}

func (ar *ActiveRecord) GroupBy(fields ...string) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "GROUP BY", strings.Join(fields, comma))
	return ar
}

func (ar *ActiveRecord) Having(cond string, args ...interface{}) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "HAVING", cond)
	ar.appendArgs(args...)
	return ar
}

func (ar *ActiveRecord) Update(tables ...string) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "UPDATE", strings.Join(tables, comma))
	return ar
}

func (ar *ActiveRecord) Set(kv ...string) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "SET", strings.Join(kv, comma))
	return ar
}

func (ar *ActiveRecord) Delete(tables ...string) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "DELETE")
	if len(tables) != 0 {
		ar.Tokens = append(ar.Tokens, strings.Join(tables, comma))
	}
	return ar
}

func (ar *ActiveRecord) InsertInto(table string, fields ...string) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, "INSERT INTO", table)
	if len(fields) != 0 {
		fieldsStr := strings.Join(fields, comma)
		ar.Tokens = append(ar.Tokens, "(", fieldsStr, ")")
	}
	return ar
}

func (ar *ActiveRecord) Values(vals []string, args ...interface{}) *ActiveRecord {
	valsStr := strings.Join(vals, comma)
	ar.Tokens = append(ar.Tokens, "VALUES", "(", valsStr, ")")
	ar.appendArgs(args...)
	return ar
}

func (ar *ActiveRecord) AddSQL(sql string) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, sql)
	return ar
}

func (ar *ActiveRecord) Subquery(sub string, alias string) string {
	return fmt.Sprintf("(%s) AS %s", sub, alias)
}

func (ar *ActiveRecord) SubAR(sub *ActiveRecord, alias string) string {
	ar.Args = append(ar.Args, sub.Args...)
	return fmt.Sprintf("(%s) AS %s", sub.String(), alias)
}

// String return the sql string represented by Tokens.
func (ar *ActiveRecord) ExecString() string {
	str := strings.Join(ar.Tokens, " ")
	if len(ar.Args) > 0 {
		for i, _ := range ar.Args {
			str = strings.Replace(str, holder, fmt.Sprintf("$%d", i+1), 1)
		}
	}
	return str
}

func (ar *ActiveRecord) String() string {
	return strings.Join(ar.Tokens, " ")
}
func (ar *ActiveRecord) ArgsString() string {
	str := fmt.Sprintf("total length %d\n", len(ar.Args))
	for i, arg := range ar.Args {
		str = str + fmt.Sprintf("$%d:%v ", i, arg)
	}
	return str
}

func (ar *ActiveRecord) PrintableString() string {
	stoken := strings.Join(ar.Tokens, " ")
	return fmt.Sprintf("%s\n[%v]\n", stoken, ar.Args)
}

func (ar *ActiveRecord) CleanTokens() *ActiveRecord {
	ar.Tokens = ar.Tokens[:0]
	ar.Args = ar.Args[:0]
	return ar
}

// AddToken append token after the existing Tokens.
func (ar *ActiveRecord) AddToken(token ...string) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, token...)
	return ar
}
func (ar *ActiveRecord) AddArgs(args ...interface{}) *ActiveRecord {
	ar.Args = append(ar.Args, args...)
	return ar
}

func (ar *ActiveRecord) AddParenthesis() *ActiveRecord {
	ar.Tokens = append([]string{"("}, ar.Tokens...)
	ar.Tokens = append(ar.Tokens, ")")
	return ar
}

// Append append another ActiveRecord's Token after the existing Tokens.
func (ar *ActiveRecord) Append(other *ActiveRecord) *ActiveRecord {
	ar.Tokens = append(ar.Tokens, other.Tokens...)
	ar.Args = append(ar.Args, other.Args...)
	return ar
}

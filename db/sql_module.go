package db

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/xyzj/toolbox/json"
	"gorm.io/gorm"
)

type dataType byte

const (
	tstr dataType = iota
	tint64
	tuint64
	tfloat64
	tbool
)

const (
	defaultDateTimeFormat = "2006-01-02 15:04:05"
)

var EmptyValue = Value{}

type Value struct {
	val any
}

// TryTime returns a formatted time string based on the underlying value and format.
func (v *Value) TryTime(f string) string {
	if v == nil {
		return ""
	}
	if f == "" {
		f = defaultDateTimeFormat
	}
	switch b := v.val.(type) {
	case time.Time:
		return b.Format(f)
	case int64:
		return time.Unix(b, 0).Format(f)
	case uint64:
		return time.Unix(int64(b), 0).Format(f)
	case float64:
		return time.Unix(int64(b), 0).Format(f)
	case float32:
		return time.Unix(int64(b), 0).Format(f)
	case []uint8:
		t, err := time.Parse(f, json.String(b))
		if err != nil {
			return ""
		}
		return t.Format(f)
	default:
		return ""
	}
}

// TryTimestamp returns a unix timestamp derived from the underlying value.
func (v *Value) TryTimestamp(f string) int64 {
	if v == nil {
		return 0
	}
	switch b := v.val.(type) {
	case time.Time:
		return b.Unix()
	case int64:
		return b
	case uint64:
		return int64(b)
	case float64:
		return int64(b)
	case float32:
		return int64(b)
	case []uint8:
		if f == "" {
			f = defaultDateTimeFormat
		}
		t, err := time.Parse(f, json.String(b))
		if err != nil {
			return 0
		}
		return t.Unix()
	default:
		return 0
	}
}

// String returns the string representation of the underlying value.
func (v *Value) String() string {
	if v == nil {
		return ""
	}
	switch b := v.val.(type) {
	case []uint8:
		return json.String(b)
	case float32:
		return strconv.FormatFloat(float64(b), 'f', -1, 32)
	case time.Time:
		return b.Format(defaultDateTimeFormat)
	case int64:
		return strconv.FormatInt(b, 10)
	case uint64:
		return strconv.FormatUint(b, 10)
	case float64:
		return strconv.FormatFloat(b, 'f', -1, 64)
	case bool:
		if b {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

// TryInt64 converts the underlying value to int64 if possible.
func (v *Value) TryInt64() int64 {
	if v == nil {
		return 0
	}
	switch b := v.val.(type) {
	case int64:
		return b
	case uint64:
		return int64(b)
	case time.Time:
		return b.Unix()
	case float64:
		return int64(b)
	case float32:
		return int64(b)
	case []uint8:
		i, _ := strconv.ParseInt(json.String(b), 10, 64)
		return i
	case bool:
		if b {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// TryFloat64 converts the underlying value to float64 with optional precision.
func (v *Value) TryFloat64(dec ...int) float64 {
	if v == nil {
		return 0
	}
	xdec := 2
	if len(dec) > 0 && dec[0] > 0 {
		xdec = dec[0]
	}
	var val float64
	switch b := v.val.(type) {
	case float64:
		val = b
	case float32:
		val = float64(b)
	case []uint8:
		val, _ = strconv.ParseFloat(json.String(b), 64)
	case int64:
		val = float64(b)
	case uint64:
		val = float64(b)
	case time.Time:
		val = float64(b.Unix())
	case bool:
		if b {
			val = 1
		}
	default:
	}
	s := strconv.FormatFloat(val, 'f', xdec, 64)
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// TryBool converts the underlying value to bool if possible.
func (v *Value) TryBool() bool {
	if v == nil {
		return false
	}
	switch b := v.val.(type) {
	case bool:
		return b
	case int64:
		return b != 0
	case uint64:
		return b != 0
	case float64:
		return b != 0
	case float32:
		return b != 0
	case []uint8:
		s := json.String(b)
		return s == "true" || s == "1"
	default:
		return false
	}
}

// TryUint64 converts the underlying value to uint64 if possible.
func (v *Value) TryUint64() uint64 {
	if v == nil {
		return 0
	}
	switch b := v.val.(type) {
	case uint64:
		return b
	case int64:
		return uint64(b)
	case time.Time:
		return uint64(b.Unix())
	case float64:
		return uint64(b)
	case float32:
		return uint64(b)
	case []uint8:
		i, _ := strconv.ParseUint(json.String(b), 10, 64)
		return i
	case bool:
		if b {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// TryInt32 converts the underlying value to int32.
func (v *Value) TryInt32() int32 {
	return int32(v.TryInt64())
}

// TryInt converts the underlying value to int.
func (v *Value) TryInt() int {
	return int(v.TryInt64())
}

// TryFloat32 converts the underlying value to float32 with optional precision.
func (v *Value) TryFloat32(dec ...int) float32 {
	return float32(v.TryFloat64(dec...))
}

// QueryDataRow 数据行
type QueryDataRow struct {
	Cells []Value `json:"vcells,omitempty"`
}

func (d *QueryDataRow) JSON() string {
	s, _ := json.MarshalToString(d)
	return s
}

// QueryData 数据集
type QueryData struct {
	Rows    []QueryDataRow `json:"rows,omitempty"`
	Columns []string       `json:"columns,omitempty"`
	Total   int            `json:"total,omitempty"`
}

func (d *QueryData) JSON() (string, error) {
	return json.MarshalToString(d)
	// return s
}

func NewDB(defaultdb, dbnames, dbtype string, gorm *gorm.DB, sql *sql.DB) *DB {
	d := &DB{
		ormdb:     gorm,
		sqldb:     sql,
		dbtype:    dbtype,
		defaultdb: defaultdb,
		dbnames:   strings.Split(dbnames, ","),
	}
	return d
}

type DB struct {
	ormdb     *gorm.DB
	sqldb     *sql.DB
	defaultdb string
	dbtype    string
	dbnames   []string
}

func (db *DB) ORM() *gorm.DB {
	return db.ormdb
}

func (db *DB) SQL() *sql.DB {
	return db.sqldb
}

func (db *DB) Name(idx int) string {
	if idx < 0 || idx >= len(db.dbnames) {
		return db.defaultdb
	}
	return db.dbnames[idx]
}

func (db *DB) Names() []string {
	return db.dbnames
}

func (db *DB) Type() string {
	return db.dbtype
}

func (db *DB) Query(s string, expire time.Duration, params ...any) (query *QueryData, err error) {
	if db.sqldb == nil {
		return nil, errors.New("sql.DB is not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), expire)
	defer cancel()
	rows, err := db.sqldb.QueryContext(ctx, s, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	query = &QueryData{
		Columns: columns,
	}
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		row := QueryDataRow{
			Cells: make([]Value, len(columns)),
		}
		for i, val := range values {
			switch b := val.(type) {
			case []byte:
				// database/sql may reuse []byte buffers across Scan calls; clone to keep row data stable.
				row.Cells[i] = Value{val: append([]byte(nil), b...)}
			default:
				row.Cells[i] = Value{val: val}
			}
		}
		query.Rows = append(query.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	query.Total = len(query.Rows)
	return query, nil
}

func (db *DB) Exec(s string, expire time.Duration, params ...any) (rowAffected, insertID int64, err error) {
	if db.sqldb == nil {
		return 0, 0, errors.New("sql.DB is not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), expire)
	defer cancel()
	res, err := db.sqldb.ExecContext(ctx, s, params...)
	if err != nil {
		return 0, 0, err
	}
	rowAffected, err = res.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	insertID, err = res.LastInsertId()
	if err != nil {
		return 0, 0, err
	}
	return rowAffected, insertID, nil
}

func (db *DB) ExecPrepare(s string, expire time.Duration, params ...any) error {
	if db.sqldb == nil {
		return errors.New("sql.DB is not initialized")
	}
	l := len(params)
	paramNum := strings.Count(s, "?")
	if l%paramNum != 0 {
		return errors.New("not enough params")
	}
	ctx, cancel := context.WithTimeout(context.Background(), expire)
	defer cancel()
	tx, err := db.sqldb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer db.rollbackCheck(tx)
	st, err := tx.PrepareContext(ctx, s)
	if err != nil {
		return err
	}
	defer st.Close()
	for i := 0; i < l; i += paramNum {
		_, err := st.ExecContext(ctx, params[i:i+paramNum]...)
		if err != nil {
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) rollbackCheck(tx *sql.Tx) error {
	// recover() 可以捕获当前 goroutine 的 panic
	if r := recover(); r != nil {
		_ = tx.Rollback()
		// 捕获 panic 后继续抛出，避免掩盖严重的程序错误
		panic(r)
	}
	if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
		return err
	}
	return nil
}

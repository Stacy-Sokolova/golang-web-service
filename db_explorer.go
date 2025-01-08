package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type DbExplorer struct {
	DB *sql.DB
}

func NewDbExplorer(DB *sql.DB) (*DbExplorer, error) {
	return &DbExplorer{DB}, nil
}

func GetError(w http.ResponseWriter, status int, err string) {
	result := map[string]interface{}{"error": err}
	data, _ := json.Marshal(&result)
	w.WriteHeader(status)
	w.Write(data)
}

func (h *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	method := r.Method
	switch method {
	case "GET", "":
		h.Read(w, r)
	case "POST":
		h.Update(w, r)
	case "DELETE":
		h.Delete(w, r)
	case "PUT":
		h.Create(w, r)
	default:
		GetError(w, http.StatusInternalServerError, "wrong method")
	}
}

type FieldInfo struct {
	ty       string
	pri      bool
	notNul   bool
	autoIncr bool
	def      interface{}
}

func (h *DbExplorer) Delete(w http.ResponseWriter, r *http.Request) {
	url := r.URL
	path := strings.Split(url.Path[1:], "/")
	table := path[0]
	id := path[1]

	var PRIfield string
	q := "SHOW FULL COLUMNS FROM " + path[0]
	rows, err := h.DB.Query(q)
	if err != nil {
		GetError(w, http.StatusInternalServerError, err.Error())
		return
	}
	colNames, _ := rows.Columns()
ROWS_LOOP:
	for rows.Next() {
		info := make([]sql.RawBytes, len(colNames))
		pointers := make([]interface{}, len(colNames))
		for i := range colNames {
			pointers[i] = &info[i]
		}
		rows.Scan(pointers...)

		for i, n := range colNames {
			if n == "Key" {
				val := string(*pointers[i].(*sql.RawBytes))
				if val == "PRI" {
					PRIfield = string(*pointers[0].(*sql.RawBytes))
					break ROWS_LOOP
				}
			}
		}

	}
	rows.Close()

	query := "DELETE FROM " + table + " WHERE " + PRIfield + " = ?"

	res, err := h.DB.Exec(query, id)
	if err != nil {
		GetError(w, http.StatusInternalServerError, err.Error())
		return
	}

	affected, _ := res.RowsAffected()

	result := map[string]interface{}{"response": map[string]interface{}{"deleted": affected}}
	data, _ := json.Marshal(&result)
	w.WriteHeader(http.StatusOK)
	w.Write(data)

}

// POST
func (h *DbExplorer) Update(w http.ResponseWriter, r *http.Request) {
	url := r.URL
	path := strings.Split(url.Path[1:], "/")
	table := path[0]
	id := path[1]

	body, _ := io.ReadAll(r.Body)
	defer r.Body.Close()

	item := map[string]interface{}{}
	json.Unmarshal(body, &item)

	fields := []string{}
	values := []interface{}{}
	for f, val := range item {
		fields = append(fields, f)
		values = append(values, val)
	}

	var PRIfield string
	q := "SHOW FULL COLUMNS FROM " + table
	rows, _ := h.DB.Query(q)
	defer rows.Close()
	colNames, _ := rows.Columns()

	for rows.Next() { //проверить что field не primary key, и тип values проходит валидацию
		info := make([]sql.RawBytes, len(colNames))
		pointers := make([]interface{}, len(colNames))
		for i := range colNames {
			pointers[i] = &info[i]
		}
		rows.Scan(pointers...)

		if string(*pointers[4].(*sql.RawBytes)) == "PRI" { //Key
			PRIfield = string(*pointers[0].(*sql.RawBytes))
		}

		field := string(*pointers[0].(*sql.RawBytes))
		for f := range fields {
			if fields[f] == field { //если это то поле, которое мы хотим обновить, то
				for i, n := range colNames {
					if n == "Key" { //проверяем на прай ки, который менять нельзя
						val := string(*pointers[i].(*sql.RawBytes))
						if val == "PRI" {
							GetError(w, http.StatusBadRequest, "field "+field+" have invalid type")
							return
						}
					}
					if n == "Type" { //проверяем данные
						t := string(*pointers[i].(*sql.RawBytes))
						v := fmt.Sprintf("%T", values[f])
						fmt.Println("must be ", t, " got ", v)
						if values[f] != nil {
							if v == "float64" && t != "int" {
								GetError(w, http.StatusBadRequest, "field "+field+" have invalid type")
								return
							}
						} else {
							canBeNull := string(*pointers[3].(*sql.RawBytes))
							if canBeNull == "NO" {
								GetError(w, http.StatusBadRequest, "field "+field+" have invalid type")
								return
							}
						}
					}
				}
			}
		}
	}

	for i := range fields {
		fields[i] = "`" + fields[i] + "` = ?"
	}

	values = append(values, id)
	query := "UPDATE " + table + " SET " + strings.Join(fields, ",") + " WHERE " + PRIfield + " = ?"
	res, err := h.DB.Exec(query, values...)
	if err != nil {
		GetError(w, http.StatusInternalServerError, err.Error())
		return
	}

	affected, err := res.RowsAffected()
	if err != nil {
		GetError(w, http.StatusInternalServerError, err.Error())
		return
	}

	result := map[string]interface{}{"response": map[string]interface{}{"updated": affected}}
	data, _ := json.Marshal(&result)
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// PUT
func (h *DbExplorer) Create(w http.ResponseWriter, r *http.Request) {
	url := r.URL
	path := strings.Split(url.Path[1:], "/")
	table := path[0]

	allFields := map[string]FieldInfo{}
	var PRIfield string
	q := "SHOW FULL COLUMNS FROM " + table
	rows, err := h.DB.Query(q)
	if err != nil {
		GetError(w, http.StatusInternalServerError, err.Error())
		return
	}
	colNames, _ := rows.Columns()
	for rows.Next() {
		info := make([]sql.RawBytes, len(colNames))
		pointers := make([]interface{}, len(colNames))
		for i := range colNames {
			pointers[i] = &info[i]
		}
		rows.Scan(pointers...)

		field := FieldInfo{}
		for i, n := range colNames {
			if n == "Extra" {
				val := string(*pointers[i].(*sql.RawBytes))
				if val == "auto_increment" {
					field.autoIncr = true
				}
			}
			if n == "Key" {
				if string(*pointers[i].(*sql.RawBytes)) == "PRI" {
					field.pri = true
					PRIfield = string(*pointers[0].(*sql.RawBytes))
				}
			}
			if n == "Null" {
				if string(*pointers[i].(*sql.RawBytes)) == "NO" {
					field.notNul = true
				}
			}
			if n == "Default" {
				if *pointers[i].(*sql.RawBytes) != nil {
					field.def = interface{}(*pointers[i].(*sql.RawBytes))
				}
			}
		}
		allFields[string(*pointers[0].(*sql.RawBytes))] = field

	}
	rows.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		GetError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer r.Body.Close()

	item := map[string]interface{}{}
	err = json.Unmarshal(body, &item)
	if err != nil {
		GetError(w, http.StatusInternalServerError, err.Error())
		return
	}

	fields := []string{}
	values := []interface{}{}
	for f, info := range allFields {
		val, ok := item[f]
		//если поле указано и оно не primary key и не auto increment
		if ok && !info.autoIncr && !info.pri {
			fields = append(fields, "`"+f+"`")
			values = append(values, val)
		}
		//если поле не указано, но оно не должно быть nil
		if !ok && info.notNul {
			if info.def != nil {
				switch info.def {
				case "NULL":
					values = append(values, nil)
				default:
					values = append(values, info.def)
				}
				fields = append(fields, "`"+f+"`")
			} else {
				switch info.ty {
				case "int":
					values = append(values, 0)
				default:
					values = append(values, "")
				}
				fields = append(fields, "`"+f+"`")
			}
		}
	}

	query := "INSERT INTO " + table + " (" + strings.Join(fields, ", ") + ") VALUES ("
	for i := range values {
		if i != len(values)-1 {
			query += "?, "
		} else {
			query += "?)"
		}
	}

	res, err := h.DB.Exec(query, values...)
	if err != nil {
		GetError(w, http.StatusInternalServerError, err.Error())
		return
	}

	insertedID, err := res.LastInsertId()
	if err != nil {
		GetError(w, http.StatusInternalServerError, err.Error())
		return
	}

	result := map[string]interface{}{"response": map[string]interface{}{PRIfield: insertedID}}
	data, _ := json.Marshal(&result)
	w.WriteHeader(http.StatusOK)
	w.Write(data)

}

func (h *DbExplorer) Read(w http.ResponseWriter, r *http.Request) {
	url := r.URL
	path := strings.Split(url.Path[1:], "/")
	var result map[string]interface{}

	tables := []string{}
	rows, _ := h.DB.Query("SHOW TABLES")
	for rows.Next() {
		var name string
		_ = rows.Scan(&name)
		tables = append(tables, name)
	}
	rows.Close()

	if path[0] == "" {
		result = map[string]interface{}{
			"response": map[string]interface{}{
				"tables": tables,
			},
		}
	} else {
		found := false
		for i := range tables {
			if tables[i] == path[0] {
				found = true
				break
			}
		}
		if !found {
			GetError(w, http.StatusNotFound, "unknown table")
			return
		}

		q := "SHOW FULL COLUMNS FROM " + path[0]
		rows, err := h.DB.Query(q)
		if err != nil {
			GetError(w, http.StatusInternalServerError, err.Error())
			return
		}

		var PRIfield string
		fields := map[string]FieldInfo{}
		colNames, _ := rows.Columns()

		for rows.Next() {
			info := make([]sql.RawBytes, len(colNames))
			pointers := make([]interface{}, len(colNames))
			for i := range colNames {
				pointers[i] = &info[i]
			}
			rows.Scan(pointers...)

			name := string(*pointers[0].(*sql.RawBytes))
			f := FieldInfo{}
			for i, n := range colNames {
				if n == "Key" {
					val := string(*pointers[i].(*sql.RawBytes))
					if val == "PRI" {
						PRIfield = string(*pointers[0].(*sql.RawBytes))
					}
				}
				if n == "Type" {
					f.ty = string(*pointers[i].(*sql.RawBytes))
				}
			}
			fields[name] = f

		}
		rows.Close()

		query := fmt.Sprintf("SELECT * FROM %s", path[0])
		if len(path) == 1 {
			q := url.Query()
			if q.Has("offset") || q.Has("limit") {
				if q.Has("limit") {
					limit := "5"
					_, err := strconv.Atoi(q.Get("limit"))
					if err == nil {
						limit = q.Get("limit")
					}
					query += " LIMIT " + limit
				}
				if q.Has("offset") {
					offset := "0"
					_, err := strconv.Atoi(q.Get("offset"))
					if err == nil {
						offset = q.Get("offset")
					}
					query += " OFFSET " + offset
				}
			}
		}

		if len(path) == 2 {
			itemId := path[1]
			query += " WHERE " + PRIfield + " = " + itemId
		}

		rows, err = h.DB.Query(query)
		if err != nil {
			GetError(w, http.StatusInternalServerError, err.Error())
			return
		}

		colNames, _ = rows.Columns()
		records := []map[string]interface{}{}
		for rows.Next() {
			record := map[string]interface{}{}
			values := make([]sql.RawBytes, len(colNames))
			pointers := make([]interface{}, len(colNames))
			for i := range colNames {
				pointers[i] = &values[i]
			}
			rows.Scan(pointers...)

			for i, n := range colNames {
				val := pointers[i].(*sql.RawBytes)
				if *val == nil {
					record[n] = *val
				} else {
					toType := fields[n].ty
					switch toType {
					case "int":
						x := string(*val)
						num, _ := strconv.Atoi(x)
						record[n] = num
					case "text", "varchar(255)":
						x := string(*val)
						record[n] = x
					default:
						GetError(w, http.StatusInternalServerError, "Unknown type of data")
						return
					}
				}
			}

			records = append(records, record)
		}
		rows.Close()

		if len(path) == 1 {
			result = map[string]interface{}{
				"response": map[string]interface{}{
					"records": records,
				},
			}
		} else {
			if len(records) == 0 {
				GetError(w, http.StatusNotFound, "record not found")
				return
			}
			result = map[string]interface{}{
				"response": map[string]interface{}{
					"record": records[0],
				},
			}
		}
	}

	data, _ := json.Marshal(&result)
	w.WriteHeader(http.StatusOK)
	w.Write(data)

}

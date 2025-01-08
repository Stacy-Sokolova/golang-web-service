package main

import (
	"database/sql"
	"fmt"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
)

var (
	//соединение с базой
	// docker run -p 3306:3306 -v $(PWD):/docker-entrypoint-initdb.d -e MYSQL_ROOT_PASSWORD=1234 -e MYSQL_DATABASE=golang -d mysql
	//DSN = "root:1234@tcp(127.0.0.1:3307)/golang?charset=utf8"
	DSN = "coursera:5QPbAUufx7@tcp(localhost:3306)/coursera?charset=utf8"
)

func main() {
	db, err := sql.Open("mysql", DSN)
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	handler, err := NewDbExplorer(db)
	if err != nil {
		panic(err)
	}

	fmt.Println("starting server at :8082")
	http.ListenAndServe(":8082", handler)
}

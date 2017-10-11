package sqlite3lib

import (
	"database/sql"
	//	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"os"
	"fmt"
	"time"
)
var DNSUpdateRunning bool

type TestItem struct {
	Id    string
	Name  string
	Phone string
}
type SyslogItem struct {
	Id         string //uint32
	Logtype    string //uint16
	Subtype    string //uint16
	Sysdate    string
	Routerdate string
	Routername string
	Msgbody    string
	MacAddr    string
	Domain     string
	SrcIP      string
	SrcIPi     uint32
	SrcPort    string //uint16
	DstIP      string
	DstIPi     uint32
	DstPort    string //uint16
	Prot       uint8
	IPlink	   string
}
type DomainItem struct {
	Domain string
	IP     []string
}

type DomainName struct{
	DN	string
	IP 	string
}

type User struct {
	Id	string
	MAC string
	IP     string
	Name	string
}

func InitDB(filepath string) *sql.DB {
	db, err := sql.Open("sqlite3", filepath+"?_busy_timeout=5000")
	if err != nil {
		panic(err)
	}
	if db == nil {
		panic("db nil")
	}
	return db
}

func CreateTable(db *sql.DB) {
	// create table if not exists
	sql_table := `
	CREATE TABLE IF NOT EXISTS userlog(
		Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		Logtype SMALLINT NOT NULL,
		Subtype SMALLINT,
		Sysdate datetime,
		Routerdate datetime,
		Routername TEXT,
		Msgbody TEXT,
		MacAddr TEXT,
		Domain TEXT,
		SrcIP TEXT,
		SrcIPi	BIGINT,
		SrcPort INTEGER,
		DstIP TEXT,
		DstIPi BIGINT,
		DstPort INTEGER,
		Prot TEXT,
		IPlink INTEGER
	);
	`

	_, err := db.Exec(sql_table)
	if err != nil {
		panic(err)
	}
	sql_table1 := `
	CREATE TABLE IF NOT EXISTS routername(
		Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		Routername TEXT NOT NULL
	);
	`
	_, err1 := db.Exec(sql_table1)
	if err1 != nil {
		panic(err1)
	}
	sql_table2 := `
	CREATE TABLE IF NOT EXISTS user(
		Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		Mac TEXT,
		IP TEXT,
		Name TEXT
	);
	`
	_, err2 := db.Exec(sql_table2)
	if err2 != nil {
		panic(err2)
	}
	sql_table3 := `
	CREATE TABLE IF NOT EXISTS dns(
		Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		Domain TEXT,
		IP TEXT,
		updatetime datetime
	);
	`
	_, err3 := db.Exec(sql_table3)
	if err3 != nil {
		panic(err3)
	}

}
func StoreDnsItems(db *sql.DB, items DomainItem) {
	sql_deleteitem := `
	DELETE FROM dns where Domain=?
	`
	stmt,err := db.Prepare(sql_deleteitem)
	if err!=nil{
		fmt.Println(items.Domain)
		panic(err)
	}
	defer stmt.Close()
	_,err1 := stmt.Exec(items.Domain)
	if err1!=nil{
		//panic(err1)
		Errortofile(err1.Error())
		return //just exit on database lock, no retry
	}
	sql_additem := `
	INSERT INTO dns(
		Domain, IP, updatetime) values(?, ?, CURRENT_TIMESTAMP)
	`
	stmt, err3 := db.Prepare(sql_additem)
	if err3 != nil {
		panic(err3)
	}
	for i :=0; i<len(items.IP);i++{
		_, err2 := stmt.Exec(items.Domain, items.IP[i])
		if err2 != nil {
			panic(err2)
		}
	}
}
func StoreItems(db *sql.DB, items []SyslogItem) {
	sql_additem := `
	INSERT INTO userlog(
		Id,	Logtype, Subtype, Sysdate, Routerdate, Routername, Msgbody, MacAddr, Domain, SrcIP, 
		SrcIPi, SrcPort, DstIP, DstIPi,	DstPort, Prot, IPlink
	) values(?, ?, ?, CURRENT_TIMESTAMP,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	stmt, err := db.Prepare(sql_additem)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	for _, item := range items {
		_, err2 := stmt.Exec("", item.Logtype, item.Subtype, item.Sysdate, item.Routerdate, item.Routername, item.Msgbody, item.MacAddr, item.Domain, item.SrcIP,
			item.SrcIPi, item.SrcPort, item.DstIP, item.DstIPi, item.DstPort, item.Prot, item.IPlink)
		if err2 != nil {
			panic(err2)
		}
	}
}

func IPmapInit(db *sql.DB) []User{
	sql :=`
	select * from user order by Id asc
	`
	rows, err := db.Query(sql)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var result []User
	for rows.Next() {
		item:=User{}
		err2 := rows.Scan(&item.Id, &item.IP, &item.MAC, &item.Name)
		if err2 != nil {
			panic(err2)
		}
		result = append(result, item)
	}
	return result
}

func StoreItem(db *sql.DB, item SyslogItem) {
	sql_additem := `
	INSERT INTO userlog(
		Logtype, Subtype, Sysdate, Routerdate, Routername, Msgbody, MacAddr, Domain, SrcIP, 
		SrcIPi, SrcPort, DstIP, DstIPi,	DstPort, Prot, IPlink
	) values(?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	stmt, err := db.Prepare(sql_additem)
	if err != nil {
		//		fmt.Println(sql_additem)
		panic(err)
	}
	defer stmt.Close()

	_, err2 := stmt.Exec(item.Logtype, item.Subtype, item.Sysdate, item.Routerdate, item.Routername, item.Msgbody, item.MacAddr, item.Domain, item.SrcIP,
		item.SrcIPi, item.SrcPort, item.DstIP, item.DstIPi, item.DstPort, item.Prot, item.IPlink)
	if err2 != nil {
		//		fmt.Println("", item.Logtype, item.Subtype, item.Sysdate, item.Routerdate, item.Routername, item.Msgbody, item.MacAddr, item.Domain, item.SrcIP,
		//			item.SrcIPi, item.SrcPort, item.DstIP, item.DstIPi, item.DstPort, item.Prot)
		panic(err2)
	}

}

func UpdateDomain(db *sql.DB){
	DNSUpdateRunning = true
	defer func(){
		DNSUpdateRunning = false
	}()
	sql := `
	select distinct Ip,Domain from dns where Ip in (select DstIP from userlog where Subtype=51) and julianday(updatetime)-julianday('now')<600
	`
	rows, err := db.Query(sql)
	defer rows.Close()
	if err != nil{
		panic(err)
	}
	for rows.Next(){
		var result DomainName
		rows.Scan(&result.DN,&result.IP)
		fmt.Println("++++++++++",result)
		sql1:=`
		update userlog set Domain=? where DstIP=?
		`
		stmt,err2 := db.Prepare(sql1)
		if err2!=nil{
			panic(err2)
		}
		defer stmt.Close()
		_,err3 := stmt.Exec(result.DN,result.IP)
		if err3 != nil {
			panic(err3)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func StoreUser(db *sql.DB, item User) int64 {
	sql_additem := `
	INSERT INTO user(
		Mac, IP, Name
	) values(?, ?, ?)
	`
	stmt, err := db.Prepare(sql_additem)
	if err != nil {
		//		fmt.Println(sql_additem)
		panic(err)
	}
	defer stmt.Close()

	res, err2 := stmt.Exec(item.MAC, item.IP, item.Name)
	if err2 != nil {
		panic(err2)
	}
	id,err2:=res.LastInsertId()
	return id
}

func ReadItem(db *sql.DB) []TestItem {
	sql_readall := `
	SELECT Id, Name, Phone FROM items
	ORDER BY datetime(InsertedDatetime) DESC
	`

	rows, err := db.Query(sql_readall)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var result []TestItem
	for rows.Next() {
		item := TestItem{}
		err2 := rows.Scan(&item.Id, &item.Name, &item.Phone)
		if err2 != nil {
			panic(err2)
		}
		result = append(result, item)
	}
	return result
}
func Errortofile(s string) bool{
	f, err := os.OpenFile("programlog.txt", os.O_APPEND, 0666)
	defer f.Close()
	if err != nil {
		panic(err)
	}
	f.WriteString(s)
	return true
}
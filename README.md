# gomDB

golang ORM库，目前仅支持mysql

###使用

```go
import (  
	"database/sql"  
	//"errors"  
	"fmt"  
	_ "github.com/go-sql-driver/mysql"  
  "github.com/gkyh/gomdb"  
)  

  db, _ := sql.Open("mysql", dbusername+":"+dbpassword+"@tcp("+dbhost+")/"+dbname+"?charset=utf8")  
	db.SetMaxOpenConns(200)  
	db.SetMaxIdleConns(10)  
	db.Ping()  
  
  //init 
  mdb := &gomDB{Db: db}  
  
  type Person struct { 
	  Id       int32  `db:"id" key:"auto"`  
	  Userid   int32  `db:"userid"`  
	  Phone    string `db:"phone"`  
	  Status   int32  `db:"status"`  
	  Accname  string `db:"acc_name"`  
	  Accno    string `db:"acc_no"`  

}  
```
###查询

```go
  var a Person  
	var arr []Person  

#####根据主键查询  
	db.FindById(&a, 241)  
  //select * from tb_person where id = 241
#####Where  
  db.Where("userid=? and phone=?", 10001, "13345678900").Get(&a)   
  //select * from  tb_person where userid=10001 and phone="13345678900" 
#####Map  
 m:= map[string]interface{}{"userid":10001,"phone": "13345678900"}
 db.Map(m).Get(&a)  
 //select * from  tb_person where userid=10001 and phone="13345678900"
 
 #####Find  
   db.Where("status=?", 1).Find(&arr)   
  //select * from  tb_person where statuse= 1
  
  db.Where("status=?", 1).Where("accno <> ?","122222").Find(&arr)  
  
 #####Page  
   db.Where("status=?", 1).Page(1,20).Sort("id","desc").Find(&arr)    
   
   //select * from  tb_person where statuse= 1 order by id desc limit 0,20
   //Page(2,20) => limit 20,20
   
  #####Count  
    db.Where("status=?", 1).Count(&arr)  
    db.Model(a).Where("status=?", 1).Count()  
    //select count(*) from  tb_person where statuse= 1  
  
   

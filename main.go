package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

var (
	dbHosts     = flag.String("h", "localhost", "db host name or ip addr. multi hosts with split ,")
	dbName      = flag.String("d", "test", "db name")
	reqNum      = flag.Int("n", 1000, "total request num")
	parallelNum = flag.Int("t", 20, "parallel number")
	isDebug     = flag.Bool("debug", false, "true if debug mode")
	insOnly     = flag.Bool("ins", false, "true if only insert")
	isCount     = flag.Bool("count", false, "true if count query")
)

var sess *gocql.Session

// Test db struct
type Test struct {
	UUID      string
	Code      string
	Text      string
	IsTest    bool
	CreatedAt time.Time
}

const printNum = 100000

func main() {
	flag.Parse()
	initSess()

	// loadTest()
	for i := 0; i < 10000; i++ {
		randomSelect()
	}
}

func loadTest() {
	var sema chan int = make(chan int, *parallelNum)
	begin := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *reqNum; i++ {
		if i%printNum == 0 {
			fmt.Println("num goroutine: ", runtime.NumGoroutine(), ", Done: ", float64(i)/float64(*reqNum), " %")
			runtime.GC()
			debug.FreeOSMemory()
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			sema <- 1
			defer func() { <-sema }()
			userID := "user" + strconv.Itoa(index/10000)
			load(sema, userID)
		}(i)
	}
	wg.Wait()
	end := time.Now()
	fmt.Println(end.Sub(begin))
	count := 0
	if *isCount {
		count = selectCount()
	}
	fmt.Printf("insert num = %+v, select count = %v\n", *reqNum, count)
}

func initSess() {
	cluster := gocql.NewCluster(strings.Split(*dbHosts, ",")...)
	cluster.Keyspace = *dbName
	cluster.Timeout = 60 * time.Second
	cluster.NumConns = 50
	var err error
	sess, err = cluster.CreateSession()
	if err != nil {
		log.Println(err)
	}
}

func load(sema chan int, userID string) {
	code := insert("", userID)
	if !*insOnly {
		selectOne(code)
		update(code)
		deleteOne(code)
		insert(code, userID)
	}
}

func insert(code, userID string) string {
	cql := "insert into test(uuid, code, user_id, text, is_test, created_at) values(?, ?, ?, ?, ?, ?)"
	if code == "" {
		code = generateUID()
	}
	if err := sess.Query(cql, gocql.TimeUUID(), code, userID, "test", true, time.Now()).Exec(); err != nil {
		log.Println(err)
	}
	return code
}

func update(code string) {
	cql := "update test set is_test = false where code = ?"
	err := sess.Query(cql, code).Exec()
	fatalIfErr(err)
	t := selectOne(code)
	if t.IsTest {
		log.Fatal("unexpected is_test = true when after update")
	}
}

func randomSelect() {
	cql := "select * from test.test_by_created_at where user_id = ? limit 100"
	rand.Seed(time.Now().UnixNano())
	userID := "user" + strconv.Itoa(rand.Intn(10000))
	begin := time.Now()
	iter := sess.Query(cql, userID).Iter()
	for iter.Scan() {
	}
	if err := iter.Close(); err != nil {
		log.Println(err)
	}
	end := time.Now()
	fmt.Println(end.Sub(begin))
}

func selectOne(code string) *Test {
	cql := "select uuid, code, text, is_test, created_at from test where code = ?"
	t := &Test{}
	iter := sess.Query(cql, code).Iter()
	for iter.Scan(&t.UUID, &t.Code, &t.Text, &t.IsTest, &t.CreatedAt) {
		pp("result: ", t.UUID, t.Code, t.Text, t.IsTest, t.CreatedAt)
	}

	if err := iter.Close(); err != nil {
		log.Println(err)
	}
	return t
}

func deleteOne(code string) {
	cql := "delete from test where code = ?"
	err := sess.Query(cql, code).Exec()
	fatalIfErr(err)
}

func selectList() {
	t := &Test{}
	iter := sess.Query("select code, text, is_test, created_at from test").Iter()
	for iter.Scan(&t.Code, &t.Text, &t.IsTest, &t.CreatedAt) {
		pp("result: ", t.Code, t.Text, t.IsTest, t.CreatedAt)
	}

	if err := iter.Close(); err != nil {
		log.Println(err)
	}
}

func selectCount() int {
	var count int
	iter := sess.Query("select count(*) from test").Iter()
	if iter.Scan(&count) {
		pp("result count: ", count)
	}

	if err := iter.Close(); err != nil {
		log.Println(err)
	}
	return count
}

func fatalIfErr(err error) {
	if err != nil {
		log.Println(err)
	}
}

// https://qiita.com/shinofara/items/5353df4f4fbdaae3d959
func generateUID() string {
	buf := make([]byte, 10)

	if _, err := rand.Read(buf); err != nil {
		log.Println(err)
	}
	str := fmt.Sprintf("%d%x", time.Now().Unix(), buf[0:10])
	return hex.EncodeToString([]byte(str))
}

func pp(msg string, i ...interface{}) {
	if *isDebug {
		fmt.Println(msg, i)
	}
}

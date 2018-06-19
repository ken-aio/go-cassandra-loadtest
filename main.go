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
	insCount    = flag.Int("nn", 1000, "insert count per user")
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
	UserID    string
	Text      string
	IsTest    bool
	CreatedAt time.Time
}

const printNum = 10000

func main() {
	flag.Parse()
	initSess()

	loadTest()
	// selectTest()
	// mateViewTest()
}

func mateViewTest() {
	userID := "user" + strconv.Itoa(rand.Intn(10000))
	loopNum := *reqNum / *insCount
	for i := 0; i < loopNum; i++ {
		multiInsert(userID)
		userID = "user" + strconv.Itoa(rand.Intn(10000))
	}
	fmt.Printf("insert num = %+v\n", *reqNum)
}

func multiInsert(userID string) {
	var sema chan int = make(chan int, *parallelNum)
	begin := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *insCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			sema <- 1
			defer func() { <-sema }()
			insert("", userID)
		}(i)
	}
	wg.Wait()
	anyCode := insert("", userID)
	deleteOne(userID, anyCode)
	end := time.Now()
	fmt.Println(end.Sub(begin))
	tBegin := time.Now()
	tableCount := selectByUserID(userID)
	tEnd := time.Now()
	mBegin := time.Now()
	mateviewCount := selectMateviewByUserID(userID)
	mEnd := time.Now()
	maxLoopCount := 100
	for i := 0; i < maxLoopCount; i++ {
		if tableCount == mateviewCount {
			break
		}
		fmt.Printf("[WARN!!] table /mateview not equal...Re select mateview %s = table: %d, mateview: %d\n", userID, tableCount, mateviewCount)
		mateviewCount = selectMateviewByUserID(userID)
	}
	fmt.Printf("%s = table: %d / %s, mateview: %d / %s\n", userID, tableCount, tEnd.Sub(tBegin), mateviewCount, mEnd.Sub(mBegin))
}

func selectTest() {
	begin := time.Now()
	for i := 0; i < 10000; i++ {
		randomSelect()
	}
	end := time.Now()
	fmt.Println(end.Sub(begin))
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
		selectOne(userID, code)
		update(userID, code)
		deleteOne(userID, code)
		insert(code, userID)
	}
	t1 := selectOne(userID, code)
	t2 := selectOneFromMateview(t1)
	if t1.Code != t2.Code {
		fmt.Printf("not match base table and mateview. t1 = %v, t2 = %v\n", t1, t2)
	}
}

func insert(code, userID string) string {
	cql := "insert into test(uuid, code, user_id, text, is_test, created_at) values(?, ?, ?, ?, ?, ?)"
	if code == "" {
		code = generateUID()
	}
	if err := sess.Query(cql, gocql.TimeUUID(), code, userID, "test", true, time.Now()).Exec(); err != nil {
		log.Println("insert", err)
	}
	return code
}

func update(userID, code string) {
	cql := "update test set is_test = false where user_id = ? and code = ?"
	err := sess.Query(cql, userID, code).Exec()
	fatalIfErr(err)
	t := selectOne(userID, code)
	t2 := selectOneFromMateview(t)
	if t.IsTest != t2.IsTest {
		fmt.Printf("unexpected is_test = true when after update. t1 = %v, t2 = %v\n", t, t2)
	}
}

func randomSelect() {
	cql := "select code, user_id, created_at from test.test_by_created_at where user_id = ? limit 100"
	rand.Seed(time.Now().UnixNano())
	userID := "user" + strconv.Itoa(rand.Intn(10000))
	begin := time.Now()
	res := make([]Test, 10000)
	tmp := &Test{}
	iter := sess.Query(cql, userID).Iter()
	for iter.Scan(&tmp.Code, &tmp.UserID, &tmp.CreatedAt) {
		res = append(res, *tmp)
	}
	if err := iter.Close(); err != nil {
		log.Println(err)
	}
	end := time.Now()
	fmt.Println(userID, len(res), end.Sub(begin))
}

func selectOne(userID, code string) *Test {
	cql := "select uuid, user_id, code, text, is_test, created_at from test where user_id = ? and code = ?"
	t := &Test{}
	iter := sess.Query(cql, userID, code).Iter()
	for iter.Scan(&t.UUID, &t.UserID, &t.Code, &t.Text, &t.IsTest, &t.CreatedAt) {
		pp("result: ", t.UUID, t.Code, t.Text, t.IsTest, t.CreatedAt)
	}

	if err := iter.Close(); err != nil {
		log.Println("selectOne", err)
	}
	return t
}

func selectOneFromMateview(t *Test) *Test {
	cql := "select uuid, code, text, is_test, created_at from test_by_created_at where user_id = ? and created_at = ? and code = ?"
	tv := &Test{}
	iter := sess.Query(cql, t.UserID, t.CreatedAt, t.Code).Iter()
	for iter.Scan(&tv.UUID, &tv.Code, &tv.Text, &tv.IsTest, &tv.CreatedAt) {
		pp("result: ", tv.UUID, tv.Code, tv.Text, tv.IsTest, tv.CreatedAt)
	}

	if err := iter.Close(); err != nil {
		log.Println("selectOneFromMateview", err)
	}
	return tv
}

func selectByUserID(userID string) int {
	cql := "select count(*) from test where user_id = ?"
	var c int
	iter := sess.Query(cql, userID).Iter()
	for iter.Scan(&c) {
		pp("result: ", c)
	}

	if err := iter.Close(); err != nil {
		log.Println(err)
	}
	return c
}

func selectMateviewByUserID(userID string) int {
	cql := "select count(*) from test_by_created_at where user_id = ?"
	var c int
	iter := sess.Query(cql, userID).Iter()
	for iter.Scan(&c) {
		pp("result: ", c)
	}

	if err := iter.Close(); err != nil {
		log.Println(err)
	}
	return c
}

func deleteOne(userID, code string) {
	cql := "delete from test where user_id = ? and code = ?"
	//fmt.Println(cql, userID, code)
	err := sess.Query(cql, userID, code).Exec()
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

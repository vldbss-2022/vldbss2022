package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Query struct {
	Query   string `json:"query"`
	ActRows int    `json:"act_rows"`
}

func mustGenQueryOnTitle(db Instance, n int) []*Query {
	cols := []string{"kind_id", "production_year", "episode_of_id", "season_nr", "episode_nr"}
	colL := []int{0, 1900, 0, 0, 0}
	colR := []int{6, 2020, 2528186, 60, 500}

	concurrency := 20
	var cnt int64 = -1
	var wg sync.WaitGroup
	qs := make([]*Query, n)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				colMask := rand.Intn((1<<len(cols))-1) + 1 // 1 ~ (1<<nCols)-1
				nCols := bits.OnesCount64(uint64(colMask))
				if nCols == 0 || nCols > 4 {
					continue
				}
				ampRatio := math.Pow(1.5, float64(nCols))
				var conds []string
				for colIdx := 0; colIdx < len(cols); colIdx++ {
					if colMask&(1<<colIdx) == 0 {
						continue
					}
					col := cols[colIdx]
					colWidth := colR[colIdx] - colL[colIdx]
					predWidth := int(float64(rand.Intn(colWidth)) * ampRatio)
					if predWidth > colWidth-1 {
						predWidth = colWidth - 1
					}
					left := colL[colIdx] + rand.Intn(colWidth-predWidth)
					right := left + predWidth
					conds = append(conds, fmt.Sprintf("%v>%v and %v<%v ", col, left, col, right))
				}
				query := fmt.Sprintf("select * from imdb.title where %v", strings.Join(conds, " and "))

				countQuery := strings.Replace(query, "select *", "select count(*)", 1)
				result := db.MustQuery(countQuery)
				result.Next()
				var actRows int
				if err := result.Scan(&actRows); err != nil {
					panic(fmt.Sprintf("q=%v, err=%v", countQuery, err))
				}
				if err := result.Close(); err != nil {
					panic(fmt.Sprintf("q=%v, err=%v", countQuery, err))
				}
				if actRows == 0 {
					fmt.Printf("ignore q=%v with zero rows\n", query)
					continue
				}

				cur := atomic.AddInt64(&cnt, 1)
				if int(cur) >= n {
					break
				}
				qs[cur] = &Query{
					Query:   query,
					ActRows: actRows,
				}
				fmt.Printf("progress %v/%v, q=%v\n", cur, n, query)
			}
		}()
	}
	wg.Wait()
	return qs
}

func mustGenQuery(nTrain, nTest int) {
	opt := Option{
		Addr:     "172.16.5.173",
		Port:     4000,
		User:     "root",
		Password: "",
		Label:    "",
	}
	db := MustConnectTo(opt)

	mustMarshalTo(mustGenQueryOnTitle(db, nTrain), fmt.Sprintf("train_%v.json", nTrain))
	mustMarshalTo(mustGenQueryOnTitle(db, nTest), fmt.Sprintf("test_%v.json", nTest))
}

func TouchFile(name string) error {
	file, err := os.OpenFile(name, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	return file.Close()
}

func mustMarshalTo(v interface{}, dst string) {
	content, err := json.MarshalIndent(v, "", "\t")
	must(err)
	must(TouchFile(dst))
	must(ioutil.WriteFile(dst, content, 0666))
}

func mustSampleTitleCSV(n int, src, dst string) {
	f, err := os.Open(src)
	must(err)
	reader := bufio.NewScanner(f)
	total := 0
	for reader.Scan() {
		total++
	}
	must(f.Close())

	f, err = os.Open(src)
	must(err)
	reader = bufio.NewScanner(f)
	result := make([]string, 0, n)
	for reader.Scan() {
		if len(result) < n {
			result = append(result, reader.Text())
		} else {
			victim := rand.Intn(total)
			if victim < n {
				result[victim] = reader.Text()
			}
		}
	}
	must(f.Close())
	must(ioutil.WriteFile(dst, []byte(strings.Join(result, "\n")), 0666))
}

func main() {
	//mustGenQuery(20000, 5000)
	//mustSampleTitleCSV(1000, "/Users/zhangyuanjia/Workspace/go/src/lab/imdb/title.csv", "./title_sample_1000.csv")
	//mustSampleTitleCSV(10000, "/Users/zhangyuanjia/Workspace/go/src/lab/imdb/title.csv", "./title_sample_10000.csv")
	mustSampleTitleCSV(20000, "/Users/zhangyuanjia/Workspace/go/src/lab/imdb/title.csv", "./title_sample_20000.csv")
	mustSampleTitleCSV(40000, "/Users/zhangyuanjia/Workspace/go/src/lab/imdb/title.csv", "./title_sample_40000.csv")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

type Option struct {
	Addr     string `toml:"addr"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Label    string `toml:"label"`
}

type Instance interface {
	Exec(sql string) error
	MustExec(sql string)
	ExecInNewSession(sql string) error
	MustQuery(query string) *sql.Rows
	Query(query string) (*sql.Rows, error)
	Version() string
	Opt() Option
	Close() error
}

type instance struct {
	db  *sql.DB
	opt Option
	ver string
}

func (ins *instance) ExecInNewSession(sql string) error {
	begin := time.Now()
	c, err := ins.db.Conn(context.Background())
	if err != nil {
		return nil
	}
	defer c.Close()
	_, err = c.ExecContext(context.Background(), sql)
	if time.Since(begin) > time.Second*3 {
		fmt.Printf("[SLOW-QUERY] access %v with SQL %v cost %v\n", ins.opt.Label, sql, time.Since(begin))
	}
	return err
}

func (ins *instance) MustExec(sql string) {
	if err := ins.Exec(sql); err != nil {
		panic(err)
	}
}

func (ins *instance) Exec(sql string) error {
	begin := time.Now()
	_, err := ins.db.Exec(sql)
	name := ins.opt.Label
	if name == "" {
		name = fmt.Sprintf("%v:%v", ins.opt.Addr, ins.opt.Port)
	}
	if time.Since(begin) > time.Second*3 {
		fmt.Printf("[SLOW-QUERY] access %v with SQL %v cost %v\n", name, sql, time.Since(begin))
	}
	return err
}

func (ins *instance) MustQuery(query string) *sql.Rows {
	ret, err := ins.Query(query)
	if err != nil {
		panic(fmt.Sprintf("%v: %v", query, err))
	}
	return ret
}

func (ins *instance) Query(query string) (*sql.Rows, error) {
	begin := time.Now()
	rows, err := ins.db.Query(query)
	name := ins.opt.Label
	if name == "" {
		name = fmt.Sprintf("%v:%v", ins.opt.Addr, ins.opt.Port)
	}
	if time.Since(begin) > time.Second*3 {
		fmt.Printf("[SLOW-QUERY]access %v with SQL %v cost %v\n", name, query, time.Since(begin))
	}
	return rows, err
}

func (ins *instance) Version() string {
	return ins.ver
}

func (ins *instance) Opt() Option {
	return ins.opt
}

func (ins *instance) Close() error {
	return ins.db.Close()
}

func (ins *instance) initVersion() error {
	rows, err := ins.Query(`SELECT VERSION()`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var version string
	rows.Next()
	if err := rows.Scan(&version); err != nil {
		return err
	}
	tmp := strings.Split(version, "-")
	ins.ver = tmp[2]
	return nil
}

func MustConnectTo(opt Option) Instance {
	dns := fmt.Sprintf("%s:%s@tcp(%s:%v)/%v", opt.User, opt.Password, opt.Addr, opt.Port, "mysql")
	if opt.Password == "" {
		dns = fmt.Sprintf("%s@tcp(%s:%v)/%v", opt.User, opt.Addr, opt.Port, "mysql")
	}
	db, err := sql.Open("mysql", dns)
	if err != nil {
		panic(err)
	}
	if err := db.Ping(); err != nil {
		panic(err)
	}
	ins := &instance{db: db, opt: opt}
	db.SetMaxOpenConns(256)
	return ins
}

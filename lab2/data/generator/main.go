package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/bits"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

//CREATE TABLE `title` (
//	`id` int(11) NOT NULL,
//	`title` varchar(512) NOT NULL,
//	`imdb_index` varchar(5) DEFAULT NULL,
//	`kind_id` int(11) NOT NULL,
//	`production_year` int(11) DEFAULT NULL,
//	`imdb_id` int(11) DEFAULT NULL,
//	`phonetic_code` varchar(5) DEFAULT NULL,
//	`episode_of_id` int(11) DEFAULT NULL,
//	`season_nr` int(11) DEFAULT NULL,
//	`episode_nr` int(11) DEFAULT NULL,
//	`series_years` varchar(49) DEFAULT NULL,
//	`md5sum` varchar(32) DEFAULT NULL,
//	PRIMARY KEY (`id`),
//	KEY `kind_id_title` (`kind_id`),
//	KEY `idx_year` (`production_year`),
//	KEY `idx1` (`production_year`, `kind_id`, `season_nr`),
//	KEY `idx2` (`kind_id`, `production_year`, `episode_nr`));
//['kind_id', 'production_year', 'episode_of_id', 'season_nr', 'episode_nr']
//
//TableScan
//	SELECT * FROM imdb.title USE INDEX(primary) WHERE predicates;
//
//IndexScan
//	SELECT production_year FROM imdb.title USE INDEX(idx_year) WHERE production_year>? AND production_year<? AND predicates;
//	SELECT kind_id FROM imdb.title USE INDEX(kind_id_title) WHERE kind_id>? AND kind_id<? AND predicates;
//
//IndexLookup
//	SELECT * FROM imdb.title USE INDEX(idx_year) WHERE production_year>? AND production_year<? AND predicates;
//	SELECT * FROM imdb.title USE INDEX(kind_id_title) WHERE kind_id>? AND kind_id<? AND predicates;
//
//HashAgg
//	SELECT production_year, count(*) FROM imdb.title WHERE production_year>? AND production_year<? AND predicates;
//
//Sort
//	SELECT * FROM imdb.title USE INDEX(idx_year) WHERE production_year>? AND production_year<? AND predicates ORDER BY production_year;
//
//HashJoin
//	SELECT /*+ HASH_JOIN(t1, t2) */ * FROM imdb.title t1, imdb.title t2 WHERE t1.production_year = t2.production_year AND predicates(t1) AND predicates(t2);
//
//IndexJoin
//	SELECT /*+ INL_JOIN(t1, t2) */ * FROM imdb.title t1, imdb.title t2 WHERE t1.production_year = t2.production_year AND predicates(t1) AND predicates(t2);

func genColPredicates(prefix, col string, passRate float64) string {
	var l, r int
	switch col {
	case "kind_id":
		l, r = 0, 7
	case "production_year":
		l, r = 1900, 2021
	case "episode_of_id":
		l, r = 0, 2528186
	case "season_nr":
		l, r = 0, 61
	case "episode_nr":
		l, r = 0, 501
	default:
		panic(col)
	}
	gap := r - l
	lb := int(float64(gap) * (1 - passRate))
	if lb == 0 {
		lb = 1
	}
	ll := l + rand.Intn(lb)
	rr := ll + int(float64(gap)*passRate)
	if prefix != "" {
		col = prefix + "." + col
	}
	return fmt.Sprintf("%v>=%v AND %v<=%v", col, ll, col, rr)
}

func genPredicates(prefix string, consideredCols []string, passRate float64) string {
	n := len(consideredCols)
	for {
		mask := rand.Intn(1 << n)
		ones := bits.OnesCount64(uint64(mask))
		if ones > 2 || ones == 0 {
			continue
		}

		var preds []string
		for i := 0; i < n; i++ {
			if (1<<i)&mask == 0 {
				continue
			}
			preds = append(preds, genColPredicates(prefix, consideredCols[i], passRate))
		}
		return strings.Join(preds, " AND ")
	}
}

func genQueries(n int) []string {
	cols := []string{"kind_id", "production_year", "episode_of_id", "season_nr", "episode_nr"}
	var qs []string
	for i := 0; i < n; i++ {
		passRate := float64(i) / float64(n)

		// TableScan
		qs = append(qs, fmt.Sprintf("SELECT * FROM imdb.title USE INDEX(primary) WHERE "+genPredicates("", cols, passRate)))

		// IndexScan
		qs = append(qs, fmt.Sprintf("SELECT production_year FROM imdb.title USE INDEX(idx1) WHERE "+
			genColPredicates("", "production_year", passRate)+" AND "+
			genPredicates("", []string{"kind_id", "season_nr"}, 0.95)))
		qs = append(qs, fmt.Sprintf("SELECT kind_id FROM imdb.title USE INDEX(idx2) WHERE "+
			genColPredicates("", "kind_id", passRate)+" AND "+
			genPredicates("", []string{"production_year", "episode_nr"}, 0.95)))

		// IndexLookUp
		qs = append(qs, fmt.Sprintf("SELECT * FROM imdb.title USE INDEX(idx_year) WHERE "+
			genColPredicates("", "production_year", passRate)+" AND "+
			genPredicates("", []string{"kind_id", "episode_of_id", "season_nr", "episode_nr"}, 0.95)))
		//qs = append(qs, fmt.Sprintf("SELECT * FROM imdb.title USE INDEX(kind_id_title) WHERE "+
		//	genColPredicates("", "kind_id", passRate)+" AND "+
		//	genPredicates("", []string{"production_year", "episode_of_id", "season_nr", "episode_nr"}, 0.95)))

		// HashAgg
		qs = append(qs, fmt.Sprintf("SELECT /*+ HASH_AGG() */ production_year, count(*) FROM imdb.title WHERE "+genPredicates("", cols, passRate)+" GROUP BY production_year"))

		// Sort
		qs = append(qs, fmt.Sprintf("SELECT production_year, kind_id FROM imdb.title USE INDEX(idx1) WHERE "+
			genPredicates("", []string{"production_year", "kind_id", "season_nr"}, passRate)+" ORDER BY season_nr"))

		//// HashJoin
		//qs = append(qs, fmt.Sprintf("SELECT /*+ HASH_JOIN(t1, t2) */ * FROM imdb.title t1, imdb.title t2 WHERE t1.production_year = t2.production_year AND "+
		//	genPredicates("t1", []string{"production_year", "episode_of_id", "episode_nr"}, passRate/30)+" AND "+
		//	genPredicates("t2", []string{"production_year", "episode_of_id", "episode_nr"}, passRate/30)))
	}
	return qs
}

func getPlan(db Instance, q string) []string {
	xq := "explain analyze format='true_card_cost' " + q
	begin := time.Now()
	rs := db.MustQuery(xq)
	if time.Since(begin) > time.Second*4 {
		return nil
	}
	var ops []*Operator
	var plan []string
	var idxs []int
	//mysql> explain analyze SELECT * FROM imdb.title USE INDEX(primary) WHERE production_year>1900 AND production_year<2019;
	//+-------------------------+----------+------------+---------+-----------+---------------+-------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------+-----------+------+
	//| id                      | estRows  | estCost    | actRows | task      | access object | execution info                                                                                                                | operator info                                                              | memory    | disk |
	//	+-------------------------+----------+------------+---------+-----------+---------------+-------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------+-----------+------+
	//| TableReader_7           | 250.00   | 122626.33  | 0       | root      |               | time:1.07ms, loops:1, cop_task: {num: 1, max: 1.02ms, proc_keys: 0, rpc_num: 1, rpc_time: 995µs, copr_cache_hit_ratio: 0.00}  | data:Selection_6, net row size:97.5, concurrency:15                        | 163 Bytes | N/A  |
	//| └─Selection_6           | 250.00   | 1815000.00 | 0       | cop[tikv] |               | tikv_task:{time:978.3µs, loops:0}                                                                                             | gt(imdb.title.production_year, 1900), lt(imdb.title.production_year, 2019) | N/A       | N/A  |
	//|   └─TableFullScan_5     | 10000.00 | 1785000.00 | 0       | cop[tikv] | table:title   | tikv_task:{time:978.3µs, loops:0}                                                                                             | keep order:false, stats:pseudo, scan row size:119                          | N/A       | N/A  |
	//+-------------------------+----------+------------+---------+-----------+---------------+-------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------+-----------+------+
	var id, estRows, estCost, actRows, task, accObj, execInfo, opInfo, mem, disk string
	cols, err := rs.Columns()
	must(err)
	plan = append(plan, strings.Join(cols, "\t"))
	for rs.Next() {
		must(rs.Scan(&id, &estRows, &estCost, &actRows, &task, &accObj, &execInfo, &opInfo, &mem, &disk))
		idx, name := formatOpName(id)
		idxs = append(idxs, idx)
		ops = append(ops, &Operator{
			Name:     name,
			EstRows:  estRows,
			ActRows:  actRows,
			Type:     task,
			ExecInfo: execInfo,
			OpInfo:   opInfo,
			Children: nil,
		})
		items := []string{id, estRows, estCost, actRows, task, accObj, execInfo, opInfo, mem, disk}
		for i := range items {
			items[i] = strings.ReplaceAll(items[i], "\t", " ")
		}
		line := strings.Join(items, "\t")
		plan = append(plan, line)
	}
	must(rs.Close())

	for i := 1; i < len(ops); i++ {
		for j := i - 1; j >= 0; j-- {
			if idxs[j] < idxs[i] {
				ops[j].Children = append(ops[j].Children, ops[i])
			}
		}
	}

	return plan
}

func main() {
	qs := genQueries(50)

	opt := Option{
		//Addr:     "127.0.0.1",
		Addr:     "172.16.5.173",
		Port:     4000,
		User:     "root",
		Password: "",
		Label:    "",
	}
	db := MustConnectTo(opt)

	cs := make([]*Case, len(qs))
	concurrency := 7
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := id; j < len(qs); j += concurrency {
				fmt.Printf(">>> worker: %v, progress: %v/%v\n", id, j, len(qs))
				plan := getPlan(db, qs[j])
				if validPlan(plan) {
					cs[j] = &Case{
						Query: qs[j],
						Plan:  plan,
					}
				}
			}
		}(i)
	}
	wg.Wait()

	tmp := make([]*Case, 0, len(cs))
	for _, c := range cs {
		if c != nil {
			tmp = append(tmp, c)
		}
	}

	newGen := len(tmp)
	var ext, total int

	appendMode := true
	if appendMode {
		f, err := os.Open("../test_plans.json")
		must(err)
		var existing []*Case
		jsonDecoder := json.NewDecoder(f)
		must(jsonDecoder.Decode(&existing))
		ext = len(existing)
		tmp = append(tmp, existing...)
		must(f.Close())
	}

	total = len(tmp)
	fmt.Printf(">>>>>>>>> %v+%v => %v\n", ext, newGen, total)

	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := json.NewEncoder(bf)
	jsonEncoder.SetIndent("", "  ")
	jsonEncoder.SetEscapeHTML(false)
	must(jsonEncoder.Encode(tmp))
	must(ioutil.WriteFile("../test_plans.json", bf.Bytes(), 0666))
}

func validPlan(plan []string) bool {
	if plan == nil {
		return false
	}
	for _, l := range plan {
		if strings.Contains(l, "TableDual") {
			return false
		}
		if strings.Contains(l, "IndexJoin") {
			return false
		}
		if strings.Contains(l, "MergeJoin") {
			return false
		}
	}
	return true
}

func formatOpName(name string) (int, string) {
	var idx int
	for ; idx < len(name); idx++ {
		if name[idx] >= 'A' && name[idx] <= 'Z' {
			break
		}
	}
	return idx, strings.Trim(name, " ├─│└─")
}

type Case struct {
	Query string   `json:"query"`
	Plan  []string `json:"plan"`
	//Plan  *Operator `json:"plan"`
}

type Operator struct {
	Name     string      `json:"operator"`
	EstRows  string      `json:"est_rows"`
	ActRows  string      `json:"act_rows"`
	Type     string      `json:"type"`
	ExecInfo string      `json:"exec_info"`
	OpInfo   string      `json:"op_info"`
	Children []*Operator `json:"children"`
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

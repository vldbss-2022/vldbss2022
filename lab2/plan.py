class Operator:
    def __init__(self, id, est_rows, est_cost, act_rows, task, acc_obj, exec_info, op_info, mem, disk):
        self.id = id
        self.est_rows = est_rows
        self.est_cost = est_cost
        self.act_rows = act_rows
        self.task = task
        self.acc_obj = acc_obj
        self.exec_info = exec_info
        self.op_info = op_info
        self.mem = mem
        self.disk = disk
        self.children = []

    def contain_index_lookup(self):
        if self.is_index_lookup():
            return True
        for child in self.children:
            if child.contain_index_lookup():
                return True
        return False

    def contain_hash_join(self):
        if self.is_hash_join():
            return True
        for child in self.children:
            if child.contain_hash_join():
                return True
        return False

    def contain_hash_agg(self):
        if self.is_hash_agg():
            return True
        for child in self.children:
            if child.contain_hash_agg():
                return True
        return False

    def contain_sort(self):
        if self.is_sort():
            return True
        for child in self.children:
            if child.contain_sort():
                return True
        return False

    def is_db_operator(self):
        return self.task == "root"

    def is_kv_operator(self):
        return not self.is_db_operator()

    def is_hash_join(self):
        return "HashJoin" in self.id

    def is_hash_agg(self):
        return "HashAgg" in self.id

    def is_selection(self):
        return "Selection" in self.id

    def is_projection(self):
        return "Projection" in self.id

    def is_sort(self):
        return "Sort" in self.id

    def is_table_reader(self):
        return "TableReader" in self.id and self.is_db_operator()

    def is_table_scan(self):
        return "Table" in self.id and "Scan" in self.id and self.is_kv_operator()

    def is_index_reader(self):
        return "IndexReader" in self.id and self.is_db_operator()

    def is_index_scan(self):
        return "Index" in self.id and "Scan" in self.id and self.is_kv_operator()

    def is_index_lookup(self):
        return "IndexLookUp" in self.id and self.is_db_operator()

    def is_build_side(self):
        return "Build" in self.id

    def is_probe_side(self):
        return "Probe" in self.id

    def est_row_counts(self):
        return float(self.est_rows)

    def tidb_est_cost(self):
        return float(self.est_cost)

    def parse_fields(self, kv_str):
        # k1:v1, k2:v2, ...
        kv_map = {}
        for kv in kv_str.split(","):
            tmp = kv.split(":")
            if len(tmp) != 2:
                continue
            k, v = tmp[0].strip(), tmp[1].strip()
            kv_map[k] = v
        return kv_map

    def row_size(self):
        kv_map = self.parse_fields(self.op_info)
        return float(kv_map['row_size'])

    def batch_size(self):
        assert (self.is_index_lookup())
        kv_map = self.parse_fields(self.op_info)
        return float(kv_map['batch_size'])

    def exec_time_in_ms(self):
        # time:262.6µs, loops:1, Concurrency:OFF
        kv_map = self.parse_fields(self.exec_info)
        t = kv_map["time"]
        if t[-2:] == "ms":
            return float(t[:-2])
        elif t[-2:] == "µs":
            return float(t[:-2]) / 1000
        elif t[-2:] == "ns":
            return float(t[:-2]) / 1000000
        else:  # s
            return float(t[:-1]) * 1000

    def debug_print(self, indent):
        print("%s%s" % (indent, self.id))
        for child in self.children:
            child.debug_print(indent + "  ")


class Plan:
    def __init__(self, query, root):
        self.query = query
        self.root = root

    def debug_print(self):
        print("query: %s" % self.query)
        self.root.debug_print("")
        pass

    def exec_time_in_ms(self):
        return self.root.exec_time_in_ms()

    def tidb_est_cost(self):
        return self.root.tidb_est_cost()

    @staticmethod
    def format_op_id(id):
        for c in ['├', '─', '│', '└']:
            id = id.replace(c, ' ')
        num_prefix_spaces = 0
        for i in range(0, len(id)):
            if id[i] != ' ':
                num_prefix_spaces = i
                break
        return id.strip(" "), num_prefix_spaces

    @staticmethod
    def parse_plan(query, plan):
        plan = plan[1:]
        rows = [row.split("\t") for row in plan]
        operators = []
        prefix_spaces = []
        for i in range(len(rows)):
            r = rows[i]
            id, num_prefix_spaces = Plan.format_op_id(r[0])
            prefix_spaces.append(num_prefix_spaces)
            operators.append(Operator(id, r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]))
            if i > 0:
                j = i - 1
                while j >= 0:
                    if prefix_spaces[j] < prefix_spaces[i]:
                        operators[j].children.append(operators[i])
                        break
                    j -= 1
        return Plan(query, operators[0])


if __name__ == '__main__':
    query = "SELECT * FROM imdb.title USE INDEX(primary) WHERE kind_id>2 AND kind_id<3"
    plan = [
        "id\testRows\testCost\tactRows\ttask\taccess object\texecution info\toperator info\tmemory\tdisk",
        "Projection_4\t115222.00\t562896518.62\t115222\troot\t\ttime:4.12s, loops:114, Concurrency:1\timdb.title.production_year, row_size: 8\t30.2 KB\tN/A",
        "└─TableReader_7\t115222.00\t562550846.62\t115222\troot\t\ttime:4.12s, loops:114, cop_task: {num: 9, max: 687.3ms, min: 169.5ms, avg: 457.4ms, p95: 687.3ms, max_proc_keys: 460749, p95_proc_keys: 460749, tot_proc: 4.1s, tot_wait: 1ms, rpc_num: 9, rpc_time: 4.12s, copr_cache: disabled}\tdata:Selection_6, row_size: 32\t1.04 MB\tN/A",
        "  └─Selection_6\t115222.00\t560678469.12\t115222\tcop[tikv]\t\ttikv_task:{proc max:684ms, min:166ms, p80:639ms, p95:684ms, iters:2510, tasks:9}, scan_detail: {total_process_keys: 2528312, total_process_keys_size: 317698253, total_keys: 7584961, rocksdb: {delete_skipped_count: 29533, key_skipped_count: 7614512, block: {cache_hit_count: 8010, read_count: 0, read_byte: 0 Bytes}}}\tge(imdb.title.kind_id, 4), le(imdb.title.kind_id, 4), row_size: 32\tN/A\tN/A",
        "    └─TableFullScan_5\t2528312.00\t553093533.12\t2528312\tcop[tikv]\ttable:title\ttikv_task:{proc max:664ms, min:160ms, p80:627ms, p95:664ms, iters:2510, tasks:9}\tkeep order:false, row_size: 32\tN/A\tN/A"
    ]

    p = Plan.parse_plan(query, plan)
    print(p.exec_time_in_ms())

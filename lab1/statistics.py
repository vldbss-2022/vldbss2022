import base64
import json
import math
import struct

MIN_VAL = int(-2**64)
MAX_VAL = int(2**64)

class TableStats:
    @staticmethod
    def load_from_json_file(json_file, int_col_names):
        with open(json_file, 'r') as f:
            data = json.load(f)
        return TableStats.load_from_json(data, int_col_names)

    @staticmethod
    def load_from_json(json_data, int_col_names):
        tbl_stats = TableStats()
        tbl_stats.db_name = json_data['database_name']
        tbl_stats.table_name = json_data['table_name']
        tbl_stats.row_count = json_data['count']
        assert(int(json_data['modify_count']) == 0)  # the stats is fresh
        columns = json_data['columns']
        for col_name in int_col_names:  # only consider int column stats
            tbl_stats.columns[col_name] = ColumnStats.load_from_json(columns[col_name], index=False)
        return tbl_stats

    def __init__(self):
        self.db_name = ""
        self.table_name = ""
        self.row_count = 0
        self.modify_count = 0
        self.columns = dict()

    def __repr__(self):
        return f'TableStats{{db_name:{self.db_name}, table_name:{self.table_name}, row_count:{self.row_count}, columns:{self.columns}}}'


class ColumnStats:
    @staticmethod
    def load_from_json(json_data, index):
        stats = ColumnStats()
        stats.null_count = json_data['null_count']
        stats.tot_col_size = json_data['tot_col_size']
        stats.correlation = json_data['correlation']
        histogram = json_data['histogram']
        stats.ndv = histogram['ndv']
        for bucket in histogram['buckets']:
            # only support int type currently
            lower_bound = decode_bucket_bound(bucket['lower_bound'], index)
            upper_bound = decode_bucket_bound(bucket['upper_bound'], index)
            stats.histogram.buckets.append(Bucket(bucket['count'], lower_bound, upper_bound, bucket['repeats']))
        if 'cm_sketch' in json_data and json_data['cm_sketch'] is not None and 'top_n' in json_data['cm_sketch']:
            for topn_item in json_data['cm_sketch']['top_n']:
                # only support int type currently
                data = decode_topn_data(topn_item['data'])
                stats.topn.append(TopNItem(data, topn_item['count']))
        return stats

    def __init__(self):
        self.null_count = 0
        self.tot_col_size = 0  # Only for column. tot_col_size of index stats is 0.
        self.correlation = 0
        self.ndv = 0
        self.histogram = Histogram()
        self.topn = []

    def __repr__(self):
        return f'Stats{{null_count:{self.null_count}, tot_col_size:{self.tot_col_size}, correlation:' \
               f'{self.correlation}, ndv:{self.ndv}, histogram:{self.histogram}, topn:{self.topn}}}'

    def between_row_count(self, left, right):
        """
        between_row_count estimates the row count where the column belongs to [left, right).
        """
        between_count = self.histogram.between_row_count(left, right, self.ndv)
        for item in self.topn:
            if left <= item.data < right:
                between_count += item.row_count
        return between_count

    def min_val(self):
        mi = self.histogram.min_val()
        for item in self.topn:
            mi = min(mi, item.data)
        return mi

    def max_val(self):
        mx = self.histogram.max_val()
        for item in self.topn:
            mx = max(mx, item.data)
        return mx


class Histogram:
    def __init__(self):
        self.buckets = []

    def __repr__(self):
        return f'Histogram{{{self.buckets}}}'

    def not_null_count(self):
        if len(self.buckets) == 0:
            return 0
        return self.buckets[-1].row_count

    def locate_bucket(self, value):
        """
        locate_bucket locates where a value falls in the range of the Histogram.
        Return value:
            exceed: whether the value is larger than the upper bound of the last Bucket of the Histogram.
            bucket_idx: which Bucket does the value fall in if exceed is false(note: the range before a Bucket is also
                considered belong to the Bucket).
            in_bucket: whether the value falls in the Bucket or between the Bucket and the previous Bucket if exceed is
                false.
            match_last_value: whether the value is the last value in the Bucket which has a counter(Bucket.repeat) if
                exceed is false.
        Examples:
            val0 |<-[bkt0]->| |<-[bkt1]->val1(last value)| val2 |<--val3--[bkt2]->| |<-[bkt3]->| val4
            locate_bucket(val0): false, 0, false, false
            locate_bucket(val1): false, 1, true, true
            locate_bucket(val2): false, 2, false, false
            locate_bucket(val3): false, 2, true, false
            locate_bucket(val4): true, 3, false, false
        """
        if len(self.buckets) == 0:
            return True, 0, False, False
        if value > self.buckets[-1].upper_bound:
            return True, len(self.buckets) - 1, False, False
        if value <= self.buckets[0].upper_bound:
            index = 0
        else:
            left, right = 0, len(self.buckets) - 1
            while right - left > 1:
                mid = (left + right) // 2
                if value <= self.buckets[mid].upper_bound:
                    right = mid
                else:
                    left = mid
            index = right
        if value < self.buckets[index].lower_bound:
            return False, index, False, False
        return False, index, True, value == self.buckets[index].upper_bound

    def less_row_count(self, value):
        """
        less_row_count estimates the row count where the column is less than value.
        """
        if len(self.buckets) == 0:
            return 0
        exceed, bucket_index, in_bucket, match_last_value = self.locate_bucket(value)
        if exceed:
            return self.not_null_count()
        pre_count = 0
        if bucket_index > 0:
            pre_count = self.buckets[bucket_index - 1].row_count
        if not in_bucket:
            return pre_count
        cur_count, cur_repeat = self.buckets[bucket_index].row_count, self.buckets[bucket_index].repeats
        if match_last_value:
            return cur_count - cur_repeat
        frac = self.buckets[bucket_index].calculate_fraction(value)
        return pre_count + frac * (cur_count - cur_repeat - pre_count)

    def between_row_count(self, left, right, ndv):
        """
        between_row_count estimates the row count where the column belongs to [left, right).
        """
        left_less_count = self.less_row_count(left)
        right_less_count = self.less_row_count(right)
        if left_less_count >= right_less_count and ndv is not None and ndv > 0:
            return min(right_less_count, self.not_null_count() - left_less_count, self.not_null_count() / ndv)
        return right_less_count - left_less_count

    def min_val(self):
        if len(self.buckets) == 0:
            return MAX_VAL
        return self.buckets[0].lower_bound

    def max_val(self):
        if len(self.buckets) == 0:
            return MIN_VAL
        return self.buckets[len(self.buckets)-1].upper_bound

    @staticmethod
    def construct_from(vals, n_buckets=10):
        vals.sort()
        n_per_bucket = len(vals) / n_buckets
        if n_per_bucket == 0:
            n_per_bucket = 1
        buckets = []
        for val in vals:
            if len(buckets) == 0: # create the first bucket
                buckets.append(Bucket(1, val, val, 1))
                continue
            last_bucket = buckets[len(buckets)-1]
            if last_bucket.upper_bound == val: # val is euqal to last bucket's upper boundary
                last_bucket.row_count += 1
                last_bucket.repeats += 1
            elif last_bucket.row_count < n_per_bucket: # put this value into last bucket
                last_bucket.row_count += 1
                last_bucket.upper_bound = val
                last_bucket.repeats = 1
            else: # create a new bucket
                buckets.append(Bucket(last_bucket.row_count+1, val, val, 1))
        hist = Histogram()
        hist.buckets = buckets
        return hist


class Bucket:
    def __init__(self, row_count, lower_bound, upper_bound, repeats):
        self.row_count = row_count
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.repeats = repeats

    def __repr__(self):
        return f'Bucket{{row_count:{self.row_count}, lower:{self.lower_bound}, upper:{self.upper_bound}, ' \
               f'repeats:{self.repeats}}}'

    def calculate_fraction(self, value):
        """
        calculate_fraction calculates the fraction of the interval [self.lower_bound, self.upper_bound] that lies within
        the interval [self.lower_bound, value] using the continuous-value assumption
        """
        if self.upper_bound < self.lower_bound:
            return 0.5
        if value <= self.lower_bound:
            return 0
        if value >= self.upper_bound:
            return 1
        frac = (value - self.lower_bound) / (self.upper_bound - self.lower_bound)
        if math.isnan(frac) or math.isinf(frac) or frac < 0 or frac > 1:
            return 0.5
        return frac


class TopNItem:
    def __init__(self, data, row_count):
        self.data = data
        self.row_count = row_count

    def __repr__(self):
        return f'TopNItem:{{data:{self.data}, row_count:{self.row_count}}}'


def decode_int(b):
    assert len(b) == 9
    assert b[0] == 3  # intFlag is 3
    return struct.unpack('>Q', b[1:])[0] ^ 0x8000000000000000


def decode_bucket_bound(s, index):
    b = base64.b64decode(s)
    if index:
        return decode_int(b)
    return int(b.decode('utf-8'))


def decode_topn_data(s):
    b = base64.b64decode(s)
    return decode_int(b)


class AVIEstimator:
    """
    Attribute Value Independence (AVI): It assumes that values for different columns were chosen independent of each other. 
    Under this assumption, the combined selectivity for predicates is calculated as sel(col_1) * sel(col_2) ... * sel(col_n).
    """
    @staticmethod
    def estimate(range_query, table_stats):
        sel = 1.0
        for col in range_query.column_names():
            min_val = table_stats.columns[col].min_val()
            max_val = table_stats.columns[col].max_val()
            (left, right) = range_query.column_range(col, min_val, max_val)
            col_cnt = table_stats.columns[col].between_row_count(left+1, right)  # (left, right) -> [left, right)
            col_sel = col_cnt / table_stats.row_count
            sel *= col_sel
        return sel


class ExpBackoffEstimator:
    """
    Exponential BackOff: When columns have correlated values, AVI assumption could cause significant underestimations. 
    Microsoft SQL Server introduced an alternative assumption, termed as Exponential BackOff, where combined selectivity is 
    calculated using only 4 most selective predicates with diminishing impact. That is, combined selectivity is given by 
        s(1) * s(2)^(1/2) * s(3)^(1/4) * s(4)^(1/8), 
    where s(k) represents k-th most selective fraction across all predicates.
    """
    @staticmethod
    def estimate(range_query, table_stats):
        # YOUR CODE HERE
        pass


class MinSelEstimator:
    """
    MinimumSel: returns the combined selectivity as the minimum selectivity across individual predicates
    """
    @staticmethod
    def estimate(range_query, table_stats):
        # YOUR CODE HERE
        pass


from viff.runtime import gather_shares
from twisted.internet.defer import Deferred, DeferredList
from math import floor, log
from collections import deque
import random

# Taken directly from VIFF
def bits_to_val(bits):
    return sum([2**i * b for (i, b) in enumerate(reversed(bits))])

def divide(x, y, l=12):
    """Returns a share of of ``x/y`` (rounded down).

       Precondition:  ``2**l * y < x.field.modulus``.

       If ``y == 0`` return ``(2**(l+1) - 1)``.

       The division is done by making a comparison for every
       i with ``(2**i)*y`` and *x*.
       Protocol by Sigurd Meldgaard.

       Communication cost: *l* rounds of comparison.

       Also works for simple integers:
       >>>divide(3, 3, 2)
       1
       >>>divide(50, 10, 10)
       5
       """
    bits = []
    for i in range(l, -1, -1):
        t = 2**i * y
        cmp = t <= x
        bits.append(cmp)
        x = x - t * cmp
    return bits_to_val(bits)

def simple_sort(rel, key):
    
    def cond_swap(x, y, key):
        b = key(x) < key(y)
        bx = [b * i for i in x]
        by = [b * i for i in y]
        x_zipped = zip(x, bx, by)
        y_zipped = zip(y, bx, by)
        ret_x = [bx + y - by for y, bx, by in y_zipped]
        ret_y = [x - bx + by for x, bx, by in x_zipped]
        return ret_y, ret_x

    res_rel = rel
    for i in range(len(rel)):
        for j in reversed(range(i)):
            res_rel[j], res_rel[j+1] = cond_swap(res_rel[j], res_rel[j+1], key)
    return res_rel

# Taken from VIFF with minor modification
def sort(rel, key, ascending=True):
    # Make a shallow copy -- the algorithm wont be in-place anyway
    # since we create lots of new Shares as we go along.
    if len(rel) < 2:
        return rel
    
    rel = rel[:]

    def bitonic_sort(low, n, ascending):
        if n > 1:
            m = n // 2
            bitonic_sort(low, m, ascending=not ascending)
            bitonic_sort(low + m, n - m, ascending)
            bitonic_merge(low, n, ascending)

    def bitonic_merge(low, n, ascending):
        if n > 1:
            # Choose m as the greatest power of 2 less than n.
            m = 2**int(floor(log(n-1, 2)))
            for i in range(low, low + n - m):
                compare(i, i+m, ascending)
            bitonic_merge(low, m, ascending)
            bitonic_merge(low + m, n - m, ascending)

    def compare(i, j, ascending):

        def xor(a, b):
            # TODO: We use this simple xor until
            # http://tracker.viff.dk/issue60 is fixed.
            return a + b - 2*a*b

        le = key(rel[i]) <= key(rel[j])
            
        # We must swap array[i] and array[j] when they sort in the
        # wrong direction, that is, when ascending is True and
        # array[i] > array[j], or when ascending is False (meaning
        # descending) and array[i] <= array[j].
        #
        # Using array[i] <= array[j] in both cases we see that
        # this is the exclusive-or:
        b = xor(ascending, le)

        # We now wish to calculate
        #
        #   ai = b * array[j] + (1-b) * array[i]
        #   aj = b * array[i] + (1-b) * array[j]
        #
        # which uses four secure multiplications. We can rewrite
        # this to use only one secure multiplication:
        ai, aj = rel[i], rel[j]
        b_ai_aj = [b * (x - y) for x, y in zip(ai, aj)]

        rel[i] = [x - b_x_y for x, b_x_y in zip(ai, b_ai_aj)]
        rel[j] = [y + b_x_y for y, b_x_y in zip(aj, b_ai_aj)]
        
    bitonic_sort(0, len(rel), ascending=ascending)
    return rel

# such defuredz wow
class MagicDeferred:

    def __init__(self, d):
        self.d = d
        self.children = []

    def another(self):
        child = Deferred()
        self.children.append(child)
        return child

    def forward_callbacks(self, rt):

        def forward_to(received, children):
            for child in children:
                rt.handle_deferred_data(child, received)

        rt.schedule_callback(self.d, forward_to, self.children)

def count(rel, cond):
    return sum([cond(row) for row in rel])

def magic(f):

    def wrapper(self, *args, **kwargs):
        md = MagicDeferred(f(self, *args, **kwargs))
        self.mag_defs.append(md)
        return md
    
    return wrapper

def cutofftail(f):
    
    def tail_len_received(tail_len, rel, rt, d):
        without_indicator = [row[:-1] for row in rel]
        rt.handle_deferred_data(d, without_indicator[0:len(rel) - int(tail_len)])

    def wrapper(self, *args, **kwargs):
        result = f(self, *args, **kwargs)
        sorted_by_val = sort(result, lambda x: x[-1], False)
        tail_len = count(sorted_by_val, lambda x: x[-1] == 0)
        opened_tail_len = self.rt.open(tail_len)
        d = Deferred()
        self.rt.schedule_callback(opened_tail_len, tail_len_received, 
            sorted_by_val, self.rt, d)
        return d
    return wrapper

class Rel:

    def __init__(self, rt):
        self.rt = rt
        self.mag_defs = []

    @cutofftail
    def _join(self, rels, join_col, other_join_col):
        rel, other_rel, result = rels[0][1], rels[1][1], []
        for row in rel:
            for other_row in other_rel:
                flag = row[join_col] == other_row[other_join_col]
                result_row = [row[join_col]] \
                           + [val for idx, val in enumerate(row) if idx != join_col] \
                           + [val for idx, val in enumerate(other_row) if idx != other_join_col] \
                           + [flag]
                result.append(result_row)
        return result

    @magic
    def join(self, rel, other_rel, join_col, other_join_col):
        d = DeferredList([rel.another(), other_rel.another()])
        return self.rt.schedule_callback(d, self._join, join_col, 
            other_join_col)

    @cutofftail
    def _aggregate_sum(self, rel, key_col, agg_col):

        # Note: the indicator value of the last element will
        # *always* be 1
        def cond_sum(e1, e2, key, val, ind):
            comp = e1[key] == e2[key]
            val1_copy = e1[val] 
            e1[val] = (1 - comp) * e1[val]
            e1[ind] = (1 - comp) # set indicator
            e2[val] = comp * val1_copy + e2[val]
            return e1, e2

        def tail_len_received(tail_len, rel, aggregated):
            without_indicator = [row[:-1] for row in rel]
            rt.handle_deferred_data(aggregated, 
                without_indicator[0:len(rel) - int(tail_len)])

        rel = [[row[key_col], row[agg_col]] for row in rel]
        sorted_by_key = sort(rel, lambda x: x[0])
        result = [row + [1] for row in sorted_by_key]

        for i in range(len(result) - 1):
            result[i], result[i + 1] = cond_sum(
                result[i], result[i + 1], 0, 1, 2)

        return result

    @magic
    def aggregate_sum(self, rel, key_col, agg_col):
        return self.rt.schedule_callback(rel.another(), self._aggregate_sum, key_col, agg_col)

    def _project(self, rel, comp):
        return [comp(*row) for row in rel]

    @magic        
    def project(self, rel, comp):
        return self.rt.schedule_callback(rel.another(), self._project, comp)
        
    @magic
    def select(self, rel, cond):
        # TODO: implement
        return rel.another()
        
    def _input(self, rel, inputters, field):

        def sizes_received(sizes, rel, shared_rel):
            sizes = [int(size) for size in sizes]
            num_cols = len(rel[0]) # bit of a hack
            combined_rel = []
            for player in self.rt.players:
                if self.rt.id == player:
                    for row in rel:
                        combined_rel.append(
                            [self.rt.input([player], field, col) for col in row]
                        )
                else:
                    for _ in range(sizes[player - 1]):
                        combined_rel.append(
                            [self.rt.input([player], field, None) for _ in range(num_cols)]
                        )
            self.rt.handle_deferred_data(shared_rel, combined_rel)
                
        sizes = self.rt.shamir_share(inputters, field, len(rel))
        sizes = gather_shares([self.rt.open(size) for size in sizes])
        shared_rel = Deferred()
        self.rt.schedule_callback(sizes, sizes_received, rel, shared_rel) 
        return shared_rel
        
    @magic
    def input(self, rel, inputters, field):
        return self._input(rel, inputters, field)
        
    def _output(self, rel):

        def update_value(value, rel, row_idx, col_idx):
            rel[row_idx][col_idx] = int(value)

        def all_gathered(dummy, d, rel):
            rel = [[int(v) for v in row] for row in rel]
            self.rt.handle_deferred_data(d, rel)
                
        to_wait_on = []
        for row_idx, row in enumerate(rel):
            for col_idx, value in enumerate(row):
                opened = self.rt.output(value)
                self.rt.schedule_callback(opened, update_value, rel, row_idx, col_idx)
                to_wait_on.append(opened)
        d = Deferred() 
        dl = gather_shares(to_wait_on)
        self.rt.schedule_callback(dl, all_gathered, d, rel)
        return d

    def output(self, rel):
        return self.rt.schedule_callback(rel.another(), self._output)
    
    def finish(self):
        for md in self.mag_defs:
            md.forward_callbacks(self.rt)

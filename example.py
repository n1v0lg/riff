from optparse import OptionParser
import viff.reactor
viff.reactor.install()
from twisted.internet import reactor
from twisted.internet.defer import DeferredList

from viff.field import GF
from viff.runtime import make_runtime_class, create_runtime, gather_shares, Runtime
from viff.comparison import ComparisonToft07Mixin
from viff.equality import ProbabilisticEqualityMixin
from viff.config import load_config
from viff.util import find_prime

from extensions import Rel, divide
import copy
import random
import subprocess

def read_from_hdfs(input_path):
    input_stream = subprocess.Popen(["hadoop", "fs", "-cat", input_path + "/*"], 
                                    stdout=subprocess.PIPE).stdout
    rel = [[int(val) for val in row.split()] for row in input_stream]
    print "Read in from HDFS: ", rel
    return rel

def write_to_hdfs(rel, output_path):
    
    def to_string(row):
        return ' '.join([str(v) for v in row])

    rel_str = '\n'.join([to_string(row) for row in rel])
    print "Will write to HDFS: "
    print rel_str

    return True

def shutdown_wrapper(_, rt):
    rt.shutdown()

def protocol(rt, Zp):
    
    ext = Rel(rt)
    all_done = []

    selected_input = ext.input(
        read_from_hdfs("/home/nikolaj/Desktop/work/Musketeer/MUSKETEER_ROOT/selected_input"), 
        rt.players, Zp)
    
    local_rev = ext.aggregate_sum(
        selected_input, 0, 1
    )

    first_val_blank_math = ext.project(
        local_rev, lambda e1, e2: [e1 * 0, e2]
    )
    
    first_val_blank = ext.select(first_val_blank_math, None)
    
    total_rev = ext.aggregate_sum(
        first_val_blank, 0, 1
    )
    
    scaled_local_rev = ext.project(
        first_val_blank, lambda e1, e2: [e1, e2 * 100]
    )
    
    local_total_rev = ext.join(
        scaled_local_rev, total_rev, 0, 0
    )

    market_share = ext.project(
        local_total_rev, 
        lambda e1, e2, e3: [e1, divide(e2, e3), e3]
    )

    market_share_squared = ext.project(
        market_share, lambda e1, e2, e3: [e1, e2 * e2, e3]
    )

    hhi = ext.aggregate_sum(
        market_share_squared, 0, 1
    )

    ext.output(hhi, write_to_hdfs, "path")

    ext.finish()

def report_error(err):
    import sys
    sys.stderr.write(str(err))

if __name__ == "__main__":
    parser = OptionParser()
    Runtime.add_options(parser)
    options, args = parser.parse_args()
    pid, players = load_config(args[0])
    Zp = GF(find_prime(2**65, blum=True))
    
    runtime_class = make_runtime_class(
        mixins=[ProbabilisticEqualityMixin, ComparisonToft07Mixin]
    )
    pre_runtime = create_runtime(pid, players, 1, options, 
        runtime_class=runtime_class)
    pre_runtime.addCallback(protocol, Zp)
    pre_runtime.addErrback(report_error)

    reactor.run()

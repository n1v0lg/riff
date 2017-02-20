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
import sys

def inputgen(pid):
    return [(1, 1) for _ in range(100)] if pid == 1 else []

def output(rel, path=""):
    print "Result: ", rel

def protocol(rt, Zp):
    ext = Rel(rt)
    selected_input = ext.scatter(inputgen(rt.id), Zp)
    gathered_keys = ext.gather(selected_input, [0], [1, 2, 3])
    aggregated = ext.aggregate_sum(gathered_keys, 0, 1)
    gathered = ext.gather(aggregated, [1], [1, 2, 3])
    ext.outputwith(gathered, output)
    ext.finish()

def report_error(err):
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

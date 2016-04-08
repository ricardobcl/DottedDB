#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import pysftp
from datetime import datetime
import os
import os.path
import sys
from subprocess import call, check_call
import subprocess
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm # recommended import according to the docs
from matplotlib.ticker import ScalarFormatter
import matplotlib.ticker as ticker
from os import walk
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd

plt.style.use('fivethirtyeight')

################################################################################
#### VARIABLES
################################################################################

bench_ip                = '192.168.112.37'
cluster_ip              = '192.168.112.'
machines                = ['38', '39', '40', '55', '56']
cluster_user            = 'gsd'
cluster_private_key     = '/Users/ricardo/.ssh/gsd_private_key'
bb_summary_path         = '/Users/ricardo/github/DottedDB/benchmarks/priv/summary.r'

local_bb_path           = '/Users/ricardo/github/basho_bench/tests/current/'
cluster_bb_path         = '/home/gsd/basho_bench/tests/current/'
cluster_ycsb_path       = '/home/gsd/YCSB/'
cluster_dotted_path     = '/home/gsd/DottedDB/_build/default/rel/dotted_db/data/stats/current/'
cluster_basic_path      = '/home/gsd/BasicDB/_build/default/rel/basic_db/data/stats/current/'
cluster_dotted_dstat    = '/home/gsd/DottedDB/benchmarks/tests/dstat.csv'
cluster_basic_dstat     = '/home/gsd/BasicDB/benchmarks/tests/dstat.csv'

cluster_path            = '/users/ricardo/github/DottedDB/benchmarks/tests/cluster/'
local_path              = '/Users/ricardo/github/DottedDB/benchmarks/tests/local/'
current_dotted_dir      = '/Users/ricardo/github/DottedDB/benchmarks/tests/current_dotted'
current_basic_dir       = '/Users/ricardo/github/DottedDB/benchmarks/tests/current_basic'

test_path               = '/Users/ricardo/github/DottedDB/benchmarks/tests/'
current_dir             = test_path + 'current'

dotted_dev1_path        = '/Users/ricardo/github/DottedDB/_build/dev/dev1/dotted_db/data/stats/current/'
dotted_dev2_path        = '/Users/ricardo/github/DottedDB/_build/dev/dev2/dotted_db/data/stats/current/'
dotted_dev3_path        = '/Users/ricardo/github/DottedDB/_build/dev/dev3/dotted_db/data/stats/current/'
dotted_dev4_path        = '/Users/ricardo/github/DottedDB/_build/dev/dev4/dotted_db/data/stats/current/'

basic_dev1_path         = '/Users/ricardo/github/BasicDB/_build/dev/dev1/basic_db/data/stats/current/'
basic_dev2_path         = '/Users/ricardo/github/BasicDB/_build/dev/dev2/basic_db/data/stats/current/'
basic_dev3_path         = '/Users/ricardo/github/BasicDB/_build/dev/dev3/basic_db/data/stats/current/'
basic_dev4_path         = '/Users/ricardo/github/BasicDB/_build/dev/dev4/basic_db/data/stats/current/'

""" Create a new folder for the incoming files.
Also, make the folder current a symlink to the new folder.
"""
def create_folder(type=""):
    if type == 'local_dotted' or type == 'local_basic':
        new = local_path + type + '/' + get_folder_time() + '/'
        os.makedirs(new)
        print "New local directory: " + new
    elif type == 'cluster_dotted' or type == 'cluster_basic':
        new = cluster_path + type + '/' + get_folder_time() + '/'
        os.makedirs(new)
        print "New cluster directory: " + new
    else:
        print "Error creating dir: " + type + "!"
        sys.exit(1)
    if type == 'cluster_dotted' or type == 'local_dotted':
        change_current_dotted(new)
    elif type == 'cluster_basic' or type == 'local_basic':
        change_current_basic(new)
    else:
        print "Error creating dir: " + type + "!"
        sys.exit(1)
    change_current(new)

def change_current(f):
    if not os.path.exists(f):
        print "Folder " + f + " does not exist!"
        sys.exit(1)
    else:
        call(["rm","-f", current_dir])
        call(["ln","-s", f, current_dir])
        print "\'current\' now points to: " + f

def change_current_basic(f):
    if not os.path.exists(f):
        print "Folder " + f + " does not exist!"
        sys.exit(1)
    else:
        call(["rm","-f", current_basic_dir])
        call(["ln","-s", f, current_basic_dir])
        print "\'basic_current\' now points to: " + f

def change_current_dotted(f):
    if not os.path.exists(f):
        print "Folder " + f + " does not exist!"
        sys.exit(1)
    else:
        call(["rm","-f", current_dotted_dir])
        call(["ln","-s", f, current_dotted_dir])
        print "\'current_dotted\' now points to: " + f

def get_folder_time():
    now = datetime.now()
    return '%4d%02d%02d_%02d%02d%02d' % (now.year, now.month, now.day, now.hour, now.minute, now.second)



""" Get stat files from the local dev folders
"""
def get_local_files(type):
    print "Getting files from local folders"
    if type != 'local_dotted' and type != 'local_basic':
        print "ERROR: get_files error in path"
        sys.exit(1)
    elif type == 'local_dotted':
        dev1_path = dotted_dev1_path
        dev2_path = dotted_dev2_path
        dev3_path = dotted_dev3_path
        dev4_path = dotted_dev4_path
    elif type == 'local_basic':
        dev1_path = basic_dev1_path
        dev2_path = basic_dev2_path
        dev3_path = basic_dev3_path
        dev4_path = basic_dev4_path
    print "Getting stats files from: dev1"
    os.makedirs(current_dir + '/dev1')
    call(["cp","-r", dev1_path, current_dir + '/dev1'])
    print "Getting stats files from: dev2"
    os.makedirs(current_dir + '/dev2')
    call(["cp","-r", dev2_path, current_dir + '/dev2'])
    print "Getting stats files from: dev3"
    os.makedirs(current_dir + '/dev3')
    call(["cp","-r", dev3_path, current_dir + '/dev3'])
    print "Getting stats files from: dev4"
    os.makedirs(current_dir + '/dev4')
    call(["cp","-r", dev4_path, current_dir + '/dev4'])

def get_local_bb():
    print "Getting basho bench files from local machine"
    os.makedirs(current_dir + '/basho_bench')
    call(["cp","-r", local_bb_path, current_dir + '/basho_bench'])


""" Get stat files from the machines in the cluster
"""
def get_cluster_files(type):
    print "Getting files from remote cluster"
    if type != 'cluster_dotted' and type != 'cluster_basic':
        print "ERROR: get_files error in path"
        sys.exit(1)
    i = 0
    for m in machines:
        i += 1
        machine = cluster_ip + m
        print "Getting stats files from: ", machine
        s = pysftp.Connection(host=machine,username=cluster_user,private_key=cluster_private_key)
        os.makedirs(current_dir + "/node%s/"%i)
        if type == 'cluster_dotted':
            s.get_d(cluster_dotted_path, current_dir + "/node%s/"%i)
            s.get(cluster_dotted_dstat, current_dir + "/node%s/dstat.csv"%i)
        if type == 'cluster_basic':
            s.get_d(cluster_basic_path, current_dir + "/node%s/"%i)
            s.get(cluster_basic_dstat, current_dir + "/node%s/dstat.csv"%i)
        s.close()

def get_cluster_bb():
    print "Getting basho bench files from cluster bench machine"
    s = pysftp.Connection(host=bench_ip,username=cluster_user,private_key=cluster_private_key)
    os.makedirs(current_dir + "/basho_bench")
    s.get_d(cluster_bb_path, current_dir + "/basho_bench")
    s.close()

def get_ycsb(type):
    print "Getting YCSB files from cluster bench machine"
    s = pysftp.Connection(host=bench_ip,username=cluster_user,private_key=cluster_private_key)
    os.makedirs(current_dir + "/ycsb")
    if type == 'cluster_dotted':
        s.get(cluster_ycsb_path + 'dotteddb/dotted_cluster.props', current_dir + "/ycsb/cluster.props")
        s.get(cluster_ycsb_path + 'dotteddb/workload', current_dir + "/ycsb/workload.props")
        s.get(cluster_ycsb_path + 'dotted_load.csv', current_dir + "/ycsb/dotted_load.csv")
        s.get(cluster_ycsb_path + 'dotted_run.csv', current_dir + "/ycsb/dotted_run.csv")
    if type == 'cluster_basic':
        s.get(cluster_ycsb_path + 'mybasicdb/basic_cluster.props', current_dir + "/ycsb/cluster.props")
        s.get(cluster_ycsb_path + 'mybasicdb/workload', current_dir + "/ycsb/workload.props")
        s.get(cluster_ycsb_path + 'basic_load.csv', current_dir + "/ycsb/basic_load.csv")
        s.get(cluster_ycsb_path + 'basic_run.csv', current_dir + "/ycsb/basic_run.csv")
    s.close()



""" Do bb stuff
"""
def do_bashobench(f=''):
    if f == '':
        call(["Rscript","--vanilla",bb_summary_path,"-i",current_dir+"/basho_bench"])
    else:
        call(["Rscript","--vanilla",bb_summary_path,"-i",f+"/basho_bench"])



################################
## PLOTTING
################################




def do_plot(type):
    initial_offset = 20

    if type == 'cluster':
        dotted = np.loadtxt((current_dotted_dir +'/node1/bench_file.csv'), delimiter=':', usecols=[1])
    elif type == 'local':
        dotted = np.loadtxt((current_dotted_dir +'/dev1/bench_file.csv'), delimiter=':', usecols=[1])
    DS = int(dotted[0]/5)-initial_offset
    DE = int(dotted[1]/5)+5

    if type == 'cluster':
        bench = np.loadtxt((current_basic_dir +'/node1/bench_file.csv'), delimiter=':', usecols=[1])
    elif type == 'local':
        bench = np.loadtxt((current_basic_dir +'/dev1/bench_file.csv'), delimiter=':', usecols=[1])

    BS = int(bench[0]/5)-initial_offset
    BE = int(bench[1]/5)+5
    NVnodes = int(bench[6])
    RF = int(bench[7])

    print " "
    print "Dotted: start", DS*5," end", DE*5
    print "Basic : start", BS*5," end", BE*5
    print "Vnodes:", NVnodes
    print "RF:\t", RF

    # print "Plot: Entries per Clock"
    # clock_entries_plot(type, DS,DE,BS,BE)

    # print "Plot: sync transferred size"
    # sync_size_plot(type, DS,DE,BS,BE)

    # print "Plot: Write Latency"
    # repair_latency_plot(type, DS,DE,BS,BE)

    print "Plot: Strip Latency"
    strip_latency_plot(type, DS,DE,BS,BE)

    # print "Plot: Total number of keys"
    # number_keys_plot(type, DS,DE,BS,BE,NVnodes,RF)

    # print "Plot: Sync Hit Ratio"
    # sync_hit_ratio_plot(type, DS,DE,BS,BE)

    # print "Plot: Node Metadata"
    # node_metadate_plot(type, DS,DE,BS,BE,bench)

    # print "Plot: dstat"
    # dstat_plot()


def dstat_plot():
    basic = load_dstat_csv(current_basic_dir)
    dotted = load_dstat_csv(current_dotted_dir)
    print basic, "\nshape: ", basic.shape
    basic2 = mean_matrix(basic)
    print basic2, "\nshape: ", basic2.shape
    dotted2 = mean_matrix(dotted)
    basic3 = basic2[basic2[:,0].argsort()]
    dotted3 = dotted2[dotted2[:,0].argsort()]
    # 0               1       2       3       4       5       6       7           8           9       10      11      12      13      14          15      16      17      18              19              20              21              22          23
    # "system",       "total cpu usage",,,,,,                         "dsk/total",,           "net/total",,   "paging",,      "system",,          "load avg",,,           "memory usage",,,,                                              "swap",
    # "time",         "usr",  "sys",  "idl",  "wai",  "hiq",  "siq",  "read",     "writ",     "recv", "send", "in",   "out",  "int",  "csw",      "1m",   "5m",   "15m",  "used",         "buff",         "cach",         "free",         "used",     "free"
    plt.style.use('fivethirtyeight')
    plt.figure()
    plt.title("Dstat")
    i=0
    for t in basic3[:,0]:
        # plt.plot(i, basic3[i,1], linewidth=2, label='Basic', c='r', marker='^')
        basic3[i,0] = i
        i = i+1
    i=0
    for t in dotted3[:,0]:
        # plt.plot(i, dotted3[i,1], linewidth=2, label='Dotted', c='g', marker='o')
        dotted3[i,0] = i
        i = i+1
    plt.plot(basic3[:,0], basic3[:,1], linewidth=1, label='Basic', c='r')
    plt.plot(dotted3[:,0], dotted3[:,1], linewidth=1, label='Dotted', c='g')
    plt.xlabel('Time (s)')
    plt.ylabel('%')
    plt.legend(loc='upper right')
    # plt.ylim(ymin=-150.0)
    # plt.ylim((-0.2,5))
    # plt.xlim(xmin=-5.0)
    # plt.xlim(xmax=(DE-DS)*5)
    plt.xlim(xmax=750)
    # plt.xlim((0,700))
    # save in PDF
    pp = PdfPages(current_dotted_dir + '/dstat.pdf')
    pp.savefig()
    pp.close()


def plot_ycsb():
    print "YCSB"
    basic = np.loadtxt((current_basic_dir +'/ycsb/basic_run.csv'), delimiter=',', skiprows=2)
    dotted = np.loadtxt((current_dotted_dir +'/ycsb/dotted_run.csv'), delimiter=',', skiprows=2)

    basic_overall = np.array(filter(lambda x: x[0] == '[OVERALL]', basic))
    dotted_overall = np.array(filter(lambda x: x[0] == '[OVERALL]', dotted))

    for op in ['[READ]','[DELETE]','[UPDATE]']:
        basic2 = np.array(filter(lambda x: x[0] == op, basic))
        dotted = np.array(filter(lambda x: x[0] == op, dotted))


def plot_bb():
    print "Throughput"
    ops_plot()

    print "Latencies"
    for op in ['delete','get','update','put']:
        latencies_plot(op)


def clock_entries_paper():
    change_current_basic(cluster_path + 'cluster_basic/entries_rf3/')
    basic2 = load_cluster_basic_csv('entries-per-clock_hist.csv', True)
    basic_rf3 = mean_matrix(basic2)
    change_current_dotted(cluster_path + 'cluster_dotted/entries_rf3/')
    dotted2 = load_cluster_dotted_csv('entries-per-clock_hist.csv', True)
    dotted_rf3 = mean_matrix(dotted2)

    initial_offset_rf3 = 15
    final_offset_rf3 = 10
    dotted = np.loadtxt((current_dotted_dir +'/node1/bench_file.csv'), delimiter=':', usecols=[1])
    DS3 = int(dotted[0]/5)-initial_offset_rf3
    DE3 = int(dotted[1]/5)+final_offset_rf3
    bench = np.loadtxt((current_basic_dir +'/node1/bench_file.csv'), delimiter=':', usecols=[1])
    BS3 = int(bench[0]/5)-initial_offset_rf3
    BE3 = int(bench[1]/5)+final_offset_rf3

    change_current_basic(cluster_path + 'cluster_basic/entries_rf6/')
    basic2 = load_cluster_basic_csv('entries-per-clock_hist.csv', True)
    basic_rf6 = mean_matrix(basic2)
    change_current_dotted(cluster_path + 'cluster_dotted/entries_rf6/')
    dotted2 = load_cluster_dotted_csv('entries-per-clock_hist.csv', True)
    dotted_rf6 = mean_matrix(dotted2)

    initial_offset_rf6 = 20
    final_offset_rf6 = 10
    dotted = np.loadtxt((current_dotted_dir +'/node1/bench_file.csv'), delimiter=':', usecols=[1])
    DS6 = int(dotted[0]/5)-initial_offset_rf6
    DE6 = int(dotted[1]/5)+final_offset_rf6
    bench = np.loadtxt((current_basic_dir +'/node1/bench_file.csv'), delimiter=':', usecols=[1])
    BS6 = int(bench[0]/5)-initial_offset_rf6
    BE6 = int(bench[1]/5)+final_offset_rf6
    # NVnodes = int(bench[6])
    # RF = int(bench[7])

    plt.style.use('fivethirtyeight')
    fig = plt.figure()
    fig.add_axes([0.10, 0.10, 0.87, 0.8])
    plt.title("Number of Clock Entries")
    me = 10
    plt.plot(basic_rf3[BS3:BE3,0]-5*BS3, basic_rf3[BS3:BE3,4], linewidth=2, label='BasicDB, RF=3', c='r', marker='^', markevery=me)
    plt.plot(dotted_rf3[DS3:DE3,0]-5*DS3, dotted_rf3[DS3:DE3,4], linewidth=2, label='DottedDB, RF=3', c='g', marker='s', markevery=me)
    plt.plot(basic_rf6[BS6:BE6,0]-5*BS6, basic_rf6[BS6:BE6,4], linewidth=2, label='BasicDB, RF=6', c='r', marker='o', markevery=me)
    plt.plot(dotted_rf6[DS6:DE6,0]-5*DS6, dotted_rf6[DS6:DE6,4], linewidth=2, label='DottedDB, RF=6', c='g', marker='x', markevery=me)
    plt.xlabel('Time (s)')
    plt.ylabel('Entries per Clock')
    plt.ylim(ymin=-0.2)
    # plt.ylim((-0.2,5))
    plt.xlim(xmin=0)
    # plt.xlim(xmax=(BE-BS)*5)
    plt.xlim(xmax=1280)
    plt.legend(loc='upper left')
    # plt.xlim((0,700))
    # save in PDF
    pp = PdfPages(test_path + 'entries_per_clock_paper.pdf')
    pp.savefig()
    pp.close()




def clock_entries_plot(type, DS,DE,BS,BE):
    if type == 'cluster':
        basic2 = load_cluster_basic_csv('entries-per-clock_hist.csv', True)
        dotted2 = load_cluster_dotted_csv('entries-per-clock_hist.csv', True)
    elif type == 'local':
        basic2 = load_local_basic_csv('entries-per-clock_hist.csv', True)
        dotted2 = load_local_dotted_csv('entries-per-clock_hist.csv', True)
    basic = mean_matrix(basic2)
    dotted = mean_matrix(dotted2)
    # print dotted
    # print mean_matrix(dotted)
    print "\n before: " + str(dotted2.shape) + "\n after: " + str(dotted.shape)
    plt.style.use('fivethirtyeight')
    plt.figure()
    plt.title("Number of Clock Entries")
    # plt.plot(f[:,0], f[:,c[0]], label=c[1], linewidth=3)
    # plt.scatter(basic[:,0], basic[:,4], s=(basic[:,2]/20), label='Basic', c='r', marker='^')
    # plt.scatter(basic[:,0], basic[:,4], s=15, label='Basic', c='r', marker='^')
    # plt.scatter(dotted[:,0], dotted[:,4], s=15, label='Dotted', c='g', marker='o')
    plt.plot(basic[BS:BE,0]-5*BS, basic[BS:BE,4], linewidth=2, label='Basic', c='r', marker='^')
    plt.plot(dotted[DS:DE,0]-5*DS, dotted[DS:DE,4], linewidth=2, label='Dotted', c='g', marker='o')
    plt.xlabel('Time')
    plt.ylabel('Entries per Clock')
    plt.legend()
    plt.ylim(ymin=-0.2)
    # plt.ylim((-0.2,5))
    plt.xlim(xmin=0)
    plt.xlim(xmax=(BE-BS)*5)
    plt.legend(loc='center right')
    # plt.xlim((0,700))
    # save in PDF
    pp = PdfPages(current_dotted_dir + '/entries_per_clock.pdf')
    pp.savefig()
    pp.close()

def sync_size_plot(type, DS,DE,BS,BE):
    if type == 'cluster':
        basic_ctx2 = load_cluster_basic_csv('sync-context-size_hist.csv', False)
        basic_md2 = load_cluster_basic_csv('sync-metadata-size_hist.csv', False)
        basic_pl2 = load_cluster_basic_csv('sync-payload-size_hist.csv', False)
        dotted_ctx2 = load_cluster_dotted_csv('sync-context-size_hist.csv', False)
        dotted_md2 = load_cluster_dotted_csv('sync-metadata-size_hist.csv', False)
        dotted_pl2 = load_cluster_dotted_csv('sync-payload-size_hist.csv', False)
    elif type == 'local':
        basic_ctx2 = load_local_basic_csv('sync-context-size_hist.csv', False)
        basic_md2 = load_local_basic_csv('sync-metadata-size_hist.csv', False)
        basic_pl2 = load_local_basic_csv('sync-payload-size_hist.csv', False)
        dotted_ctx2 = load_local_dotted_csv('sync-context-size_hist.csv', False)
        dotted_md2 = load_local_dotted_csv('sync-metadata-size_hist.csv', False)
        dotted_pl2 = load_local_dotted_csv('sync-payload-size_hist.csv', False)

    basic_ctx = mean_matrix(basic_ctx2)
    basic_md = mean_matrix(basic_md2)
    basic_pl = mean_matrix(basic_pl2)
    dotted_ctx = mean_matrix(dotted_ctx2)
    dotted_md = mean_matrix(dotted_md2)
    dotted_pl = mean_matrix(dotted_pl2)
    plt.style.use('fivethirtyeight')
    # plt.style.use('ggplot')
    # plt.style.use('dark_background')
    plt.figure()
    plt.title("Traffic in Node Synchronization")
    ms = 7
    lw = 3
    factor = (4/1024.0)
    # plt.plot(basic_ctx[:,0], basic_ctx[:,10]*factor, linewidth=2, label='Basic Context', c='g', marker='^', markersize=ms)
    # plt.plot(basic_md[:,0], basic_md[:,10]*factor, linewidth=2, label='Basic Metadata', c='r', marker='^', markersize=ms)
    # plt.plot(basic_pl[:,0], basic_pl[:,10]*factor, linewidth=2, label='Basic Payload', c='b', marker='^', markersize=ms)
    # plt.plot(dotted_ctx[:,0], dotted_ctx[:,10]*factor, linewidth=2, label='Dotted Context', c='g', marker='o', markersize=ms)
    # plt.plot(dotted_md[:,0], dotted_md[:,10]*factor, linewidth=2, label='Dotted Metadata', c='r', marker='o', markersize=ms)
    # plt.plot(dotted_pl[:,0], dotted_pl[:,10]*factor, linewidth=2, label='Dotted Payload', c='b', marker='o', markersize=ms)
    basic_total = (basic_ctx[BS:BE,10] + basic_md[BS:BE,10] + basic_pl[BS:BE,10])
    dotted_total = (dotted_ctx[DS:DE,10] + dotted_md[DS:DE,10] + dotted_pl[DS:DE,10])
    plt.plot(basic_pl[BS:BE,0]-5*BS, basic_total*factor, linewidth=lw, label='BasicDB', color='r', marker='^', markersize=ms, markevery=5)
    plt.plot(dotted_pl[DS:DE,0]-5*DS, dotted_total*factor, linewidth=lw, label='DottedDB', color='g', marker='o', markersize=ms, markevery=5)
    # plt.plot(basic_pl[:,0]-1*interval, basic_total*factor, linewidth=lw, label='Basic Total', color='r')
    # plt.plot(dotted_pl[:,0]-6*interval, dotted_total*factor, linewidth=lw, label='Dotted Total', color='g')
    plt.xlabel('Time')
    plt.ylabel('Size (KBytes)')
    plt.legend()
    plt.ylim(ymin=-0.5)
    # plt.ylim(ymax=1000)
    # plt.ylim((-0.2,5))
    plt.xlim(xmin=0)
    plt.xlim(xmax=(DE-DS)*5)
    # plt.show()
    # plt.xlim((0,700))
    # save in PDF
    pp = PdfPages(current_dotted_dir + '/sync_size.pdf')
    pp.savefig()
    pp.close()


def repair_latency_plot(type, DS,DE,BS,BE):
    if type == 'cluster':
        basic = load_cluster_basic_csv('write-latency_gauge.csv', False)
        dotted = load_cluster_dotted_csv('write-latency_gauge.csv', False)
    elif type == 'local':
        basic = load_local_basic_csv('write-latency_gauge.csv', False)
        dotted = load_local_dotted_csv('write-latency_gauge.csv', False)

    print "\n dotted: " + str(dotted.shape) + "\n basic: " + str(basic.shape)
    plt.style.use('fivethirtyeight')
    plt.figure()
    # plt.title("CDF of Replication Latency")

    # # Estimate the 2D histogram
    # nbins = 100
    # # H, xedges, yedges = np.histogram2d(dotted[:,0], dotted[:,1], bins=nbins)
    # H, xedges, yedges = np.histogram2d(basic[:,0], basic[:,1], bins=nbins)
    # # H needs to be rotated and flipped
    # H = np.rot90(H)
    # H = np.flipud(H)
    # # Mask zeros
    # Hmasked = np.ma.masked_where(H==0,H) # Mask pixels with a value of zero
    # # Plot 2D histogram using pcolor
    # plt.pcolormesh(xedges,yedges,Hmasked)

    basicY = basic[:,1]
    basic_ecdf = sm.distributions.ECDF(basicY)
    basicX = np.linspace(min(basicY), max(basicY))
    basicY2 = basic_ecdf(basicX)
    plt.step(basicX/1000.0, basicY2, label="BasicDB")

    dottedY = dotted[:,1]
    dotted_ecdf = sm.distributions.ECDF(dottedY)
    dottedX = np.linspace(min(dottedY), max(dottedY))
    dottedY2 = dotted_ecdf(dottedX)
    plt.step(dottedX/1000.0, dottedY2, label="DottedDB")

    plt.xlabel('Time (s)')
    plt.xlim(xmin=-1.0)
    # plt.xlim(xmax=13)
    plt.legend()
    pp = PdfPages(current_dotted_dir + '/repair_latency_CDF.pdf')
    pp.savefig()
    pp.close()

def forceAspect(ax,aspect=1):
    im = ax.get_images()
    extent =  im[0].get_extent()
    ax.set_aspect(abs((extent[1]-extent[0])/(extent[3]-extent[2]))/aspect)

def strip_paper():
    change_current_dotted(cluster_path + 'cluster_dotted/strip_hh/')
    delete_hh = load_cluster_dotted_csv('strip-delete-latency_gauge.csv', False)
    write_hh = load_cluster_dotted_csv('strip-write-latency_gauge.csv', False)

    change_current_dotted(cluster_path + 'cluster_dotted/strip_hl/')
    delete_hl = load_cluster_dotted_csv('strip-delete-latency_gauge.csv', False)
    write_hl = load_cluster_dotted_csv('strip-write-latency_gauge.csv', False)

    change_current_dotted(cluster_path + 'cluster_dotted/strip_mh/')
    delete_mh = load_cluster_dotted_csv('strip-delete-latency_gauge.csv', False)
    write_mh = load_cluster_dotted_csv('strip-write-latency_gauge.csv', False)

    change_current_dotted(cluster_path + 'cluster_dotted/strip_ml/')
    delete_ml = load_cluster_dotted_csv('strip-delete-latency_gauge.csv', False)
    write_ml = load_cluster_dotted_csv('strip-write-latency_gauge.csv', False)

    change_current_dotted(cluster_path + 'cluster_dotted/strip_lh/')
    delete_lh = load_cluster_dotted_csv('strip-delete-latency_gauge.csv', False)
    write_lh = load_cluster_dotted_csv('strip-write-latency_gauge.csv', False)

    change_current_dotted(cluster_path + 'cluster_dotted/strip_ll/')
    delete_ll = load_cluster_dotted_csv('strip-delete-latency_gauge.csv', False)
    write_ll = load_cluster_dotted_csv('strip-write-latency_gauge.csv', False)


    plt.style.use('fivethirtyeight')
    fig = plt.figure()
    # fig.add_axes([0.13, 0.10, 0.85, 0.8])


    plt.rcParams.update({'font.size': 10})
    plt.subplot(111)
    # ax = plt.subplot(161)
    # for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
    #             ax.get_xticklabels() + ax.get_yticklabels()):
    #     item.set_fontsize(20)
    # plt.title("CDF of Strip Latency HH")
    plt.title("CDF of Delete Latency")

    # if delete_lh.shape != (0,):
    deleteY = delete_hh[:,1]
    delete_ecdf = sm.distributions.ECDF(deleteY)
    deleteX = np.linspace(min(deleteY), max(deleteY))
    deleteY2 = delete_ecdf(deleteX)
    plt.step(deleteX/1000.0, deleteY2, label="10000ms Strip Interval, 100% Message Loss", lw=2)

    deleteY = delete_hl[:,1]
    delete_ecdf = sm.distributions.ECDF(deleteY)
    deleteX = np.linspace(min(deleteY), max(deleteY))
    deleteY2 = delete_ecdf(deleteX)
    plt.step(deleteX/1000.0, deleteY2, label="10000ms Strip Interval, 10% Message Loss", lw=2)

    deleteY = delete_mh[:,1]
    delete_ecdf = sm.distributions.ECDF(deleteY)
    deleteX = np.linspace(min(deleteY), max(deleteY))
    deleteY2 = delete_ecdf(deleteX)
    plt.step(deleteX/1000.0, deleteY2, label="1000ms Strip Interval, 100% Message LossMH", lw=2)

    deleteY = delete_ml[:,1]
    delete_ecdf = sm.distributions.ECDF(deleteY)
    deleteX = np.linspace(min(deleteY), max(deleteY))
    deleteY2 = delete_ecdf(deleteX)
    plt.step(deleteX/1000.0, deleteY2, label="1000ms Strip Interval, 10% Message LossML", lw=2)

    deleteY = delete_lh[:,1]
    delete_ecdf = sm.distributions.ECDF(deleteY)
    deleteX = np.linspace(min(deleteY), max(deleteY))
    deleteY2 = delete_ecdf(deleteX)
    plt.step(deleteX/1000.0, deleteY2, label="100ms Strip Interval, 100% Message LossLH", lw=2)

    deleteY = delete_ll[:,1]
    delete_ecdf = sm.distributions.ECDF(deleteY)
    deleteX = np.linspace(min(deleteY), max(deleteY))
    deleteY2 = delete_ecdf(deleteX)
    plt.step(deleteX/1000.0, deleteY2, label="100ms Strip Interval, 10% Message Loss", lw=2)

    # writeY = write_hh[:,1]
    # write_ecdf = sm.distributions.ECDF(writeY)
    # writeX = np.linspace(min(writeY), max(writeY))
    # writeY2 = write_ecdf(writeX)
    # plt.step(writeX/1000.0, writeY2, label="Writes")

    plt.xlabel('Time (Seconds)')
    plt.legend(loc='lower right')
    plt.xlim(xmin=-0.5)
    plt.xlim(xmax=20)

    pp = PdfPages(test_path + '/delete_latency_paper.pdf')
    pp.savefig()
    pp.close()


    plt.style.use('fivethirtyeight')
    fig = plt.figure()
    # fig.add_axes([0.13, 0.10, 0.85, 0.8])


    plt.rcParams.update({'font.size': 10})
    plt.subplot(111)
    # ax = plt.subplot(161)
    # for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
    #             ax.get_xticklabels() + ax.get_yticklabels()):
    #     item.set_fontsize(20)
    # plt.title("CDF of Strip Latency HH")
    plt.title("CDF of Write Latency")

    # if write.shape != (0,):
    writeY = write_hh[:,1]
    write_ecdf = sm.distributions.ECDF(writeY)
    writeX = np.linspace(min(writeY), max(writeY))
    writeY2 = write_ecdf(writeX)
    plt.step(writeX/1000.0, writeY2, label="10000ms Strip Interval, 100% Message Loss", lw=2)

    writeY = write_hl[:,1]
    write_ecdf = sm.distributions.ECDF(writeY)
    writeX = np.linspace(min(writeY), max(writeY))
    writeY2 = write_ecdf(writeX)
    plt.step(writeX/1000.0, writeY2, label="10000ms Strip Interval, 10% Message Loss", lw=2)

    writeY = write_mh[:,1]
    write_ecdf = sm.distributions.ECDF(writeY)
    writeX = np.linspace(min(writeY), max(writeY))
    writeY2 = write_ecdf(writeX)
    plt.step(writeX/1000.0, writeY2, label="1000ms Strip Interval, 100% Message LossMH", lw=2)

    writeY = write_ml[:,1]
    write_ecdf = sm.distributions.ECDF(writeY)
    writeX = np.linspace(min(writeY), max(writeY))
    writeY2 = write_ecdf(writeX)
    plt.step(writeX/1000.0, writeY2, label="1000ms Strip Interval, 10% Message LossML", lw=2)

    writeY = write_lh[:,1]
    write_ecdf = sm.distributions.ECDF(writeY)
    writeX = np.linspace(min(writeY), max(writeY))
    writeY2 = write_ecdf(writeX)
    plt.step(writeX/1000.0, writeY2, label="100ms Strip Interval, 100% Message LossLH", lw=2)

    writeY = write_ll[:,1]
    write_ecdf = sm.distributions.ECDF(writeY)
    writeX = np.linspace(min(writeY), max(writeY))
    writeY2 = write_ecdf(writeX)
    plt.step(writeX/1000.0, writeY2, label="100ms Strip Interval, 10% Message Loss", lw=2)

    # writeY = write_hh[:,1]
    # write_ecdf = sm.distributions.ECDF(writeY)
    # writeX = np.linspace(min(writeY), max(writeY))
    # writeY2 = write_ecdf(writeX)
    # plt.step(writeX/1000.0, writeY2, label="Writes")

    plt.xlabel('Time (Seconds)')
    plt.legend(loc='lower right')
    plt.xlim(xmin=-0.5)
    plt.xlim(xmax=20)

    pp = PdfPages(test_path + '/strip_paper.pdf')
    pp.savefig()
    pp.close()




#     plt.subplot(162)
#     plt.title("HL")

#     # if delete_ll.shape != (0,):
#     deleteY = delete_hl[:,1]
#     delete_ecdf = sm.distributions.ECDF(deleteY)
#     deleteX = np.linspace(min(deleteY), max(deleteY))
#     deleteY2 = delete_ecdf(deleteX)
#     plt.step(deleteX/1000.0, deleteY2, label="Deletes")

#     writeY = write_hl[:,1]
#     write_ecdf = sm.distributions.ECDF(writeY)
#     writeX = np.linspace(min(writeY), max(writeY))
#     writeY2 = write_ecdf(writeX)
#     plt.step(writeX/1000.0, writeY2, label="Writes")

#     plt.xlabel('Time (Seconds)')
#     # plt.legend(loc='center right')
#     plt.xlim(xmax=15)


#     plt.subplot(163)
#     plt.title("MH")

#     # if delete_ll.shape != (0,):
#     deleteY = delete_mh[:,1]
#     delete_ecdf = sm.distributions.ECDF(deleteY)
#     deleteX = np.linspace(min(deleteY), max(deleteY))
#     deleteY2 = delete_ecdf(deleteX)
#     plt.step(deleteX/1000.0, deleteY2, label="Deletes")

#     writeY = write_mh[:,1]
#     write_ecdf = sm.distributions.ECDF(writeY)
#     writeX = np.linspace(min(writeY), max(writeY))
#     writeY2 = write_ecdf(writeX)
#     plt.step(writeX/1000.0, writeY2, label="Writes")

#     plt.xlabel('Time (Seconds)')
#     # plt.legend(loc='center right')
#     plt.xlim(xmax=15)


#     plt.subplot(164)
#     plt.title("ML")

#     # if delete_ll.shape != (0,):
#     deleteY = delete_ml[:,1]
#     delete_ecdf = sm.distributions.ECDF(deleteY)
#     deleteX = np.linspace(min(deleteY), max(deleteY))
#     deleteY2 = delete_ecdf(deleteX)
#     plt.step(deleteX/1000.0, deleteY2, label="Deletes")

#     writeY = write_ml[:,1]
#     write_ecdf = sm.distributions.ECDF(writeY)
#     writeX = np.linspace(min(writeY), max(writeY))
#     writeY2 = write_ecdf(writeX)
#     plt.step(writeX/1000.0, writeY2, label="Writes")

#     plt.xlabel('Time (Seconds)')
#     # plt.legend(loc='center right')
#     plt.xlim(xmax=15)


#     plt.subplot(165)
#     plt.title("LH")

#     # if delete_ll.shape != (0,):
#     deleteY = delete_lh[:,1]
#     delete_ecdf = sm.distributions.ECDF(deleteY)
#     deleteX = np.linspace(min(deleteY), max(deleteY))
#     deleteY2 = delete_ecdf(deleteX)
#     plt.step(deleteX/1000.0, deleteY2, label="Deletes")

#     writeY = write_lh[:,1]
#     write_ecdf = sm.distributions.ECDF(writeY)
#     writeX = np.linspace(min(writeY), max(writeY))
#     writeY2 = write_ecdf(writeX)
#     plt.step(writeX/1000.0, writeY2, label="Writes")

#     plt.xlabel('Time (Seconds)')
#     # plt.legend(loc='center right')
#     plt.xlim(xmax=15)


#     plt.subplot(166)
#     plt.title("LL")

#     # if delete_ll.shape != (0,):
#     deleteY = delete_ll[:,1]
#     delete_ecdf = sm.distributions.ECDF(deleteY)
#     deleteX = np.linspace(min(deleteY), max(deleteY))
#     deleteY2 = delete_ecdf(deleteX)
#     plt.step(deleteX/1000.0, deleteY2, label="Deletes")

#     writeY = write_ll[:,1]
#     write_ecdf = sm.distributions.ECDF(writeY)
#     writeX = np.linspace(min(writeY), max(writeY))
#     writeY2 = write_ecdf(writeX)
#     plt.step(writeX/1000.0, writeY2, label="Writes")

#     plt.xlabel('Time (Seconds)')
#     # plt.legend(loc='center right')
#     plt.xlim(xmax=15)

    # plt.axes().set_aspect('equal', adjustable='box')

    # pp = PdfPages(test_path + '/strip_paper.pdf')
    # pp.savefig()
    # pp.close()


def strip_latency_plot(type, DS,DE,BS,BE):
    if type == 'cluster':
        delete = load_cluster_dotted_csv('strip-delete-latency_gauge.csv', False)
        write = load_cluster_dotted_csv('strip-write-latency_gauge.csv', False)
    elif type == 'local':
        delete = load_local_dotted_csv('strip-delete-latency_gauge.csv', False)
        write = load_local_dotted_csv('strip-write-latency_gauge.csv', False)

    print "\n deleted: " + str(delete.shape) + "\n writes: " + str(write.shape)
    plt.style.use('fivethirtyeight')
    plt.figure()
    plt.title("CDF of Strip Latency")

    if delete.shape != (0,):
        deleteY = delete[:,1]
        delete_ecdf = sm.distributions.ECDF(deleteY)
        deleteX = np.linspace(min(deleteY), max(deleteY))
        deleteY2 = delete_ecdf(deleteX)
        plt.step(deleteX/1000.0, deleteY2, label="Deletes")

    writeY = write[:,1]
    write_ecdf = sm.distributions.ECDF(writeY)
    writeX = np.linspace(min(writeY), max(writeY))
    writeY2 = write_ecdf(writeX)
    plt.step(writeX/1000.0, writeY2, label="Writes")

    plt.xlabel('Time (Seconds)')
    plt.legend(loc='center right')
    pp = PdfPages(current_dotted_dir + '/strip_latency.pdf')
    pp.savefig()
    pp.close()



def deletes_paper():
    change_current_basic(cluster_path + 'cluster_basic/deletes_50k/')
    change_current_dotted(cluster_path + 'cluster_dotted/deletes_50k/')

    basic_w = load_cluster_basic_csv('written-keys_hist.csv', True)
    basic_d = load_cluster_basic_csv('deleted-keys_hist.csv', True)
    dotted_wc = load_cluster_dotted_csv('write-completed_hist.csv', True)
    dotted_wi = load_cluster_dotted_csv('write-incomplete_hist.csv', True)
    dotted_d = load_cluster_dotted_csv('deletes-incomplete_hist.csv', True)

    basic2 = np.concatenate([basic_d, basic_w], axis=0)
    basic = mean_matrix(basic2)

    dotted3 = np.concatenate([dotted_wc, dotted_wi], axis=0)
    dotted1 = mean_matrix(dotted3)
    dotted2 = np.concatenate([dotted_d, dotted_wc, dotted_wi], axis=0)
    dotted = mean_matrix(dotted2)

    initial_offset= 12
    final_offset= 35
    dotted_bench = np.loadtxt((current_dotted_dir +'/node1/bench_file.csv'), delimiter=':', usecols=[1])
    DS = int(dotted_bench[0]/5)-initial_offset
    DE = int(dotted_bench[1]/5)+final_offset
    basic_bench = np.loadtxt((current_basic_dir +'/node1/bench_file.csv'), delimiter=':', usecols=[1])
    BS = int(basic_bench[0]/5)-initial_offset
    BE = int(basic_bench[1]/5)+final_offset
    NVnodes = int(basic_bench[6])
    RF = int(basic_bench[7])


    plt.style.use('fivethirtyeight')
    fig = plt.figure()
    fig.add_axes([0.13, 0.10, 0.85, 0.8])
    plt.title("Keys over Time with Deletes")
    me = 10
    lw = 2
    plt.plot(basic[BS:BE,0]-BS*5, basic[BS:BE,10]/basic[BS:BE,2]*2*NVnodes/RF, linewidth=lw, label='BasicDB', marker='o', c='r', markevery=me)
    # plt.plot(basic[:,0]-3*interval, basic[:,10]*2*(16/32.0), linewidth=2, label='Basic', c='r', marker='^')
    # plt.plot(dotted[DS:DE,0]-DS*5, dotted[DS:DE,10]*3*(16/32.0), linewidth=2, label='Dotted', c='g', marker='o')
    plt.plot(dotted[DS:DE,0]-DS*5, dotted[DS:DE,10]/dotted[DS:DE,2]*3*NVnodes/RF, linewidth=lw, label='DottedDB', marker='^', c='g', markevery=me)
    plt.plot(dotted1[DS:DE,0]-DS*5, dotted1[DS:DE,10]/dotted1[DS:DE,2]*2*NVnodes/RF, linewidth=2, label='Ideal', marker='.', c='b', markevery=me)
    plt.xlabel('Time (s)')
    plt.ylabel('Keys')
    plt.legend(loc='lower right')

    plt.ylim(ymin=-0.2)
    # plt.ylim((-0.2,5))
    plt.xlim(xmin=0)
    # plt.xlim(xmax=(BE-BS)*5)
    plt.xlim(xmax=1375)
    # plt.xlim((0,700))
    # save in PDF
    pp = PdfPages(test_path + 'deletes_paper.pdf')
    pp.savefig()
    pp.close()




def number_keys_plot(type, DS,DE,BS,BE,NVnodes,RF):
    if type == 'cluster':
        basic_w = load_cluster_basic_csv('written-keys_hist.csv', True)
        basic_d = load_cluster_basic_csv('deleted-keys_hist.csv', True)
        dotted_wc = load_cluster_dotted_csv('write-completed_hist.csv', True)
        dotted_wi = load_cluster_dotted_csv('write-incomplete_hist.csv', True)
        dotted_d = load_cluster_dotted_csv('deletes-incomplete_hist.csv', True)
    elif type == 'local':
        basic_w = load_local_basic_csv('written-keys_hist.csv', True)
        basic_d = load_local_basic_csv('deleted-keys_hist.csv', True)
        dotted_wc = load_local_dotted_csv('write-completed_hist.csv', True)
        dotted_wi = load_local_dotted_csv('write-incomplete_hist.csv', True)
        dotted_d = load_local_dotted_csv('deletes-incomplete_hist.csv', True)

    basic2 = np.concatenate([basic_d, basic_w], axis=0)
    basic = mean_matrix(basic2)

    dotted3 = np.concatenate([dotted_wc, dotted_wi], axis=0)
    dotted1 = mean_matrix(dotted3)
    dotted2 = np.concatenate([dotted_d, dotted_wc, dotted_wi], axis=0)
    dotted = mean_matrix(dotted2)
    # print "\n basic  before: " + str(basic2.shape) + "\n after: " + str(basic.shape)
    # print "\n dotted before: " + str(dotted2.shape) + "\n after: " + str(dotted.shape)
    plt.style.use('fivethirtyeight')
    fig = plt.figure()
    fig.add_axes([0.15, 0.10, 0.8, 0.8])
    plt.title("Total Number of Keys in the Cluster")
    # plt.plot(basic[:,0]-4*interval, basic[:,9]*2*4, linewidth=2, label='Basic', c='r', marker='^')
    # plt.plot(dotted[:,0]-2*interval, dotted[:,9]*3*4, linewidth=2, label='Dotted', c='g', marker='o')
    lw = 4
    plt.plot(basic[BS:BE,0]-BS*5, basic[BS:BE,10]/basic[BS:BE,2]*2*NVnodes/RF, ls='-.', linewidth=lw, label='BasicDB', c='r')
    # plt.plot(basic[:,0]-3*interval, basic[:,10]*2*(16/32.0), linewidth=2, label='Basic', c='r', marker='^')
    # plt.plot(dotted[DS:DE,0]-DS*5, dotted[DS:DE,10]*3*(16/32.0), linewidth=2, label='Dotted', c='g', marker='o')
    plt.plot(dotted[DS:DE,0]-DS*5, dotted[DS:DE,10]/dotted[DS:DE,2]*3*NVnodes/RF, ls='--',linewidth=lw, label='DottedDB', c='g')
    plt.plot(dotted1[DS:DE,0]-DS*5, dotted1[DS:DE,10]/dotted1[DS:DE,2]*2*NVnodes/RF, ls='-', linewidth=2, label='Ideal', c='b')
    plt.xlabel('Time (s)')
    plt.ylabel('# Keys')
    plt.legend(loc='lower right')
    plt.ylim(ymin=-150.0)
    # plt.ylim((-0.2,5))
    plt.xlim(xmin=60)
    plt.xlim(xmax=(DE-DS)*5+50)
    # plt.xlim(xmax=400)
    # plt.xlim((0,700))
    # save in PDF
    pp = PdfPages(current_dotted_dir + '/total_number_keys.pdf')
    pp.savefig()
    pp.close()

def sync_paper():
    change_current_basic(cluster_path + 'cluster_basic/sync_hhh/')
    change_current_dotted(cluster_path + 'cluster_dotted/sync_hh/')

    basic_m2 = load_cluster_basic_csv('sync-segment-keys-missing_hist.csv', False)
    basic_tm2 = load_cluster_basic_csv('sync-segment-keys-truly-missing_hist.csv', False)
    dotted2 = load_cluster_dotted_csv('sync-hit-ratio_hist.csv', False)
    dotted_m2 = load_cluster_dotted_csv('sync-sent-missing_hist.csv', False)
    dotted_tm2 = load_cluster_dotted_csv('sync-sent-truly-missing_hist.csv', False)

    basic_m = mean_matrix(basic_m2)
    basic_tm = mean_matrix(basic_tm2)
    basic = join_matrix(basic_m, basic_tm)
    # print "\n basic before: " + str(basic.shape) + "\n after: " + str(basic_tm.shape) + "\n\n"

    dotted = mean_matrix(dotted2)
    # print "\n dotted before: " + str(dotted2.shape) + "\n after: " + str(dotted.shape)

    dotted_m = mean_matrix(dotted_m2)
    dotted_tm = mean_matrix(dotted_tm2)
    dotted3 = join_matrix(dotted_m, dotted_tm)
    # print "\n dotted3 before: " + str(dotted3.shape) + "\n after: " + str(dotted_tm.shape) + "\n\n"




    # change_current_basic(cluster_path + 'cluster_basic/sync_hhl/')
    # change_current_dotted(cluster_path + 'cluster_dotted/sync_hl/')

    # change_current_basic(cluster_path + 'cluster_basic/sync_hlh/')
    # change_current_dotted(cluster_path + 'cluster_dotted/sync_lh/')

    # change_current_basic(cluster_path + 'cluster_basic/sync_hll/')
    # change_current_dotted(cluster_path + 'cluster_dotted/sync_ll/')

    # change_current_basic(cluster_path + 'cluster_basic/sync_lhh/')
    # change_current_basic(cluster_path + 'cluster_basic/sync_lhl/')
    # change_current_basic(cluster_path + 'cluster_basic/sync_llh/')
    # change_current_basic(cluster_path + 'cluster_basic/sync_lll/')

    initial_offset= 12
    final_offset= 35
    dotted_bench = np.loadtxt((current_dotted_dir +'/node1/bench_file.csv'), delimiter=':', usecols=[1])
    DS = int(dotted_bench[0]/5)-initial_offset
    DE = int(dotted_bench[1]/5)+final_offset
    basic_bench = np.loadtxt((current_basic_dir +'/node1/bench_file.csv'), delimiter=':', usecols=[1])
    BS = int(basic_bench[0]/5)-initial_offset
    BE = int(basic_bench[1]/5)+final_offset
    NVnodes = int(basic_bench[6])
    RF = int(basic_bench[7])

    plt.style.use('fivethirtyeight')
    fig = plt.figure()
    fig.add_axes([0.13, 0.10, 0.85, 0.8])
    plt.title("")
    me = 10

    basic_pct0 = basic[BS:BE,22]*100/(basic[BS:BE,10]*1.0)
    basic_pct = np.array(map(lambda x: min(x,100), basic_pct0))
    plt.plot(basic[BS:BE,0]-5*BS, basic_pct, linewidth=2, label='BasicDB HHH', c='r', marker='^', markevery=me)
    plt.plot(dotted3[DS:DE,0]-5*DS, dotted3[DS:DE,22]*100/(dotted3[DS:DE,10]*1.0), linewidth=2, label='Dotted3 HH', c='b', marker='x', markevery=5)
    plt.plot(dotted[DS:DE,0]-5*DS, dotted[DS:DE,4], linewidth=2, label='DottedDB HH', c='g', marker='o', markevery=5)

    plt.xlabel('Time')
    plt.ylabel('Percentage (%)')
    plt.legend(loc='lower left')
    # plt.ylim(ymin=-150.0)
    plt.ylim((-1,102))
    plt.xlim(xmin=0)
    plt.xlim(xmax=(BE-BS)*5)
    # plt.xlim(xmax=1375)
    # save in PDF
    pp = PdfPages(test_path + 'hit_ratio_paper.pdf')
    pp.savefig()
    pp.close()




def sync_hit_ratio_plot(type, DS,DE,BS,BE):
    DS = DS-1
    BS = max(BS-1,0)

    if type == 'cluster':
        basic_m2 = load_cluster_basic_csv('sync-segment-keys-missing_hist.csv', False)
        basic_tm2 = load_cluster_basic_csv('sync-segment-keys-truly-missing_hist.csv', False)
        dotted2 = load_cluster_dotted_csv('sync-hit-ratio_hist.csv', False)
        dotted_m2 = load_cluster_dotted_csv('sync-sent-missing_hist.csv', False)
        dotted_tm2 = load_cluster_dotted_csv('sync-sent-truly-missing_hist.csv', False)
    elif type == 'local':
        basic_m2 = load_local_basic_csv('sync-segment-keys-missing_hist.csv', False)
        basic_tm2 = load_local_basic_csv('sync-segment-keys-truly-missing_hist.csv', False)
        dotted2 = load_local_dotted_csv('sync-hit-ratio_hist.csv', False)
        dotted_m2 = load_local_dotted_csv('sync-sent-missing_hist.csv', False)
        dotted_tm2 = load_local_dotted_csv('sync-sent-truly-missing_hist.csv', False)

    basic_m = mean_matrix(basic_m2)
    basic_tm = mean_matrix(basic_tm2)
    basic = join_matrix(basic_m, basic_tm)
    # print "\n basic before: " + str(basic.shape) + "\n after: " + str(basic_tm.shape) + "\n\n"

    dotted = mean_matrix(dotted2)
    # print "\n dotted before: " + str(dotted2.shape) + "\n after: " + str(dotted.shape)

    dotted_m = mean_matrix(dotted_m2)
    dotted_tm = mean_matrix(dotted_tm2)
    dotted3 = join_matrix(dotted_m, dotted_tm)
    # print "\n dotted3 before: " + str(dotted3.shape) + "\n after: " + str(dotted_tm.shape) + "\n\n"

    plt.style.use('fivethirtyeight')
    plt.figure()
    plt.title("Sync Hit Ratio")
    basic_pct0 = basic[BS:BE,22]*100/(basic[BS:BE,10]*1.0)
    basic_pct = np.array(map(lambda x: min(x,100), basic_pct0))
    plt.plot(basic[BS:BE,0]-5*BS, basic_pct, linewidth=2, label='BasicDB', c='r', marker='^', markevery=5)
    # plt.plot(dotted3[DS:DE,0]-5*DS, dotted3[DS:DE,22]*100/(dotted3[DS:DE,10]*1.0), linewidth=2, label='dotted3', c='b', marker='x', markevery=5)
    plt.plot(dotted[DS:DE,0]-5*DS, dotted[DS:DE,4], linewidth=2, label='DottedDB', c='g', marker='o', markevery=5)
    plt.xlabel('Time')
    plt.ylabel('Percentage (%)')
    plt.legend(loc='lower left')
    # plt.ylim(ymin=-150.0)
    plt.ylim((-1,102))
    plt.xlim(xmin=-5.0)
    plt.xlim(xmax=(DE-DS)*5)
    # plt.xlim(xmax=400)
    # plt.xlim((0,700))
    # save in PDF
    pp = PdfPages(current_dotted_dir + '/sync_hit_ratio.pdf')
    pp.savefig()
    pp.close()

def node_metadate_plot(type, DS, DE, BS, BE, bench):
    if type == 'cluster':
        dotted_bvv = load_cluster_dotted_csv('bvv-size_hist.csv', True)
        dotted_kl = load_cluster_dotted_csv('kl-size_hist.csv', True)
        dotted_nsk = load_cluster_dotted_csv('nsk-size_hist.csv', True)
        basic_mt = load_cluster_basic_csv('mt-size_hist.csv', True)
    elif type == 'local':
        dotted_bvv = load_local_dotted_csv('bvv-size_hist.csv', True)
        dotted_kl = load_local_dotted_csv('kl-size_hist.csv', True)
        dotted_nsk = load_local_dotted_csv('nsk-size_hist.csv', True)
        basic_mt = load_local_basic_csv('mt-size_hist.csv', True)

    dotted1 = mean_matrix(dotted_bvv)
    dotted2 = mean_matrix(dotted_kl)
    dotted3 = mean_matrix(dotted_nsk)


    basic = mean_matrix(basic_mt)

    plt.style.use('fivethirtyeight')
    fig = plt.figure()
    fig.add_axes([0.15, 0.10, 0.8, 0.8])
    plt.title("Node Metadata Size")

    print "tamanhos " + str(DS) + " "+ str(DE) + " "+ str(BS) + " "+ str(BE) + " "
    num_keys = bench[2]
    vnodes = bench[6]
    RF = bench[7]
    hash_size = bench[8]
    key_size = bench[9]
    mt = bench[10]
    mt_metadata = 11
    block_size = mt_metadata + hash_size + key_size
    basic_size = (block_size + mt*block_size + (mt**2)*block_size + (RF*num_keys/(vnodes*1.0))*block_size) * RF
    n_lines, _ = dotted1.shape
    basic_total = np.empty(n_lines)
    basic_total.fill(basic_size)
    print str(basic_size/1024.0) + " KB\n"
    dotted_total = (dotted1[:,4] + dotted2[:,4] + dotted3[:,4])
    plt.plot(dotted1[DS:DE,0]-DS*5, basic_total[DS:DE]/1024.0, linewidth=2, label='MT Theoretical Size', c='r', linestyle='--')
    # plt.plot(basic[BS:BE,0]-BS*5, basic_total[DS:DE]/1024.0, linewidth=2, label='MT Theoretical Size', c='r', linestyle='--')
    plt.plot(basic[BS:BE,0]-BS*5, basic[BS:BE,4]/1024.0, linewidth=3, label='BasicDB', c='r', marker='s', markevery=5)
    plt.plot(dotted1[DS:DE,0]-DS*5, dotted_total[DS:DE]/1024.0, linewidth=3, label='DottedDB', c='g', marker='o', markevery=5)
    plt.xlabel('Time')
    plt.ylabel('Size (KB)')
    plt.legend(loc='center right')
    ####### make the y axis logarithmic
    # plt.yscale('log')
    # ax = plt.gca()
    # ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y,pos: ('{{:.{:1d}f}}'.format(int(np.maximum(-np.log10(y),0)))).format(y)))

    # for axis in [ax.xaxis, ax.yaxis]:
        # axis.set_major_formatter(ScalarFormatter())
    # plt.ylim(ymin=-150.0)
    # plt.ylim((-1,101))
    plt.xlim(xmin=-5.0)
    plt.xlim(xmax=(DE-DS)*5)
    plt.ylim(ymin=-5.0)
    # plt.xlim(xmax=400)
    # plt.xlim((0,700))
    # save in PDF
    pp = PdfPages(current_dotted_dir + '/node_metadata.pdf')
    pp.savefig()
    pp.close()

def ops_plot(type):
    basic = np.loadtxt((current_basic_dir +'/basho_bench/summary.csv'), delimiter=',', skiprows=1)
    dotted = np.loadtxt((current_dotted_dir +'/basho_bench/summary.csv'), delimiter=',', skiprows=1)

    plt.style.use('fivethirtyeight')
    fig = plt.figure()
    fig.add_axes([0.15, 0.10, 0.8, 0.8])
    plt.title("Throughput")

    il = -1 #ignore last n lines
    plt.plot(basic[:il,0], basic[:il,3]/basic[:il,1], linewidth=3, label='Basic', c='r', marker='^')
    plt.plot(dotted[:il,0], dotted[:il,3]/dotted[:il,1], linewidth=3, label='Dotted', c='g', marker='o')
    plt.xlabel('Time (s)')
    plt.ylabel('Ops/Sec')
    plt.legend(loc='center right')
    plt.xlim(xmin=-2.0)
    # plt.xlim(xmax=(DE-DS)*5)
    plt.ylim(ymin=-10)
    # save in PDF
    pp = PdfPages(current_dotted_dir + '/ops_sec.pdf')
    pp.savefig()
    pp.close()


def latencies_plot(name):
    bname = current_basic_dir +'/basho_bench/'+name+'_latencies.csv'
    if os.path.isfile(bname):
        basic = np.loadtxt(bname, delimiter=',', skiprows=1)
    else:
        return -1

    dname = current_dotted_dir +'/basho_bench/'+name+'_latencies.csv'
    if os.path.isfile(dname):
        dotted = np.loadtxt(dname, delimiter=',', skiprows=1)
    else:
        return -1

    plt.style.use('fivethirtyeight')
    fig = plt.figure()
    fig.add_axes([0.15, 0.10, 0.8, 0.8])
    plt.title(name+" Latencies")

    ill = -1 #ignore last n lines
    ifl = 1 #ignore first n lines
    lw = 1
    ## elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors
    plt.plot(basic[ifl:ill,0], basic[ifl:ill,4]/1000.0, linewidth=lw, label='Basic Mean', color='r', marker='o',linestyle='-')
    plt.plot(basic[ifl:ill,0], basic[ifl:ill,6]/1000.0, linewidth=lw, label='Basic 95p', color='r', marker='s',linestyle='-')
    # plt.plot(basic[ifl:ill,0], basic[ifl:ill,9], linewidth=lw, label='Basic Max', color='b', marker='.',linestyle='-')
    plt.plot(dotted[ifl:ill,0], dotted[ifl:ill,4]/1000.0, linewidth=lw, label='Dotted Mean', color='g', marker='o',linestyle=':')
    plt.plot(dotted[ifl:ill,0], dotted[ifl:ill,6]/1000.0, linewidth=lw, label='Dotted 95p', color='g', marker='s',linestyle=':')
    # plt.plot(dotted[ifl:ill,0], dotted[ifl:ill,9], linewidth=lw, label='Dotted Max', color='b', marker='.',linestyle=':')
    plt.xlabel('Time (s)')
    plt.ylabel('Latency (ms)')
    plt.legend(loc='upper left')
    plt.xlim(xmin=-2.0)
    # plt.xlim(xmax=(DE-DS)*5)
    # plt.ylim(ymin=-10)
    # save in PDF
    pp = PdfPages(current_dotted_dir + '/'+name+'_latencies.pdf')
    pp.savefig()
    pp.close()




columns_names = [
    (0,'elapsed'),
    (1,'window'),
    (2,'n'),
    (3,'min'),
    (4,'mean'),
    (5,'median'),
    (6,'95p'),
    (7,'99p'),
    (8,'99.9p'),
    (9,'max'),
    (10,'total'),
    (11,'std_dev')]


def mean_matrix(d):
    return np.array([ (([xVal]+(np.mean([row[1:] for row in d if xVal==row[0]],axis=0)).tolist() )) for xVal in np.unique(d[:,0])])

def join_matrix(a,b):
    d = np.concatenate([a,b],axis=0)
    return np.array([ (([xVal]+(np.concatenate([row[1:] for row in d if xVal==row[0]],axis=1)).tolist() )) for xVal in np.unique(d[:,0])])

def filter_zero_n(m):
    return np.array(filter(lambda x:x[2] != 0, m))

def read_csv(name, do_filter, skip=4):
    if do_filter == True:
        return filter_zero_n( np.loadtxt( name,  delimiter=',', skiprows=skip))
    else:
        return np.loadtxt( name,  delimiter=',', skiprows=skip)

def load_local_basic_csv(name, do_filter=True):
    csv1 = read_csv((current_basic_dir +'/dev1/'+ name), do_filter)
    csv2 = read_csv((current_basic_dir +'/dev2/'+ name), do_filter)
    csv3 = read_csv((current_basic_dir +'/dev3/'+ name), do_filter)
    csv4 = read_csv((current_basic_dir +'/dev4/'+ name), do_filter)
    return np.concatenate([csv1,csv2,csv3,csv4], axis=0)

def load_local_dotted_csv(name, do_filter=True):
    csv1 = read_csv((current_dotted_dir +'/dev1/'+ name), do_filter)
    csv2 = read_csv((current_dotted_dir +'/dev2/'+ name), do_filter)
    csv3 = read_csv((current_dotted_dir +'/dev3/'+ name), do_filter)
    csv4 = read_csv((current_dotted_dir +'/dev4/'+ name), do_filter)
    return np.concatenate([csv1,csv2,csv3,csv4], axis=0)

def load_cluster_basic_csv(name, do_filter=True, skip=4):
    csv1 = read_csv((current_basic_dir +'/node1/'+ name), do_filter, skip)
    csv2 = read_csv((current_basic_dir +'/node2/'+ name), do_filter, skip)
    csv3 = read_csv((current_basic_dir +'/node3/'+ name), do_filter, skip)
    csv4 = read_csv((current_basic_dir +'/node4/'+ name), do_filter, skip)
    csv5 = read_csv((current_basic_dir +'/node5/'+ name), do_filter, skip)
    return np.concatenate([csv1,csv2,csv3,csv4,csv5], axis=0)

def load_cluster_dotted_csv(name, do_filter=True, skip=4):
    csv1 = read_csv((current_dotted_dir +'/node1/'+ name), do_filter, skip)
    csv2 = read_csv((current_dotted_dir +'/node2/'+ name), do_filter, skip)
    csv3 = read_csv((current_dotted_dir +'/node3/'+ name), do_filter, skip)
    csv4 = read_csv((current_dotted_dir +'/node4/'+ name), do_filter, skip)
    csv5 = read_csv((current_dotted_dir +'/node5/'+ name), do_filter, skip)
    return np.concatenate([csv1,csv2,csv3,csv4,csv5], axis=0)

def load_dstat_csv(path):
    # csv1 = pd.read_csv((path +'/node1/dstat.csv'), parse_dates=[0], header=7)

    csv1 = (pd.read_csv((path +'/node1/dstat.csv'), parse_dates=[0], header=7)).as_matrix()
    csv2 = (pd.read_csv((path +'/node2/dstat.csv'), parse_dates=[0], header=7)).as_matrix()
    csv3 = (pd.read_csv((path +'/node3/dstat.csv'), parse_dates=[0], header=7)).as_matrix()
    csv4 = (pd.read_csv((path +'/node4/dstat.csv'), parse_dates=[0], header=7)).as_matrix()
    csv5 = (pd.read_csv((path +'/node5/dstat.csv'), parse_dates=[0], header=7)).as_matrix()
    return np.concatenate([csv1,csv2,csv3,csv4,csv5], axis=0)

################################
## MAIN
################################

def main(argv):


    #print 'Number of arguments:', len(sys.argv), 'arguments.'
    #print 'Argument List:', str(sys.argv)
    arg1 = ""
    if len(sys.argv)>1:
        arg1 = sys.argv[1]
    print "EXECUTE " + arg1
    if arg1 == 'cluster_dotted' or arg1 == 'cluster_basic':
        create_folder(arg1)
        get_cluster_files(arg1)
        # get_cluster_bb()
        # do_bashobench()
        get_ycsb(arg1)
    elif arg1 == 'local_dotted' or arg1 == 'local_basic':
        create_folder(arg1)
        get_local_files(arg1)
        get_local_bb()
        do_bashobench()
    elif arg1 == 'cluster_plot':
        do_plot('cluster')
    elif arg1 == 'local_plot':
        do_plot('local')
    elif arg1 == 'bb':
        plot_bb()
    elif arg1 == 'ycsb':
        plot_ycsb()
    elif arg1 == 'current':
        if len(sys.argv) == 3:
            fol = sys.argv[2]
            print "Changing \'current\' to " + fol
            change_current(fol)
        else:
            print "Missing name of the new \'current\' folder."
    elif arg1 == 'entries':
        clock_entries_paper()
    elif arg1 == 'deletes':
        deletes_paper()
    elif arg1 == 'sync':
        sync_paper()
    elif arg1 == 'strip':
        strip_paper()
    else:
        print "No args :("


if __name__ == "__main__":
    main(sys.argv[1:])


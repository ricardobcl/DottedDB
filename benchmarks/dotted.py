#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import pysftp
from datetime import datetime
import os
import sys
from subprocess import call, check_call
import subprocess
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm # recommended import according to the docs
from os import walk
from matplotlib.backends.backend_pdf import PdfPages

plt.style.use('fivethirtyeight')

################################################################################
#### VARIABLES
################################################################################

bench_ip                = '192.168.112.37'
cluster_ip              = '192.168.112.'
machines                = ['38', '39', '40', '55', '56']
cluster_user            = 'gsd'
cluster_private_key     = '/Users/ricardo/.ssh/gsd_private_key'
remote_bb_path          = '/home/gsd/basho_bench/tests/current/'
dotted_path             = '/home/gsd/DottedDB/_build/default/rel/dotted_db/data/stats/current/'
basic_path              = '/home/gsd/BasicDB/_build/default/rel/basic_db/data/stats/current/'
bb_summary_path         = '/Users/ricardo/github/DottedDB/benchmarks/priv/summary.r'
local_path              = '/Users/ricardo/github/DottedDB/benchmarks/tests/'
current_dir             = local_path + 'current/'



""" Create a new folder for the incoming files.
Also, make the folder current a symlink to the new folder.
"""
def create_folder(type=""):
    if type == 'dotted' or type == 'basic':
        new = local_path + type + '/' + get_folder_time() + '/'
        os.makedirs(new)
        print "New directory: " + new
        change_current(new)
    else:
        print "Error creating dir: " + type + "!"
        sys.exit(1)

def change_current(f):
    if not os.path.exists(f):
        print "Folder " + f + " does not exist!"
        sys.exit(1)
    else:
        call(["rm","-rf", current_dir])
        call(["ln","-s", f, current_dir])
        print "\'current\' now points to: " + f

def get_folder_time():
    now = datetime.now()
    return '%4d%02d%02d_%02d%02d%02d' % (now.year, now.month, now.day, now.hour, now.minute, now.second)



""" Get stat files from the machines in the cluster
"""
def get_files(type):
    print "Getting files from remote cluster"
    ori = dotted_path
    dest = current_dir
    if type != 'dotted' and type != 'vc':
        print "ERROR: get_files error in path"
        sys.exit(1)
    i = 0
    for m in machines:
        i += 1
        machine = cluster_ip + m
        print "Getting stats files from: ", machine
        s = pysftp.Connection(host=machine,username=cluster_user,private_key=cluster_private_key)
        os.makedirs(dest + "node%s/"%i)
        s.get_d(ori, dest + "node%s/"%i)
        s.close()
    print "Getting basho bench files from: ", machine
    s = pysftp.Connection(host=bench_ip,username=cluster_user,private_key=cluster_private_key)
    os.makedirs(dest + "basho_bench")
    s.get_d(remote_bb_path, dest + "basho_bench")
    s.close()


def do_bashobench(f=''):
    if f == '':
        call(["Rscript","--vanilla",bb_summary_path,"-i",current_dir+"basho_bench"])
    else:
        call(["Rscript","--vanilla",bb_summary_path,"-i",f+"/basho_bench"])








###############################################
### Plotting
###############################################


def filter_zero_n(m):
    return np.array(filter(lambda x:x[2] != 0, m))

def read_csv(name):
    return filter_zero_n( np.loadtxt( name,  delimiter=',', skiprows=1))

def load_local_histogram(name):
        print "Getting stats files from: ", machine
        s = pysftp.Connection(host=machine,username=cluster_user,private_key=cluster_private_key)
        os.makedirs(dest + "/node%s/"%i)
        s.get_d(ori, dest + "/node%s/"%i)
        s.close()
    print "Getting basho bench files from: ", machine
    s = pysftp.Connection(host=bench_ip,username=cluster_user,private_key=cluster_private_key)
    os.makedirs(dest + "/basho_bench")
    s.get_d(cluster_bb_path, dest + "/basho_bench")
    s.close()


def do_bashobench(f=''):
    if f == '':
        call(["Rscript","--vanilla",bb_summary_path,"-i",current_dir+"/basho_bench"])
    else:
        call(["Rscript","--vanilla",bb_summary_path,"-i",f+"/basho_bench"])








###############################################
### Plotting
###############################################


def filter_zero_n(m):
    return np.array(filter(lambda x:x[2] != 0, m))

def read_csv(name):
    return filter_zero_n( np.loadtxt( name,  delimiter=',', skiprows=1))

def load_local_histogram(name):
    csv1 = read_csv(('/Users/ricardo/github/DottedDB/dev/dev1/data/stats/current/' + name + '_hist.csv'))
    csv2 = read_csv(('/Users/ricardo/github/DottedDB/dev/dev2/data/stats/current/' + name + '_hist.csv'))
    csv3 = read_csv(('/Users/ricardo/github/DottedDB/dev/dev3/data/stats/current/' + name + '_hist.csv'))
    csv4 = read_csv(('/Users/ricardo/github/DottedDB/dev/dev4/data/stats/current/' + name + '_hist.csv'))
    result = filter(lambda x: x.size != 0, [csv1,csv2,csv3,csv4])
    return np.concatenate(result, axis=0)

def movingaverage(interval, window_size):
    window= np.ones(int(window_size))/float(window_size)
    return np.convolve(interval, window, 'same')

def do_local_plot():
    print "Loading local stats."
    bvv                 = load_local_histogram("bvv-size")
    kl                  = load_local_histogram("kl-len")
    sync_local_str      = load_local_histogram("sync-local-dcc-strip")
    sync_sent_str       = load_local_histogram("sync-sent-dcc-strip")
    sync_relevant       = load_local_histogram("sync-relevant-ratio")
    sync_hit            = load_local_histogram("sync-hit-ratio")
    sync_payload_size   = load_local_histogram("sync-payload-size")
    sync_metadata_size  = load_local_histogram("sync-metadata-size")

    print "Plotting Hit Ratio."
    plt.figure(1)
    plt.subplot(221)
    plt.title("Hit Ratio")
    plt.ylim((-2,102))
    # plt.xlim((-2,250))
    
    plt.plot(sync_hit[:,0], sync_hit[:,4], "k.",    label='b_mean')
    sync_hit_av = movingaverage(sync_hit[:,4], 10)
    plt.plot(sync_hit[:,0], sync_hit_av, "r", linewidth=2)
    plt.grid(True)
    
    plt.legend()
    plt.xlabel('Time')
    plt.ylabel('%')
    
    print "Plotting Relevant Keys Ratio."
    plt.subplot(223)
    plt.title("Relevant Keys Ratio")
    plt.ylim((-2,102))
    # plt.xlim((-2,250))
    plt.plot(sync_relevant[:,0], sync_relevant[:,5],      label='k_median', linewidth=3 ) # , marker = 'o')
    plt.plot(sync_relevant[:,0], sync_relevant[:,4],      label='k_mean', linewidth=3   ) # , marker = 'o')
    plt.plot(sync_relevant[:,0], sync_relevant[:,6],      label='k_95', linewidth=3     ) # , marker = 'o')
    # plt.plot(sync_relevant[:,0], sync_relevant[:,9],      label='k_max', linewidth=3    ) # , marker = 'o')
    plt.legend()
    plt.xlabel('Time')
    plt.ylabel('%')
    
    
    # plt.figure(2)
    print "Plotting Metadata Size on Syncs."
    plt.subplot(222)
    plt.title("Metadata Size on Syncs")
    # plt.ylim((-2,102))
    # plt.xlim((-2,250))
    plt.plot(sync_metadata_size[:,0], sync_metadata_size[:,5],      label='k_median', linewidth=3 ) # , marker = 'o')
    plt.plot(sync_metadata_size[:,0], sync_metadata_size[:,4],      label='k_mean', linewidth=3   ) # , marker = 'o')
    plt.plot(sync_metadata_size[:,0], sync_metadata_size[:,6],      label='k_95', linewidth=3     ) # , marker = 'o')
    plt.plot(sync_metadata_size[:,0], sync_metadata_size[:,7],      label='k_99', linewidth=3     ) # , marker = 'o')
    # plt.plot(sync_metadata_size[:,0], sync_metadata_size[:,9],      label='k_max', linewidth=3    ) # , marker = 'o')
    plt.legend()
    plt.xlabel('Time')
    plt.ylabel('Size (Bytes')
    

    print "Plotting Size on Syncs."
    plt.subplot(224)
    plt.title("Payload Size on Syncs")
    # plt.ylim((-2,102))
    # plt.xlim((-2,250))
    plt.plot(sync_payload_size[:,0], sync_payload_size[:,5],      label='k_median', linewidth=3 ) # , marker = 'o')
    plt.plot(sync_payload_size[:,0], sync_payload_size[:,4],      label='k_mean', linewidth=3   ) # , marker = 'o')
    plt.plot(sync_payload_size[:,0], sync_payload_size[:,6],      label='k_95', linewidth=3     ) # , marker = 'o')
    plt.plot(sync_payload_size[:,0], sync_payload_size[:,7],      label='k_99', linewidth=3     ) # , marker = 'o')
    # plt.plot(sync_payload_size[:,0], sync_payload_size[:,9],      label='k_max', linewidth=3    ) # , marker = 'o')
    plt.legend()
    plt.xlabel('Time')
    plt.ylabel('Size (Bytes')
    
    print "Showing."
    plt.show()









def filter_zero_n(m):
    return np.array(filter(lambda x:x[2] != 0, m))

def read_csv(name, do_filter):
    if do_filter == True:
        return filter_zero_n( np.loadtxt( name,  delimiter=',', skiprows=1))
    else:
        return np.loadtxt( name,  delimiter=',', skiprows=1)

def load_csv(name, do_filter=True):
    csv1 = read_csv(('/Users/ricardo/github/DottedDB/benchmarks/tests/current/node1' + name), do_filter)
    csv2 = read_csv(('/Users/ricardo/github/DottedDB/benchmarks/tests/current/node2' + name), do_filter)
    csv3 = read_csv(('/Users/ricardo/github/DottedDB/benchmarks/tests/current/node3' + name), do_filter)
    csv4 = read_csv(('/Users/ricardo/github/DottedDB/benchmarks/tests/current/node4' + name), do_filter)
    csv5 = read_csv(('/Users/ricardo/github/DottedDB/benchmarks/tests/current/node5' + name), do_filter)
    return np.concatenate([csv1,csv2,csv3,csv4,csv5], axis=0)

def get_csv_names():
    f = []
    mypath='/Users/ricardo/github/DottedDB/benchmarks/tests/current/node1/'
    for (dirpath, dirnames, filenames) in walk(mypath):
        f.extend(filenames)
        break
    return f

def draw(stats, col):
# # plt.style.use('ggplot')
    plt.style.use('fivethirtyeight')
    for s in stats:
        name = os.path.splitext(s)[0][:-5]
        type_stat = os.path.splitext(s)[0][-5:]
        print("Plotting: " + name + " type: " + type_stat)
        if type_stat == "_hist":
            f = load_csv(s)
            plt.figure()
            plt.title(name.replace("-"," ").title())
            i = 0
            for c in col:
                # plt.plot(f[:,0], f[:,c[0]], label=c[1], linewidth=3)
                plt.scatter(f[:,0], f[:,c[0]], s=(f[:,2]*2), label=c[1], c=markers[i][1], marker=markers[i][0])
                i = i+1
            plt.xlabel('Time')
            # plt.ylabel('Size')
            plt.legend()
            plt.ylim(ymin=-2)
            # plt.ylim((-2,150))
            # plt.xlim((0,700))
            # save in PDF
            pp = PdfPages(dest_path + name + '.pdf')
            pp.savefig()
            pp.close()
        elif type_stat == "gauge":
            f0 = load_csv(s, False)
            plt.figure()
            plt.title(name.replace("-"," ").title())

    markers = [('o','g'),('x','r'),('+','b')]
    files = get_csv_names()
    draw(files, [columns[4], columns[9]])
    # draw([files[0]], [columns[4], columns[9]])
    return 0


###############################################
### Main
###############################################


def main(argv):
    #print 'Number of arguments:', len(sys.argv), 'arguments.'
    #print 'Argument List:', str(sys.argv)
    arg1 = ""
    if len(sys.argv)>1:
        arg1 = sys.argv[1]
    print "EXECUTE " + arg1
    if arg1 == 'dotted' or arg1 == 'basic':
        create_folder(arg1)
        get_files(arg1)
        do_bashobench()
    elif arg1 == 'cluster_dotted':
        do_cluster_plotting('/Users/ricardo/github/DottedDB/benchmarks/tests/cluster/')
    elif arg1 == 'bb':
        do_bashobench()
    elif arg1 == 'local_plot':
        do_local_plot()
    elif arg1 == 'current':
        if len(sys.argv) == 3:
            fol = sys.argv[2]
            print "Changing \'current\' to " + fol
            change_current(fol)
        else:
            print "Missing name of the new \'current\' folder."
    else:
        print "No args :("


if __name__ == "__main__":
    main(sys.argv[1:])



import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm # recommended import according to the docs
from os import walk
import os
from matplotlib.backends.backend_pdf import PdfPages


def filter_zero_n(m):
    return np.array(filter(lambda x:x[2] != 0, m))

def read_csv(name, do_filter):
    if do_filter == True:
        return filter_zero_n( np.loadtxt( name,  delimiter=',', skiprows=1))
    else:
        return np.loadtxt( name,  delimiter=',', skiprows=1)

def load_csv(name, do_filter=True):
    csv1 = read_csv(('/Users/ricardo/github/DottedDB/_build/dev/dev1/dotted_db/data/stats/current/' + name), do_filter)
    csv2 = read_csv(('/Users/ricardo/github/DottedDB/_build/dev/dev2/dotted_db/data/stats/current/' + name), do_filter)
    csv3 = read_csv(('/Users/ricardo/github/DottedDB/_build/dev/dev3/dotted_db/data/stats/current/' + name), do_filter)
    csv4 = read_csv(('/Users/ricardo/github/DottedDB/_build/dev/dev4/dotted_db/data/stats/current/' + name), do_filter)
    return np.concatenate([csv1,csv2,csv3,csv4], axis=0)

def get_csv_names():
    f = []
    mypath='/Users/ricardo/github/DottedDB/_build/dev/dev1/dotted_db/data/stats/current/'
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

            f = np.array(map(lambda x: x/1000, f0))
            ecdf = sm.distributions.ECDF(f)
            x = np.linspace(min(f), max(f))
            y = ecdf(x)
            plt.step(x, y, label="Strip")

            plt.ylabel('Percentage')
            plt.xlabel('Seconds') 
            plt.legend()
            plt.ylim(ymin=0)
            plt.ylim(ymax=1)
            # save in PDF
            pp = PdfPages(dest_path + name + 'cdf.pdf')
            pp.savefig()
            pp.close()
        else:
            print "unknown type: " + type_stat
        print("Done.")
    return 0



## Do the plotting

dest_path = '/Users/ricardo/github/DottedDB/benchmarks/tests/local/'
if not os.path.exists(dest_path):
    os.makedirs(dest_path)

columns = [ (0,'elapsed'),
            (1,'window'),
            (2,'n'),
            (3,'min'),
            (4,'mean'),
            (5,'median'),
            (6,'95p'),
            (7,'99p'),
            (8,'99.9p'),
            (9,'max')]

markers = [('o','g'),('x','r'),('+','b')]

files = get_csv_names()
draw(files, [columns[4], columns[9]])
# draw([files[0]], [columns[4], columns[9]])


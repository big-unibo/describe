#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
import math
import matplotlib.font_manager as font_manager
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# ==============================================================================
# Chart variables
# ==============================================================================
# titlesize = 12
# subtitlesize = 10
# labelsize = 9
# axessize = 9
# legendsize = 9
markersize = 5

# http://scipy-cookbook.readthedocs.io/items/Matplotlib_LaTeX_Examples.html
plt.rcParams.update(plt.rcParamsDefault)
# print(plt.style.available)
# plt.style.use('seaborn') # tableau-colorblind10
# plt.style.use('Solarize_Light2')
# plt.style.use('grayscale')

# plt.rc('text', usetex=True)
plt.rc('font', family='serif', size=10)
plt.rcParams['mathtext.fontset'] = 'dejavuserif'
font = font_manager.FontProperties(family='serif', size=10)

# You typically want your plot to be ~1.33x wider than tall. This plot is a rare
# exception because of the number of lines being plotted on it.
# Common sizes: (10, 7.5) and (12, 9)
# Make room for the ridiculously large title.
# plt.subplots_adjust(top=0.8)
def figsize(cols):
    return (2.5 * cols, 2.5)
figsize11 = (2.5, 2.5)
figsize12 = (5, 2.5)
figsize13 = (7.5, 2.5)
figsize14 = (10, 2.5)

# Markers - https://matplotlib.org/api/markers_api.html
markers = ["v", "^", "<", ">", "8", "s", "p", "P", "*", "+", "X", "D", "o", "s"]

# Lines - https://matplotlib.org/gallery/lines_bars_and_markers/line_styles_reference.html

# =============================================================================
# Location String	Location Code
# 'best'	0
# 'upper right'	1
# 'upper left'	2
# 'lower left'	3
# 'lower right'	4
# 'right'	5
# 'center left'	6
# 'center right'	7
# 'lower center'	8
# 'upper center'	9
# 'center'	10
# =============================================================================

sea = [(76, 114, 176), (196, 78, 82), (129, 114, 178), (204, 185, 116), (100, 181, 205), (76, 114, 176), (85, 168, 104)]
# =============================================================================
# These are the "Tableau 20" colors as RGB.
# http://www.randalolson.com/2014/06/28/how-to-make-beautiful-data-visualizations-in-python-with-matplotlib/
# =============================================================================
tableau20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]
# Scale the RGB values to the [0, 1] range, which is the format matplotlib accepts.
for i in range(len(tableau20)):
    r, g, b = tableau20[i]
    tableau20[i] = (r / 255., g / 255., b / 255.)
for i in range(len(sea)):
    r, g, b = sea[i]
    sea[i] = (r / 255., g / 255., b / 255.)

col_width = 0.7


def myticks(x):
    if x == 0 or int(np.log10(x)) == 0: return str(x)
    exponent = int(np.log10(x))
    coeff = float(x) / 10.0 ** exponent
    return r"${:2.1f} \cdot 10^{{{:2d}}}$".format(coeff, exponent)


def getIntention(x):
    if x == "past":
        return "Past"
    elif x == "sibling":
        return "Sibling"
    elif x == "target":
        return "Constant"
    elif x == "external":
        return "External"
    elif x % 10 == 3:
        return "Past"
    elif x % 10 == 1:
        return "Constant"
    elif x % 10 == 2:
        return "Sibling"
    else:
        return x

def getExecutionPlan(x):
    return x[1:]


def replacePlan(X):
    # X["plan"] = X["plan"].replace("JOININMEMORY", "1PLAIN").replace("JOININDBMS", "2JBO").replace("PIVOTMV", "3PBO").replace("PIVOT", "4POG-OLD")
    X["plan"] = X["plan"].replace("JOININMEMORY", "1NP").replace("JOININDBMS", "2JOP").replace("PIVOTMV", "3POP").replace("PIVOT", "4POG-OLD")


def replaceCard(X):
    # sibling
    X.loc[X["cardinality_extcube"] == 1, 'cardinality'] = 1

    X.loc[X["cardinality_extcube"] == 81,       'cardinality'] = 82
    X.loc[X["cardinality_extcube"] == 96633,    'cardinality'] = 125299
    X.loc[X["cardinality_extcube"] == 58,       'cardinality'] = 245039

    X.loc[X["cardinality_extcube"] == 82,       'cardinality'] = 82
    X.loc[X["cardinality_extcube"] == 970871,   'cardinality'] = 1241986
    X.loc[X["cardinality_extcube"] == 462,      'cardinality'] = 2396552

    X.loc[X["cardinality_extcube"] == 18301,    'cardinality'] = 23775
    X.loc[X["cardinality_extcube"] == 193095,   'cardinality'] = 249475
    X.loc[X["cardinality_extcube"] == 1924865,  'cardinality'] = 2479958

    # past
    X.loc[X["cardinality_extcube"] == 1,        'cardinality'] = 1

    X.loc[X["cardinality_extcube"] == 25,       'cardinality'] = 25
    X.loc[X["cardinality_extcube"] == 37433,    'cardinality'] = 39296
    X.loc[X["cardinality_extcube"] == 35,       'cardinality'] = 76838

    X.loc[X["cardinality_extcube"] == 25,       'cardinality'] = 25
    X.loc[X["cardinality_extcube"] == 285,      'cardinality'] = 770182
    X.loc[X["cardinality_extcube"] == 374070,   'cardinality'] = 392920

    X.loc[X["cardinality_extcube"] == 1413,     'cardinality'] = 1498
    X.loc[X["cardinality_extcube"] == 15054,    'cardinality'] = 15728
    X.loc[X["cardinality_extcube"] == 149721,   'cardinality'] = 157191
    return X

data = pd.read_csv('../../../resources/assess/time.csv', encoding="utf-8")
data = data.sort_values("plan", ascending=True)
ddata = replaceCard(data)
replacePlan(data)

for index in data["indexes"].unique():
    for dbms in ["oracle"]:
        for idx in ["target", "external", "sibling", "past"]:
            df = data
            df = df[df["dbms"] == dbms]
            df = df[df["benchmark_type"] == idx]
            df = df[df["indexes"] == index]
            c = 0
            
            plans = len(df['plan'].unique())
            fig, ax = plt.subplots(1, plans, figsize=figsize(plans))
            for key, group_df in df.groupby(['plan'], sort=True):
                axx = ax[c] if plans > 1 else ax

                group_dff = group_df.groupby(['cardinality'])
                k = list(group_dff.groups.keys())
                ind = np.arange(len(k))

                meanval = group_dff["time_cube"].median() / 1000.0  # .median()
                sumval = meanval
                axx.bar(ind - (2 * col_width / 6), meanval, col_width / 6, label="Get $C$", color=sea[0])

                minval = meanval
                meanval = group_dff["time_benchmark"].median() / 1000.0  # .median()
                sumval += meanval
                axx.bar(ind - (1 * col_width / 6), meanval, col_width / 6, label="Get $B$", color=sea[6])

                if idx != "target" and key != "1NP":
                    minval += meanval
                    meanval = group_dff["time_cube"].median() / 1000.0 # .median()
                    sumval += meanval
                    axx.bar(ind - (2 * col_width / 6), meanval, col_width / 6, label="Get $C+B$", color=sea[1])
                else:
                    axx.bar(ind - (2 * col_width / 6), 0, col_width / 6, label="Get $C+B$", color=sea[1])

                minval += meanval
                meanval = group_dff["time_transform"].median() / 1000.0  # .median()
                sumval += meanval
                axx.bar(ind - (0 * col_width / 6), meanval, col_width / 6, label="Trans.", color=sea[2])

                minval += meanval
                meanval = group_dff["time_join"].median() / 1000.0  # .median()
                sumval += meanval
                axx.bar(ind + (1 * col_width / 6), meanval, col_width / 6, label="Join", color=sea[3])

                minval += meanval
                meanval = group_dff["time_comparison"].median() / 1000.0  # .median()
                sumval += meanval
                axx.bar(ind + (2 * col_width / 6), meanval, col_width / 6, label="Comp.", color=sea[4])

                minval += meanval
                meanval = group_dff["time_labeling"].median() / 1000.0  # .median()
                sumval += meanval
                axx.bar(ind + (3 * col_width / 6), meanval, col_width / 6, label="Label", color=sea[5])

                # axs2[c % 2].xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(myticks))
                axx.set_xticks(ind)
                axx.set_xticklabels(["$SSB_1$", "$SSB_{10}$", "$SSB_{100}$"]) 
                # axx.set_xticklabels([myticks(x) for x in k])  # , fontsize=axessize
                # axx.set_xlabel("$|C|$")  # , fontsize=labelsize
                axx.set_ylabel("Time (s)")  # , fontsize=labelsize
                axx.yaxis.set_tick_params(labelbottom=True)
                axx.set_yscale('log')
                axx.set_axisbelow(True)
                axx.grid(axis='y')
                maxp = math.ceil(math.log(sumval.max(), 10))
                axx.set_yticks([math.pow(10, x) for x in range(-3, maxp + 1)])
                axx.set_ylim(ymin=0.001, ymax=math.pow(10, maxp) + 1)
                axx.tick_params(axis='both', which='major')  # , labelsize=axessize
                axx.set_title(getExecutionPlan(key))  # , fontsize=subtitlesize)
                # for tick in axx.get_xticklabels():
                #     tick.set_rotation(15)
                if c == 0:
                    handles, labels = axx.get_legend_handles_labels()
                    fig.legend(handles, labels, loc='lower left', ncol=7, mode="expand", bbox_to_anchor=(0, -0.02, 1, 0.2), columnspacing=0.2, labelspacing=0.2, frameon=True)  # , fontsize=legendsize)
                c += 1

            fig.tight_layout(rect=(0, 0.05, 1, 1))
            fig.savefig("../../../resources/assess/charts/efficiency" + "_" + idx + "_" + index + "_" + dbms + ".pdf")
            fig.suptitle(idx + " - " + index)

    for dbms in ["oracle"]:
        fig2, axs = plt.subplots(1, 4, figsize=figsize14)  # , sharey="all", constrained_layout=True
        outer = 0
        for idx in ["target", "external", "sibling", "past"]:
            df = data
            df = df[df["dbms"] == dbms]
            df = df[df["benchmark_type"] == idx]
            df = df[df["indexes"] == index]

            c = 0
            startidx = 0 if idx == "target" else 1    
            for key, group_df in df.groupby(['plan']):  # , "benchmark"
                group_dff = group_df.groupby(['cardinality'])
                k = list(group_dff.groups.keys())
                ind = np.arange(len(k))
                axx = axs[outer]


                meanval = group_dff["time_cube"].median() / 1000.0  # .median()
                sumval = meanval
                
                minval = meanval
                meanval = group_dff["time_benchmark"].median() / 1000.0  # .median()
                sumval += meanval

                minval += meanval
                meanval = group_dff["time_transform"].median() / 1000.0  # .median()
                sumval += meanval

                minval += meanval
                meanval = group_dff["time_join"].median() / 1000.0  # .median()
                sumval += meanval

                minval += meanval
                meanval = group_dff["time_comparison"].median() / 1000.0  # .median()
                sumval += meanval

                minval += meanval
                meanval = group_dff["time_labeling"].median() / 1000.0  # .median()
                sumval += meanval
                
                print("{} {} time".format(idx, key))
                print(sumval)
                axx.bar(ind + ((c - startidx) * col_width / 3), sumval, col_width / 3, label=getExecutionPlan(key), color=sea[c])  # bottom=minval

                axx.set_xticks(ind)
                axx.set_xticklabels(["$SSB_1$", "$SSB_{10}$", "$SSB_{100}$"])  # , fontsize=axessize
                # axx.set_xticklabels([myticks(x) for x in k])  # , fontsize=axessize
                # axx.set_xlabel("$|C|$")  # , fontsize=labelsize
                axx.set_ylabel("Time (s)")  # , fontsize=labelsize

                axx.yaxis.set_tick_params(labelbottom=True)
                axx.set_axisbelow(True)
                axx.grid(b=True, which='major', linestyle='-', axis='y')
                axx.set_yscale('log')
                # maxp = math.ceil(math.log(sumval.max(), 10))
                # axx.set_yticks([math.pow(10, x) for x in range(-1, maxp + 1)])
                # axx.set_ylim(ymin=0.1, ymax=math.pow(10, maxp) + 1)
                axx.tick_params(axis='both', which='major')  # , labelsize=axessize
                axx.set_title(getIntention(idx))
                if outer == 3:
                    axx.legend(columnspacing=0.2, labelspacing=0.2, frameon=True)  # , fontsize=legendsize)
                # for tick in axx.get_xticklabels():
                #     tick.set_rotation(15)
                c += 1
            outer += 1
        fig2.tight_layout()
        fig2.savefig("../../../resources/assess/charts/efficiency_overall_" + index + "_" + dbms + ".pdf")
        fig2.suptitle(index)
plt.show()

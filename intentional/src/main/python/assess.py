import argparse
import json
import math
import os
import numpy as np
import pandas as pd
import time
from datetime import datetime
from scipy.stats import zscore
from sklearn.linear_model import LinearRegression
import sys

###############################################################################
# FUNCTIONS
###############################################################################
def difference(a, b):
    return a - b

def minmaxnorm(a):
    minv = a.min()
    maxv = a.max()
    return (a - minv) / (maxv - minv)

def zscore(a):
    return a.apply(zscore)

def likert(a):
    return pd.cut(a, [-1, 0.2, 0.4, 0.6, 0.8, 1.0], include_lowest = True, labels=["*", "**", "***", "****", "*****"])

def decile(a):
    return percentile(a, 10)

def quartile(a):
    return percentile(a, 4)

def percentile(a, q=100):
    q = min(len(a.unique()), q)
    return pd.qcut(a, q=q, labels=[str(x + 1) for x in range(q)])

def ratio(a, b):
    return a / b

# lookup table for the implemented functions, new functions MUST be added here
functions = {
    'difference' : difference,
    'minmaxnorm' : minmaxnorm,
    'zscore' : zscore,
    '5star': likert,
    'percentile': percentile,
    'deciles': decile,
    'quartiles': quartile,
    'ratio': ratio
}

# #######################################################################################################
# Evaluate a tree of nested functions. Intermediate values are stored as new columns in the extended cube
def evaluate(X, fun, params):
    """
        X: extended cube
        fun: function id
        params: list of parameters
        return the label of the last function
    """
    par_n = 0 # id of the nested function
    def evaluate_1(X, fun, params):
        nonlocal par_n
        par_n += 1 # increase the id of the nested function
        def rec(p): # recursive function
            if isinstance(p, dict) and "fun" in p: # if the parameter is a nested function, do the recursive function
                return X[evaluate_1(X, p["fun"], p["params"])]
            else: # if the parameter is a float or a string
                try:
                    X[str(p)] = float(p) # if its a float, I need to create a new column
                    return X[str(p)]
                except (ValueError, TypeError): # otherwise, refer to an existing column using the function name
                    return X[p]
        col = functions[fun](*[rec(p) for p in params]) # run the function
        key = fun + "_" + str(par_n)
        X[key] = col # store the result
        return key # return the key
    return evaluate_1(X, fun, params) # return the last key

toprint = {
    "time_benchmark": 1,
    "time_transform": 1,
    "time_join":  1,
    "time_comparison":  1,
    "time_labeling":  1,
    "cardinality": 0,
    "cardinality_benchmark": 0,
}

def compute_benchmark_pivot(path, file, session_step, measure, benchmark_type, benchmark):
    Y = pd.read_csv(path + file + "_" + str(session_step) + ".csv", encoding="utf-8")
    Y.columns = [x.lower().replace("bc_", "benchmark.") for x in Y.columns]
    toprint["cardinality_benchmark"] = len(Y.index)
    if benchmark_type == "past":
        start_time = datetime.now()
        gc = sorted([x for x in Y.columns if "benchmark." in x])
        def regression(X):
            model = LinearRegression()
            model.fit([[int(x.replace("benchmark.", ""))] for x in gc if not math.isnan(X[x])], [X[x] for x in gc if not math.isnan(X[x])])
            return model.predict([[int(benchmark) + 1]])[0] # put the dates of which you want to predict kwh here
        Y["benchmark." + measure] = Y.apply(lambda x: regression(x), axis=1)
        Y = Y.drop(columns=[x for x in Y.columns if "level_" in x])
        elapsed = datetime.now() - start_time
        toprint["time_transform"] = elapsed.seconds * 1000 + int(elapsed.microseconds / 1000)
    if Y.empty:
        raise Exception('Empty benchmark cube')
    return Y

def compute_benchmark_joinindbms(path, file, session_step, measure, benchmark_type, cube):
    Y = pd.read_csv(path + file + "_" + str(session_step) + ".csv", encoding="utf-8")
    Y.columns = [x.lower().replace("bc_", "benchmark.") for x in Y.columns]
    if benchmark_type == "past":
        sc = [x for x in cube["SC"] if x["SLICE"] and x["SLICE"]][0]
        attr, val = sc["ATTR"], sc["VAL"][0].replace("'", "")
        # set the date format
        date_format = "%Y-%m" if "month" in attr else "%Y" if "year" in attr else "%Y-%m-%d"
        # cast the slice value to datetime
        slice = datetime.strptime(val, date_format)
        # cast all dates to datetime format
        Y["benchmark." + attr] = Y["benchmark." + attr].apply(lambda x: time.mktime(datetime.strptime(x, date_format).timetuple()))
        start_time = datetime.now()
        def regression(X):
            model = LinearRegression()
            model.fit(X["benchmark." + attr].values.reshape(-1, 1), X["benchmark." + measure].values)
            return pd.DataFrame(model.predict([[time.mktime(slice.timetuple())]]), columns=["benchmark." + measure]) # put the dates of which you want to predict kwh here
        gc = [x for x in Y.columns if "benchmark." not in x]
        group_dff = Y.groupby(gc if len(gc) > 0 else lambda x: True)
        Y = group_dff.apply(lambda X: regression(X)).reset_index()
        Y = Y.drop(columns=[x for x in Y.columns if "level_" in x])
        elapsed = datetime.now() - start_time
        toprint["time_transform"] = elapsed.seconds * 1000 + int(elapsed.microseconds / 1000)
    return Y

def compute_benchmark_joininmemory(path, file, session_step, X, measure, benchmark_type, benchmark, cube):
    Y = pd.read_csv(path + file + "_bc_" + str(session_step) + ".csv", encoding="utf-8")
    Y.columns = ["benchmark." + x.lower() for x in Y.columns]
    if Y.empty:
        raise Exception('Empty benchmark cube')
    toprint["cardinality_benchmark"] = len(Y.index)
    join = []

    start_time = datetime.now()
    if benchmark_type == "target":
        return X
    elif benchmark_type == "sibling":
        attr, op, val = benchmark[1:-1].split(",") # remove ( ) wrapping the triple (product,=,'FANTA')
        # join all the attributes that are not measure and that are not the sibling
        join = [x for x in cube["GC"] if x != attr]
    elif benchmark_type == "past":
        # get the temporal slice
        sc = [x for x in cube["SC"] if x["SLICE"] and x["SLICE"]][0]
        attr, val = sc["ATTR"], sc["VAL"][0].replace("'", "")
        # set the date format
        date_format = "%Y-%m" if "month" in attr else "%Y" if "year" in attr else "%Y-%m-%d"
        # cast the slice value to datetime
        slice = datetime.strptime(val, date_format)
        # cast all dates to datetime format
        Y["benchmark." + attr] = Y["benchmark." + attr].apply(lambda x: time.mktime(datetime.strptime(x, date_format).timetuple()))
        # group cells by all attributes but the temporal one
        gc = ["benchmark." + x for x in cube["GC"] if attr not in x]
        group_dff = Y.groupby(gc if len(gc) > 0 else lambda x: True)
        def regression(X):
            model = LinearRegression()
            model.fit(X["benchmark." + attr].values.reshape(-1, 1), X["benchmark." + measure].values)
            return pd.DataFrame(model.predict([[time.mktime(slice.timetuple())]]), columns=["benchmark." + measure]) # put the dates of which you want to predict kwh here
        Y = group_dff.apply(lambda X: regression(X)).reset_index()
        Y["benchmark." + attr] = val
        Y = Y.drop(columns=[x for x in Y.columns if "level_" in x])
        join = cube["GC"]
    elapsed = datetime.now() - start_time
    toprint["time_transform"] = elapsed.seconds * 1000 + int(elapsed.microseconds / 1000)

    start_time = datetime.now()
    if len(join) > 0:
        X = pd.merge(X, Y, left_on=join, right_on=["benchmark." + x for x in join])
    else: # cartesian product
        X["fake_key"] = "key"
        Y["fake_key"] = "key"
        X = pd.merge(X, Y, on=["fake_key"])
        X = X[[x for x in X.columns if "fake_key" not in x]]
    elapsed = datetime.now() - start_time
    toprint["time_join"] = elapsed.seconds * 1000 + int(elapsed.microseconds / 1000)

    return X

def assess(path, file, session_step, distance_function, benchmark_type, benchmark, measure, cube, labeling_schema, execution_plan):
    global toprint
    toprint["cardinality_benchmark"] = 0
    toprint["benchmark_type"] = benchmark_type
    toprint["benchmark"] = benchmark.replace(",", ";")
    X = pd.DataFrame()

    ###############################################################################
    # COMPUTE BENCHMARK
    ###############################################################################
    if benchmark_type.lower() == "target":
        X = pd.read_csv(path + file + "_" + str(session_step) + ".csv", encoding="utf-8")
        X.columns = [x.lower() for x in X.columns]
        toprint["cardinality"] = len(X.index)
    else:
        if execution_plan.upper() == "PIVOT" or execution_plan.upper() == "PIVOTMV":
            X = compute_benchmark_pivot(path, file, session_step, measure, benchmark_type, benchmark)
        elif execution_plan.upper() == "JOININMEMORY":
            X = pd.read_csv(path + file + "_" + str(session_step) + ".csv", encoding="utf-8")
            X.columns = [x.lower() for x in X.columns]
            toprint["cardinality"] = len(X.index)
            X = compute_benchmark_joininmemory(path, file, session_step, X, measure, benchmark_type, benchmark, json.loads(cube))
        elif execution_plan.upper() == "JOININDBMS":
            X = compute_benchmark_joinindbms(path, file, session_step, measure, benchmark_type, json.loads(cube))
        else:
            raise ValueError("Unknown plan: " + execution_plan)

    cardinality_join = len(X.index)
    if cardinality_join == 0:
        raise ValueError("Extended cube is empty")
    toprint["cardinality_extcube"] = cardinality_join

    ###############################################################################
    # DISTANCE
    ###############################################################################
    start_time = datetime.now()
    if distance_function == "" or distance_function == "{}":
        outer_key = measure
    else:
        using = json.loads(distance_function)
        outer_key = evaluate(X, using["fun"], using["params"])
        X = X[[x for x in X.columns if "benchmark." not in x]]
    elapsed = datetime.now() - start_time
    toprint["time_comparison"] = elapsed.seconds * 1000 + int(elapsed.microseconds / 1000)

    ###############################################################################
    # LABELING
    ###############################################################################
    start_time = datetime.now()
    if labeling_schema in functions:
        X["model_labeling"] = functions[labeling_schema](X[outer_key])
    else:
        labels = [y[2] for y in [x[1:-1].split(",") for x in labeling_schema.split(";")]]
        bins = [float(y[0]) for y in [x[1:-1].split(",") for x in labeling_schema.split(";")]]
        bins.append(float(labeling_schema.split(";")[-1].split(",")[1]))
        X["model_labeling"] = pd.cut(X[outer_key], bins=bins, labels=labels)
    elapsed = datetime.now() - start_time
    toprint["time_labeling"] = elapsed.seconds * 1000 + int(elapsed.microseconds / 1000)

    # orig_columns = X.columns
    # P = X["model_labeling"].value_counts().rename_axis('interval').reset_index(name='counts').sort_values(by=['interval'])
    # P["label"] = labels
    # X = pd.merge(X, P, left_on=["model_labeling"], right_on=["interval"])
    # X = X.rename(columns={"model_labeling": "foo", "label": "model_labeling" })
    # X = X[orig_columns]
    # P["label"] = "model_labeling=" + P["label"]
    # P.to_csv(path + file + "_" + str(session_step) + "_properties.csv", index=False)
    # X.to_csv(path + file + "_" + str(session_step) + "_enhanced.csv", index=False)
    if cardinality_join != len(X.index):
        raise ValueError("Cardinality does not match, before: " + str(init_len) + ", after: " + str(len(X.index)))
    return X

if __name__ == '__main__':
    ###############################################################################
    # PARAMETERS SETUP
    ###############################################################################
    parser = argparse.ArgumentParser()
    parser.add_argument("--path",              help="where to put the output", type=str)
    parser.add_argument("--file",              help="the file name",           type=str)
    parser.add_argument("--session_step",      help="the session step",        type=str)
    parser.add_argument("--distance_function", help="where to put the output", type=str)
    parser.add_argument("--cube",              help="cube to assess",          type=str)
    parser.add_argument("--benchmark_type",    help="type of benchmark",       type=str)
    parser.add_argument("--benchmark",         help="benchmark value",         type=str)
    parser.add_argument("--labeling_schema",   help="labeling schema",         type=str)
    parser.add_argument("--measure",           help="assessed measure",        type=str)
    parser.add_argument("--time_cube",         help="time to compute the target cube", type=int)
    parser.add_argument("--time_benchmark",    help="time to compute the benchmark cube", type=int)
    parser.add_argument("--id",                help="statement id", type=int)
    parser.add_argument("--plan",              help="which execution plan to use", type=str)
    parser.add_argument("--dbms",              help="used dbms", type=str)
    parser.add_argument("--indexes",           help="used dbms", type=str)
    parser.add_argument("--save",              help="used dbms", type=str)
    args  = parser.parse_args()
    print(args)
    path  = args.path
    file  = args.file
    session_step = args.session_step
    distance_function = args.distance_function.lower()
    benchmark_type = args.benchmark_type.lower()
    benchmark = args.benchmark
    measure = args.measure
    cube = args.cube
    labeling_schema = args.labeling_schema
    execution_plan = args.plan
    path = path.replace("\"", "")
    df = assess(path, file, session_step, distance_function, benchmark_type, benchmark, measure, cube, labeling_schema, execution_plan)
    if not args.save is None:
        df \
            .round(decimals=1)\
            .replace([np.inf], "Infinity")\
            .sort_values(by=sorted(list(df.columns)), axis=0).to_csv(path + file + "_" + session_step + "_enriched.csv", index=False)
    exists = os.path.exists('resources/assess/time.csv')
    with open("resources/assess/time.csv", 'a+') as o:
        toprint["time_cube"] = args.time_cube if args.time_cube > 0 else 1
        toprint["time_benchmark"] = args.time_benchmark if args.time_cube > 0 else 1
        toprint["id"] = args.id
        toprint["plan"] = execution_plan
        toprint["dbms"] = args.dbms
        toprint["indexes"] = args.indexes
        header = []
        values = []
        for key, value in toprint.items():
            header.append(key)
            values.append(str(value))
        if not exists:
            o.write(','.join(header) + "\n")
        o.write(','.join(values) + "\n")
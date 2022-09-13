import argparse
import json
import numpy as np
import pandas as pd
from scipy import stats
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest
from yellowbrick.cluster import KElbowVisualizer

###############################################################################
# PARAMETERS SETUP
###############################################################################
parser = argparse.ArgumentParser()
parser.add_argument("--path", help="where to put the output", type=str)
parser.add_argument("--file", help="the file name", type=str)
parser.add_argument("--session_step", help="the session step", type=int)
parser.add_argument("--k", help="size k", type=int)
parser.add_argument("--models", nargs='*', help="mining models to apply")
parser.add_argument("--cube", help="cube")
parser.add_argument("--computeproperty", help="whether to compute properties")
args = parser.parse_args()
path = args.path.replace("\"", "")
file = args.file
session_step = args.session_step
cube = args.cube.replace("__", " ")
compute_property = bool(args.computeproperty)
k = args.k
cube = json.loads(cube)
models = args.models

###############################################################################
# APPLY MODELS
###############################################################################
try:
    X = pd.read_csv(path + file + "_" + str(session_step) + ".csv", encoding="utf-8")
except:
    X = pd.read_csv(path + file + "_" + str(session_step) + ".csv", encoding="cp1252")

X.columns = [x.lower() for x in X.columns]
cells = len(X.index)
measures = [x["MEA"].lower() for x in cube["MC"]]
P = pd.DataFrame(columns=["model", "component", "property", "value"])

if cells > 0:
    prop = []

    for m in measures:
        X["zscore_" + m] = np.around(np.nan_to_num(stats.zscore(X[m]), 0), decimals=3)

    if "clustering" in models and (k is None or k > 1):
        def_k = k
        if cells > 10:
            if def_k is None:
                model = KMeans()
                visualizer = KElbowVisualizer(model, k=(2, min(6, cells)))
                visualizer.fit(X[measures].astype(float))  # Fit the data to the visualizer
                def_k = visualizer.elbow_value_
            if def_k is None:
                def_k = 3
            if def_k < cells:
                kmeans = KMeans(n_clusters=def_k, random_state=0).fit(X[measures])
                X["model_clustering"] = kmeans.labels_
                # print(kmeans.inertia_)
                for idx, c in enumerate(kmeans.cluster_centers_):
                    prop.append(["model_clustering", idx, "centroid", round(c[0], 2)])

    if "outliers" in models:
        def_k = k
        if def_k is None:
            def_k = int(cells / 4)
        outliers = IsolationForest(random_state=0).fit(X[measures])
        X["outlierness"] = outliers.predict(X[measures])
        X["model_outliers"] = X["outlierness"].isin(X[X["outlierness"] < 0]["outlierness"].nsmallest(def_k, keep='first'))
        if compute_property and len(X[X["model_outliers"] == True].index) > 0:
            prop.append(["model_outliers", "True", "outlierness", round(X[X["model_outliers"] == True]["outlierness"].mean(), 2)])
        if compute_property and len(X[X["model_outliers"] == False].index) > 0:
            prop.append(["model_outliers", "False", "outlierness", round(X[X["model_outliers"] == False]["outlierness"].mean(), 2)])
        X = X.drop("outlierness", axis=1)

    if "skyline" in models and len(measures) > 1:
        # #########################################################################
        # https://stackoverflow.com/questions/32791911/fast-calculation-of-pareto-front-in-python
        # #########################################################################
        def is_pareto_efficient_simple(costs):
            """
            Find the pareto-efficient points
            :param costs: An (n_points, n_costs) array
            :return: A (n_points, ) boolean array, indicating whether each point is Pareto efficient
            """
            is_efficient = np.ones(costs.shape[0], dtype=bool)
            for i, c in enumerate(costs):
                if is_efficient[i]:
                    is_efficient[is_efficient] = np.any(costs[is_efficient] >= c, axis=1)  # Keep any point with a lower cost
                    is_efficient[i] = True  # And keep self
            return is_efficient
        X["model_skyline"] = is_pareto_efficient_simple(X[measures].to_numpy())
        if compute_property and len(X[X["model_skyline"] == True].index) > 0:
            prop.append(["model_skyline", "True", "avgZscore", round(X[X["model_skyline"] == True]["zscore_" + measures[0]].mean(), 2)])
        if compute_property and len(X[X["model_skyline"] == False].index) > 0:
            prop.append(["model_skyline", "False", "avgZscore", round(X[X["model_skyline"] == False]["zscore_" + measures[0]].mean(), 2)])

    if "top-k" in models:
        def_k = k
        if def_k is None:
            def_k = int(cells / 4)
        for m in measures:
            X["model_top_" + m] = X[m].isin(X[m].nlargest(def_k, keep='first'))
            if compute_property and len(X[X["model_top_" + m] == True].index) > 0:
                prop.append(["model_top_" + m, "True", "avgZscore", round(X[X["model_top_" + m] == True]["zscore_" + m].mean(), 2)])
            if compute_property and len(X[X["model_top_" + m] == False].index) > 0:
                prop.append(["model_top_" + m, "False", "avgZscore", round(X[X["model_top_" + m] == False]["zscore_" + m].mean(), 2)])

    if "bottom-k" in models:
        def_k = k
        if def_k is None:
            def_k = int(cells / 4)
        for m in measures:
            X["model_bottom_" + m] = X[m].isin(X[m].nsmallest(def_k, keep='first'))
            if compute_property and len(X[X["model_bottom_" + m] == True].index) > 0:
                prop.append(["model_bottom_" + m, "True", "avgZscore", round(X[X["model_bottom_" + m] == True]["zscore_" + m].mean(), 2)])
            if compute_property and len(X[X["model_bottom_" + m] == False].index) > 0:
                prop.append(["model_bottom_" + m, "False", "avgZscore", round(X[X["model_bottom_" + m] == False]["zscore_" + m].mean(), 2)])

    X.to_csv(path + file + "_" + str(session_step) + "_ext.csv", index=False)
    if compute_property:
        P = P.append(pd.DataFrame(prop, columns=["model", "component", "property", "value"]))
        P.to_csv(path + file + "_" + str(session_step) + "_properties.csv", index=False)
else:
    raise ValueError('Empty data')

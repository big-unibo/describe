import pandas as pd
import numpy as np
import unittest
import math
# sys.path.append('src/main/python/')
# sys.path.append('../../main/python/')
from assess import *

class TestAssess(unittest.TestCase):

    def test_difference(self):
        X = pd.DataFrame([1, 2, 3])
        Y = pd.DataFrame([4, 5, 6])
        res = difference(X, Y)
        self.assertTrue(pd.DataFrame([-3, -3, -3]).equals(res), res)

    def test_differenceNormMinMax(self):
        X = pd.DataFrame([4, 6, 8])
        Y = pd.DataFrame([1, 2, 3])
        res = minmaxnorm(difference(X, Y))
        self.assertTrue(pd.DataFrame([0.0, 0.5, 1.0]).equals(res), res)

    def test_ratio(self):
        X = pd.DataFrame([40, 60, 80])
        Y = pd.DataFrame([20, 30, 40])
        res = ratio(X, Y)
        self.assertTrue(pd.DataFrame([2.0, 2.0, 2.0]).equals(res), res)

    def test_eval(self):
        X = pd.DataFrame([[40, 1, 20],
                          [60, 1, 20]], columns=["a", "b", "c"])
        res = pd.DataFrame([[40, 1, 20, 40.0, 20.0],
                            [60, 1, 20, 60.0, 40.0]], columns=["a", "b", "c", "ratio_1", "difference_2"])
        using = {
            "fun": "difference",
            "params":
                [
                    {"fun": "ratio", "params": ["a", "b"]},
                    "c"
                ]
        }
        evaluate(X, using["fun"], using["params"])
        self.assertTrue(pd.DataFrame(X).equals(res), X)

    def test_eval2(self):
        X = pd.DataFrame([[40, 1, 20],
                          [60, 1, 20]], columns=["a", "b", "c"])
        res = pd.DataFrame([[40, 1, 20, 50.0, -10.0],
                            [60, 1, 20, 50.0, 10.0]], columns=["a", "b", "c", "50", "difference_1"])
        using = {
            "fun": "difference",
            "params": ["a", 50]
        }
        evaluate(X, using["fun"], using["params"])
        self.assertTrue(pd.DataFrame(X).equals(res), X)

    def test_eval(self):
        X = pd.DataFrame([[40, 1, 20],
                          [60, 1, 20]], columns=["a", "b", "c"])
        res = pd.DataFrame([[40, 1, 20, 1.0, 40.0, 20.0, 20.0],
                            [60, 1, 20, 1.0, 60.0, 20.0, 40.0]],
                           columns=["a", "b", "c", "1", "ratio_2", "ratio_3", "difference_3"])
        using = {
            "fun": "difference",
            "params":
                [
                    {"fun": "ratio", "params": ["a", 1]},
                    {"fun": "ratio", "params": ["c", 1]},
                ]
        }
        key = evaluate(X, using["fun"], using["params"])
        self.assertTrue(pd.DataFrame(X).equals(res), X)
        self.assertTrue(key == "difference_3", X)

    path = "../../../src/main/resources/assess/"
    cube_fixed = """{"SC":[],"PROPERTIES":[],"GC":["the_month","country"],"MC":[{"MEA":"unit_sales","AGG":"sum","AS":"unit_sales"}]}"""
    cube_sibling = """{"SC":[{"VAL":["'1997-07'"],"SLICE":true,"TIME":true,"COP":"=","ATTR":"the_month"}],"PROPERTIES":[],"GC":["country","the_month"],"MC":[{"MEA":"unit_sales","AGG":"sum","AS":"unit_sales"}]}"""

    def test_workload2(self):
        res = assess(self.path, "siblingnaive", "0", """{"params":[{"params":["unit_sales","benchmark.unit_sales"],"fun":"difference"}], "fun":"minmaxnorm"}""", "sibling", "(product_subcategory,=,['Wine'])", "unit_sales", self.cube_sibling, "5star", "JOININMEMORY")
        self.assertTrue(set([0.46, 0.81, 0.21, 0.08, 1.0, 0.33, 0.42, 0.49, 0.74, 0.37, 0.62, 0.0]) == set([round(x, 2) for x in res["minmaxnorm_2"].values]), res)

    def test_workload3(self):
        res = assess(self.path, "fixed", "0", """{"params":["unit_sales",0],"fun":"difference"}""", "target", "0", "unit_sales", self.cube_fixed, "(100,130,bad);(130,160,ok);(160,Infinity,good)", "JOININMEMORY")
        self.assertTrue(["bad", "bad", "ok", "bad", "ok", "ok", "good", "bad", "ok", "bad", "good", "good"] == [x for x in res["model_labeling"].values], res)

    def test_paper1(self):
        cube = self.cube_fixed
        session_step = "0"
        comparison = """{"params":["unit_sales", 0], "fun":"difference"}"""
        labeling = "deciles"
        result = ["1", "1", "5", "1", "8", "6", "9", "3", "7", "4", "10", "10"]

        # Naive
        res = assess(self.path, "fixed", session_step, comparison, "target", "0", "unit_sales", cube, labeling, "JOININMEMORY")
        self.assertTrue(result == [x for x in res["model_labeling"].values], res)

        # Not Naive
        res = assess(self.path, "fixed", session_step, comparison, "target", "0", "unit_sales", cube, labeling, False)
        self.assertTrue(result == [x for x in res["model_labeling"].values], res)

    def test_paper2(self):
        cube = self.cube_fixed
        session_step = "0"
        comparison = """{"params":["unit_sales"], "fun":"minmaxnorm"}"""
        labeling = "5star"
        result = sorted(["*", "*", "**", "*", "****", "***", "****", "*", "***", "**", "*****", "*****"])

        # Naive
        res = assess(self.path, "fixed", session_step, comparison, "target", "0", "unit_sales", cube, labeling, "JOININMEMORY")
        self.assertTrue(result == sorted([x for x in res["model_labeling"].values]), res)

        # Not Naive
        res = assess(self.path, "fixed", session_step, comparison, "target", "0", "unit_sales", cube, labeling, "NOJOIN")
        self.assertTrue(result == sorted([x for x in res["model_labeling"].values]), res)

    def test_paper3(self):
        cube = """{"SC":[{"VAL":["'Beer'"],"SLICE":true,"TIME":false,"COP":"=","ATTR":"product_subcategory"}],"PROPERTIES":["population"],"GC":["country","the_month"],"MC":[{"MEA":"unit_sales","AGG":"sum","AS":"unit_sales"}]}"""
        session_step = "0"
        comparison = """{"params":[{"params":[{"params":["unit_sales","population"],"fun":"ratio"},{"params":["benchmark.unit_sales","benchmark.population"],"fun":"ratio"}],"fun":"difference"}],"fun":"minmaxnorm"}"""
        labeling = "5star"
        result = sorted(["*", "*", "**", "**", "**", "***", "***", "***", "****", "****", "*****", "*****"])

        # Naive
        res0 = assess(self.path, "siblingnaive", session_step, comparison, "sibling", "(product_subcategory,=,['Wine'])", "unit_sales", cube, labeling, "JOININMEMORY")
        self.assertTrue(result == sorted([x for x in res0["model_labeling"].values]), res0)

        # Not Naive (My OPT)
        res1 = assess(self.path, "siblingopt", session_step, comparison, "sibling", "(product_subcategory,=,['Wine'])", "unit_sales", cube, labeling, "NOJOIN")
        self.assertTrue(result == sorted([x for x in res1["model_labeling"].values]), res1)

        # Not Naive (Patrick's OPT)
        res2 = assess(self.path, "siblingopt", 1, comparison, "sibling", "(product_subcategory,=,['Wine'])", "unit_sales", cube, labeling, "JOININDBMS")
        self.assertTrue(result == sorted([x for x in res2["model_labeling"].values]), res2)

        res0 = res0.reindex(sorted(res0.columns), axis=1)
        res1 = res1.reindex(sorted(res1.columns), axis=1)
        res2 = res2.reindex(sorted(res2.columns), axis=1)
        self.assertTrue(res0.equals(res1), str(res0) + "\n" + str(res1))
        self.assertTrue(res0.equals(res2), str(res0) + "\n" + str(res2))

    def test_paper4(self):
        cube = self.cube_sibling
        session_step = "0"
        comparison = """{"params":["unit_sales","benchmark.unit_sales"],"fun":"ratio"}"""
        labeling = "(0.0,0.9,worse);(0.9,1.1,fine);(1.1,Infinity,better)"
        result = ["better"]

        # Naive
        res0 = assess(self.path, "pastnaive", session_step, comparison, "past", "4", "unit_sales", cube, labeling, "JOININMEMORY")
        self.assertTrue(result == res0["model_labeling"].values, res0)

        # Not Naive (My OPT)
        res1 = assess(self.path, "pastopt", session_step, comparison, "past", "4", "unit_sales", cube, labeling, "NOJOIN")
        self.assertTrue(result == res1["model_labeling"].values, res1)

        # Not Naive (My OPT)
        res2 = assess(self.path, "pastopt", 1, comparison, "past", "4", "unit_sales", cube, labeling, "NOJOIN")
        self.assertTrue(["fine"] == res2["model_labeling"].values, res2)

        # Not Naive (Patrick's OPT)
        res3 = assess(self.path, "pastopt", 2, comparison, "past", "4", "unit_sales", cube, labeling, "JOININDBMS")
        self.assertTrue(["fine"] == res3["model_labeling"].values, res3)

        res0 = res0.reindex(sorted(res0.columns), axis=1)
        res1 = res1.reindex(sorted(res1.columns), axis=1)
        res2 = res2.reindex(sorted(res2.columns), axis=1)
        res3 = res3.reindex(sorted(res3.columns), axis=1)
        res0["ratio_1"] = res0["ratio_1"].round(1)
        res1["ratio_1"] = res1["ratio_1"].round(1)
        res2["ratio_1"] = res2["ratio_1"].round(1)
        res3["ratio_1"] = res3["ratio_1"].round(1)
        self.assertTrue(res0.equals(res1), str(res0) + "\n" + str(res1))
        self.assertTrue(res2.equals(res3), str(res2) + "\n" + str(res3))

    def test_quality(self):
        N = pd.read_csv(self.path + "paper_sibling_naive.csv").values
        O = pd.read_csv(self.path + "paper_sibling_opt.csv").values
        self.assertTrue(np.array_equal(N, O))

if __name__ == '__main__':
    unittest.main()

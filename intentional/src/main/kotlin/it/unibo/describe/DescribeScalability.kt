@file:JvmName("DescribeScalability")

package it.unibo.describe

import com.google.common.collect.Lists
import it.unibo.conversational.database.Config
import it.unibo.conversational.database.DBmanager
import it.unibo.describe.Scalability.L
import it.unibo.describe.Scalability.csvPrinter
import it.unibo.describe.Scalability.csvPrinter2
import it.unibo.describe.Scalability.first
import it.unibo.describe.Scalability.path
import krangl.*
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Statement
import java.util.*

object Scalability {
    val L: Logger = LoggerFactory.getLogger(Scalability::class.java)
    const val path = "resources/describe/output/"
    val writer = Files.newBufferedWriter(Paths.get("resources/describe/time.csv"))
    val csvPrinter = CSVPrinter(writer, CSVFormat.DEFAULT)
    val writer2 = Files.newBufferedWriter(Paths.get("resources/describe/sessions.csv"))
    val csvPrinter2 = CSVPrinter(writer2, CSVFormat.DEFAULT)
    var first = true
}

fun main() {
    scalability()
    covid()
    foodmart()
    csvPrinter.close()
    csvPrinter2.close()
}

fun covid() {
    val intentions = listOf(
            listOf(
                    "with COVID-19 describe deaths by continentExp",
                    "with COVID-19 describe deaths by countriesAndTerritories for continentExp = 'America'",
                    "with COVID-19 describe deaths by month, countriesAndTerritories for countriesAndTerritories = 'United_States_of_America'",
                    "with COVID-19 describe deaths, cases by month, countriesAndTerritories for countriesAndTerritories = 'United_States_of_America'",
            ),
            listOf(
                    "with COVID-19 describe deaths by continentExp",
                    "with COVID-19 describe deaths by countriesAndTerritories for continentExp = 'America'",
                    "with COVID-19 describe deaths by month, countriesAndTerritories for countriesAndTerritories in ('United_States_of_America', 'Italy')",
                    "with COVID-19 describe deaths by month, countriesAndTerritories for countriesAndTerritories in ('United_States_of_America', 'Italy', 'France')",
                    "with COVID-19 describe deaths, cases by month, countriesAndTerritories for countriesAndTerritories in ('United_States_of_America', 'Italy', 'France')",
            ),
            listOf(
                    "with COVID-19 describe deaths by month for countriesAndTerritories = 'Italy'",
                    "with COVID-19 describe deaths by month, countriesAndTerritories for countriesAndTerritories in ('Italy', ' France', ' Germany')",
                    "with COVID-19 describe deaths by countriesAndTerritories for continentExp in ('America', 'Europe', 'Africa')",
                    "with COVID-19 describe deaths, cases by month, countriesAndTerritories for countriesAndTerritories in ('United_States_of_America', 'Italy')",
            ),
            listOf(
                    "with COVID-19 describe deaths by month for countriesAndTerritories = 'United_States_of_America'",
                    "with COVID-19 describe deaths by month, countriesAndTerritories for continentExp in ('America')",
                    "with COVID-19 describe deaths by month, countriesAndTerritories for continentExp in ('America', 'Europa', 'Africa')",
                    "with COVID-19 describe deaths by continentExp",
            ),
            listOf(
                    "with COVID-19 describe deaths by month",
                    "with COVID-19 describe deaths by month, countriesAndTerritories for countriesAndTerritories = 'Italy'",
                    "with COVID-19 describe deaths by month, countriesAndTerritories",
                    "with COVID-19 describe deaths by countriesAndTerritories",
            ),
            listOf(
                    "with COVID-19 describe deaths by month for countriesAndTerritories = 'Italy'",
                    "with COVID-19 describe deaths by month, countriesAndTerritories for countriesAndTerritories in ('Italy', 'Spain', 'Germany')",
                    "with COVID-19 describe deaths by countriesAndTerritories",
                    "with COVID-19 describe deaths by dateRep, countriesAndTerritories for countriesAndTerritories in ('Italy', 'Spain', 'Germany')",
            ),
            listOf(
                    "with COVID-19 describe deaths by month for countriesAndTerritories = 'Italy'",
                    "with COVID-19 describe deaths, cases by month for countriesAndTerritories = 'Italy'",
                    "with COVID-19 describe deaths by countriesAndTerritories for continentExp in ('America')",
                    "with COVID-19 describe deaths by countriesAndTerritories for continentExp in ('America', 'Europe')",
                    "with COVID-19 describe deaths by month, countriesAndTerritories for countriesAndTerritories in ('United_States_of_America')",
            ),
            listOf(
                    "with COVID-19 describe deaths by dateRep for countriesAndTerritories = 'Italy'",
                    "with COVID-19 describe deaths by countriesAndTerritories for continentExp in ('Europe')",
                    "with COVID-19 describe deaths by countriesAndTerritories for countriesAndTerritories in ('Italy', 'Spain', 'Germany')",
                    "with COVID-19 describe deaths by month, countriesAndTerritories for countriesAndTerritories in ('Italy', 'Spain', 'Germany')",
                    "with COVID-19 describe deaths by dateRep, countriesAndTerritories for countriesAndTerritories in ('Italy', 'Spain', 'Germany')",
                    "with COVID-19 describe deaths by countriesAndTerritories for continentExp in ('America', 'Europe')",
                    "with COVID-19 describe deaths, cases by dateRep for countriesAndTerritories = 'Italy'",
            ),
    )

    val sessions: MutableList<List<String>> = mutableListOf()
    val randomIntentions: List<String> = intentions.flatten().distinct()
    val random = Random(3)
    for (i in 0..9) {
        var lastIdx = 0
        val session: MutableList<String> = mutableListOf()
        val length = 3 + random.nextInt(4)
        for (j in 0..length) {
            var nextIdx = random.nextInt(randomIntentions.size)
            while (nextIdx == lastIdx) {
                nextIdx = random.nextInt(randomIntentions.size)
            }
            lastIdx = nextIdx
            session += randomIntentions[nextIdx]
        }
        sessions += session
    }
    for (t in 0..1) {
        for (version in listOf("adbis", "ext")) {
            for (i in intentions.indices) {
                run("covid$i", intentions[i], t, "COVID-19", version)
            }
            for (i in sessions.indices) {
                run("random$i", sessions[i], t, "COVID-19", version)
                for (s in sessions[i]) {
                    csvPrinter2.printRecord("random$i", s)
                    csvPrinter2.flush()
                }
            }
        }
    }
}

fun foodmart() {
    for (t in 0..0) {
        for (version in listOf("adbis", "ext")) {
            val intentions = listOf(
                    "with sales describe unit_sales, store_sales by the_date", // 323
                    "with sales describe unit_sales, store_sales by the_date, customer_id", // 20k
                    "with sales describe unit_sales, store_sales by the_date, product_id", // 77k
                    "with sales describe unit_sales, store_sales by the_date, customer_id,  product_id"// 87k
            )
            run("foodmart0", intentions, t, "sales", version)
        }
    }
}

fun scalability() {
    for (scaleFactor in Lists.newArrayList(2, 10, 100)) {
        val cube = "ssbora$scaleFactor"
        val conn = DBmanager.getDataConnection(Config.getCube(cube))
        conn.createStatement().use {
            execute(it, "CREATE MATERIALIZED VIEW MV_Y_NPR_SR ENABLE QUERY REWRITE AS select year, nation, supplier, sum(QUANTITY) as QUANTITY, SUM(REVENUE) AS REVENUE FROM lineorder$scaleFactor FT INNER JOIN date$scaleFactor ON date$scaleFactor.datekey = FT.datekey INNER JOIN customer$scaleFactor ON customer$scaleFactor.custkey = FT.custkey INNER JOIN supplier$scaleFactor ON supplier$scaleFactor.suppkey = FT.suppkey where region = 'EUROPE' and s_region = 'EUROPE' GROUP BY year, nation, supplier")
            execute(it, "CREATE MATERIALIZED VIEW MV_Y_NPR ENABLE QUERY REWRITE AS select year, nation, sum(QUANTITY) as QUANTITY, SUM(REVENUE) AS REVENUE FROM lineorder$scaleFactor FT INNER JOIN date$scaleFactor ON date$scaleFactor.datekey = FT.datekey INNER JOIN customer$scaleFactor ON customer$scaleFactor.custkey = FT.custkey INNER JOIN supplier$scaleFactor ON supplier$scaleFactor.suppkey = FT.suppkey where region = 'EUROPE' and s_region = 'EUROPE' GROUP BY year, nation")
            execute(it, "CREATE MATERIALIZED VIEW MV_YYY ENABLE QUERY REWRITE AS select year, sum(QUANTITY) as QUANTITY, SUM(REVENUE) AS REVENUE FROM lineorder$scaleFactor FT INNER JOIN date$scaleFactor ON date$scaleFactor.datekey = FT.datekey INNER JOIN customer$scaleFactor ON customer$scaleFactor.custkey = FT.custkey INNER JOIN supplier$scaleFactor ON supplier$scaleFactor.suppkey = FT.suppkey where region = 'EUROPE' and s_region = 'EUROPE' GROUP BY year")
        }
        conn.close()
        for (t in 0..2) {
            val intentions = listOf(
                    "with $cube describe quantity, revenue by year for S_REGION = 'EUROPE' AND REGION = 'EUROPE'",
                    "with $cube describe quantity, revenue by year, nation for S_REGION = 'EUROPE' AND REGION = 'EUROPE'",
                    "with $cube describe quantity, revenue by year, nation, supplier for S_REGION = 'EUROPE' AND REGION = 'EUROPE'"
            )
            run("ssb0", intentions, t, cube, "ext")
        }
    }
}

fun run(sessionId: String, intentions: List<String>, t: Int, cube: String, version: String) {
    var idx = 0
    var d = Describe(null, false)
    var firstIntention = true
    var coverage: DataFrame = dataFrameOf("foo")("bar")
    val fact =
            if (cube.contains("covid", true)) { //  || cube.contains("sales", true)
                getEntireCube(cube)
            } else {
                dataFrameOf("foo")("bar")
            }
    DescribeExecute.clear()
    intentions.forEach {
        d = DescribeExecute.parse(if (firstIntention) null else d, it, false)

        d.statistics["sessionid"] = sessionId
        d.statistics["id"] = idx++
        d.statistics["run"] = t
        d.statistics["dataset"] = cube
        d.statistics["intention"] = it
        d.statistics["intention_characters"] = it.length
        d.statistics["coverage"] = -1
        d.statistics["intersection"] = -1
        d.statistics["version"] = version

        val res = DescribeExecute.execute(d, path, makePivot = !cube.contains("ssb"), oldInterest = (version == "adbis"))
        if (first) {
            first = false
            csvPrinter.printRecord(d.statistics.keys.sorted())
        }

        if (cube.contains("covid", true)) { //  || cube.contains("sales", true)
            var c = res.second.rename(*res.second.names.map { it to it.lowercase() }.toTypedArray())
            c = c.filter { it[res.third.first] eq res.third.second }
            val join = fact.innerJoin(c, by = d.attributes.map { it.lowercase() }, suffices = "" to "bc_").removeIf { it.name.contains("bc_") }
            if (firstIntention) {
                coverage = join
            } else {
                val old: List<DataFrameRow> = coverage.rows.map { it }
                val new: List<DataFrameRow> = join.rows.map { it }
                coverage = dataFrameOf(old + new)
            }

            val cols =
                    if (cube.contains("sales")) {
                        listOf("time_id", "product_id", "customer_id", "store_id", "promotion_id")
                    } else if (cube.contains("covid", true)) {
                        listOf("countriesandterritories", "daterep")
                    } else {
                        listOf()
                    }
            var grouped = coverage.groupBy(*cols.toTypedArray()).count()
            grouped = grouped.addColumn("n") { it["n"] - 1 }
            val duplicates: Double = grouped["n"].sum()!!.toDouble()
            d.statistics["coverage"] = 1.0 * grouped.nrow / fact.nrow
            d.statistics["intersection"] = 1.0 * duplicates / coverage.nrow
        }
        // L.warn(d.statistics.keys.filter { it.equals("version") || it.equals("coverage") || it.equals("intersection") }.sorted().map { it + ": " + d.statistics[it].toString() }.reduce { a, b -> "$a, $b" })
        csvPrinter.printRecord(d.statistics.keys.sorted().map { d.statistics[it] })
        csvPrinter.flush()
        firstIntention = false
    }
}

fun execute(stmt: Statement, sql: String) {
    try {
        stmt.execute(sql)
        L.warn("Done. $sql")
    } catch (e: Exception) {
        L.warn("${e.message}. $sql")
    }
}

fun getEntireCube(c: String): DataFrame {
    val cube = Config.getCube(c)
    val s: String =
            if (cube.factTable.equals("sales_fact_1997")) {
                // T.the_month, T.the_year, C.gender, S.store_city, S.store_country, P.product_subcategory, P.product_category
                "select distinct FT.time_id, TO_CHAR(T.the_date, 'YYYY-MM-DD') as the_date, C.customer_id, S.store_id, P.product_id, promotion_id FROM sales_fact_1997 FT INNER JOIN time_by_day T ON FT.time_id = T.time_id INNER JOIN customer C ON FT.customer_id = C.customer_id INNER JOIN store S ON FT.store_id = S.store_id inner join product P on FT.product_id = P.product_id"
            } else {
                "select T.daterep, T.month, T.year, C.countriesAndTerritories, C.continentExp FROM covidfact FT INNER JOIN coviddate T ON FT.daterep = T.daterep INNER JOIN country C ON FT.countriesAndTerritories = C.countriesAndTerritories"
            }
    var res: DataFrame = dataFrameOf("foo") ("bar")
    DBmanager.executeDataQuery(cube, s) {
        res = DataFrame.fromResultSet(it)
    }
    res = res.rename(*res.names.map { it to it.lowercase() }.toTypedArray())
    return res
}
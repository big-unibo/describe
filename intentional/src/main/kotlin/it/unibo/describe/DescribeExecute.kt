package it.unibo.describe

import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.google.common.collect.Sets
import it.unibo.Intention
import it.unibo.antlr.gen.DescribeLexer
import it.unibo.antlr.gen.DescribeParser
import it.unibo.conversational.database.QueryGenerator
import it.unibo.conversational.datatypes.DependencyGraph
import krangl.*
import org.antlr.v4.runtime.ANTLRInputStream
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.tree.ParseTree
import org.antlr.v4.runtime.tree.ParseTreeWalker
import org.apache.commons.lang3.tuple.Pair
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.io.File
import kotlin.math.abs
import kotlin.math.roundToInt

/**
 * Describe intention in action.
 */
object DescribeExecute {
    private val L = LoggerFactory.getLogger(DescribeExecute::class.java)
    val Vcoord: MutableSet<MutableSet<String>> = mutableSetOf()
    val Vmemb: MutableMap<String, Double> = mutableMapOf()

    fun clear() {
        Vcoord.clear()
        Vmemb.clear()
    }

    fun parse(input: String?): Describe {
        return parse(null, input, true)
    }

    fun parse(input: String?, accumulateAttributes: Boolean): Describe {
        return parse(null, input, accumulateAttributes)
    }

    fun parse(d: Describe?, input: String?): Describe {
        return parse(d, input, true)
    }

    fun parse(d: Describe?, input: String?, accumulateAttributes: Boolean): Describe {
        val lexer = DescribeLexer(ANTLRInputStream(input)) // new ANTLRInputStream(System.in);
        val tokens = CommonTokenStream(lexer) // create a buffer of tokens pulled from the lexer
        val parser = DescribeParser(tokens) // create a parser that feeds off the tokens buffer
        val tree: ParseTree = parser.describe() // begin parsing at init rule
        val walker = ParseTreeWalker() // create standard walker
        val extractor = DescribeListenerCustom(d, accumulateAttributes)
        walker.walk(extractor, tree) // initiate walk of tree with listener
        return extractor.describe!!
    }

    /**
     * Extend the cube with the proxy cells.
     *
     * @param d the current intention
     * @param cube the current cube
     */
    fun extendCubeWithProxy(d: Describe, c: DataFrame, p: DataFrame): Triple<DataFrame, DataFrame, Set<String>> {
        val prevGc = d.previousAttributes // get the previous coordinate
        val coordinate = d.attributes
        val gencoord: MutableSet<String> = Sets.newLinkedHashSet()
        var cube = c
        var prevCube = p

        coordinate.forEach { currA ->
            if (!prevGc.contains(currA)) { // the previous cube contains the same attribute
                val prev: String? = prevGc.find { DependencyGraph.lca(d.cube, currA, it).isPresent } // find an attribute with a rollup/drilldown relationship from the previous coordinate
                if (!prev.isNullOrEmpty() && !(DependencyGraph.lca(d.cube, currA, prev).get() != currA && DependencyGraph.lca(d.cube, currA, prev).get() != prev)) {
                    val specific: String = DependencyGraph.lca(d.cube, currA, prev).get() // get the most specific attribute
                    val spec2gen: Map<String, String> = QueryGenerator.getFunctionalDependency(d.cube, specific, if (specific.equals(currA)) prev else currA)
                    if (prevGc.contains(specific)) { // if it is a rollup
                        cube = cube.addColumn(currA) { it[currA].map<Any> { it.toString() } }
                        prevCube = prevCube.addColumn(currA) {
                            it[specific].map<Any> { toFind -> spec2gen[toFind.toString()] }
                        }
                        gencoord.add(currA)
                    } else { // if it is a drill down
                        prevCube = prevCube.addColumn(prev) { it[prev].map<Any> { it.toString() } }
                        cube = cube.addColumn(prev) {
                            it[specific].map<Any> { toFind -> spec2gen[toFind.toString()] }
                        }
                        gencoord.add(prev)
                    }
                } else {
                    prevCube = prevCube.addColumn("all_$currA") { prevCube.cols[0].map<Any> { "all" } }
                    cube = cube.addColumn("all_$currA") { cube[currA].map<Any> { "all" } }
                    gencoord.add("all_$currA")
                }
            } else {
                gencoord.add(currA)
            }
        }
        return Triple(cube, prevCube, gencoord)
    }

    @JvmOverloads
    @Throws(Exception::class)
    fun execute(d: Describe, path: String, pythonPath: String = "src/main/python/", makePivot: Boolean = true, oldInterest: Boolean = true): Triple<JSONObject, DataFrame, Triple<String, Any, Double>> {
        val timeQuery = d.writeMultidimensionalCube(path)
        L.warn("Computing models...")
        val timeModel = d.computePython(pythonPath, path, "describe.py")
        L.warn("Models computed")
        val coordinate: Set<String> = d.attributes
        var cube: DataFrame = DataFrame.readCSV("$path${d.filename}_${d.sessionStep}_ext.csv").sortedBy(*coordinate.toTypedArray())
        val rows = cube.nrow
        L.warn("Read $path${d.filename}_${d.sessionStep}_ext.csv")
        d.statistics["time_query"] = timeQuery
        d.statistics["time_model"] = timeModel
        d.statistics["cardinality"] = cube.nrow

        var startTime: Long = System.currentTimeMillis()
        if (d.sessionStep >= 1) { // if this is not the first intention
            // get the previous cube
            var prevCube: DataFrame = DataFrame.readCSV(path + d.filename + "_" + (d.sessionStep - 1) + "_enhanced.csv")
            // prevCube = prevCube.rename(*prevCube.names.map { it to "${it}_bc" }.toTypedArray())
            val p = extendCubeWithProxy(d, cube, prevCube)
            cube = p.first
            prevCube = p.second
            val gencoord = p.third

            val stats = cube
                    .leftJoin(right = prevCube, by = gencoord, suffices = "" to "_bc") // and join them base on the proxy
                    .groupBy(*coordinate.toTypedArray()) // ... group by coordinate
                    .summarize(*(d.measures.map { m -> // ... average the z-score from the previous cube (i.e., proxy cells)
                        ("avg_zscore_$m") to {
                            if (d.prevMeasures.contains(m)) {
                                val avg: Double = it["zscore_${m}_bc"].mean(true)!!
                                if (avg.isNaN()) prevCube["zscore_${m}"].mean(false) else avg
                            } else {
                                0
                            }
                        }
                    }.toTypedArray()))
                    .sortedBy(*coordinate.toTypedArray()) // it is essential that this tuples are sorted as the ones from the original cube
            if (stats.nrow != rows) {
                throw java.lang.IllegalArgumentException("Left join is broken")
            }
            cube = cube.addColumns( // compute the difference between the zscores
                    *(d.measures
                        .filter { m -> prevCube.names.contains(m) }
                        .map { m -> ("zscore_$m") `=` { (it["zscore_$m"] - stats["avg_zscore_$m"]).map<Double> { 1.0 * (it * 1000).roundToInt() / 1000.0 } } }
                        .toTypedArray()
                    )).remove(gencoord.filter { !coordinate.contains(it) })
        }
        L.warn("Proxy completed")
        // get the peculiarity
        cube = cube.addColumn("peculiarity") { myMax(*(d.measures.map { m -> it["zscore_$m"] }.toTypedArray())) }
        if (!oldInterest) {
            // normalize the peculiarity
            var maxPeculiarity: Double = 0.0
            cube = cube.addColumn("peculiarity") {
                it["peculiarity"].map<Double> {
                    val v = abs(it)
                    maxPeculiarity = Math.max(maxPeculiarity, v)
                    v
                }.toTypedArray()
            }
            if (maxPeculiarity > 0) {
                cube = cube.addColumn("peculiarity") { it["peculiarity"] / maxPeculiarity }
            }

            // estimate the surprise
            cube = cube.addColumn("surprise") { innercube ->
                if (d.sessionStep == 0) {
                    1.0
                } else {
                    val columnValues = coordinate.map { attribute -> innercube[attribute].values() }
                    val sums = mutableListOf<Double>()
                    for (j in columnValues[0].indices) {
                        var sum = 0.0
                        for (i in columnValues.indices) {
                            sum += Vmemb.getOrDefault(columnValues[i][j], 0.0) / d.sessionStep
                        }
                        sums += 1.0 - sum / columnValues.size
                    }
                    sums
                }
            }

            // estimate the novelty (and update the hisory V---need to be done AFTER surprise)
            val currentMembers: MutableSet<String> = mutableSetOf()
            cube = cube.addColumn("novelty") { innercube ->
                val columnValues = coordinate.map { attribute -> innercube[attribute].values() }
                val sums = mutableListOf<Double>()
                for (j in columnValues[0].indices) {
                    val currentCoordinate: MutableSet<String> = mutableSetOf()
                    for (i in columnValues.indices) {
                        val member = columnValues[i][j].toString()
                        currentMembers += member
                        currentCoordinate.add(member)
                    }
                    sums += if (Vcoord.contains(currentCoordinate)) { 0.0 } else { 1.0 }
                    Vcoord += currentCoordinate
                }
                sums
            }
            currentMembers.forEach { member -> Vmemb.compute(member) { _, v -> (v ?: 0.0) + 1 } }

            //  // update the history V
            //  val currentMembers: MutableSet<String> = mutableSetOf()
            //  cube.rows.forEach {
            //      val currentCoordinate: MutableSet<String> = mutableSetOf()
            //      coordinate.forEach { attribute ->
            //          val member = it[attribute].toString()
            //          currentMembers += member
            //          currentCoordinate.add(member)
            //      }
            //      Vcoord += currentCoordinate
            //  }
            //  currentMembers.forEach { member -> Vmemb.compute(member) { _, v -> (v ?: 0.0) + 1 } }
        }
        d.statistics["time_interest"] = System.currentTimeMillis() - startTime

        val modelInterest: MutableMap<String, MutableMap<Any, Pair<Double, Long>>> = Maps.newLinkedHashMap()
        // val maxInterest: Double = -1.0
        // val maxInterestingComponent: Triple<String, String, Double>?
        cube.rows.forEach { row ->
            row.forEach { (model, component) ->
                if (model.startsWith("model_")) {
                    val interestingness: MutableMap<Any, Pair<Double, Long>> = modelInterest.getOrPut(model, { mutableMapOf() })
                    interestingness.compute(component!!)
                    { _, v ->
                        var sum = if (v == null) 0.0 else v.left
                        var count = if (v == null) 0L else v.right
                        sum +=
                                if (oldInterest) {
                                    row["peculiarity"] as Double
                                } else {
                                    (row["peculiarity"] as Double + row["novelty"] as Double + row["surprise"] as Double) / 3
                                }
                        count += 1
                        Pair.of(sum, count)
                    }
                }
            }
        }
        val bestModel: Triple<String, Any, Double> =
                modelInterest.entries.stream().map {
                    val max = it.value.maxByOrNull { it.value.left / it.value.right }!!
                    Triple(it.key, max.key, max.value.left / max.value.right)
                }.max { a, b ->
                    java.lang.Double.compare(a.third, b.third)
                }.get()

        val json = JSONObject()
        if (makePivot) {
            var properties = DataFrame.readCSV(path + d.filename + "_" + d.sessionStep + "_properties.csv")
            // TODO only the first property is kept
            properties = properties.groupBy("model", "component").summarize("properties") { Pair.of(it["property"].map<Any> { it.toString() }.first(), it["value"].map<Any> { it.toString() }.first()) }
            properties = properties.addColumn("interest") {
                it["model"]
                        .map<Any> { it }
                        .zip(it["component"].map<Any> { it.toString().lowercase() })
                        .map { p ->
                            val pair = modelInterest[p.first!!]!![parseVal(p.second!!.toString())]!!
                            pair.left / pair.right
                        }
            }
            properties.sortedByDescending("interest").rows.forEach {
                val rowJson = JSONObject()
                rowJson.put("component", it["model"] as String + "=" + it["component"].toString().lowercase())
                rowJson.put("interest", (it["interest"] as Double * 1000).roundToInt() / 1000.0)
                rowJson.put("properties", it["properties"])
                json.append("components", rowJson)
            }
            properties.writeCSV(File(path + d.filename + "_" + d.sessionStep + "_model.csv"))

            /* ***********************************************************************
             * WRITE TO FILE
             * **********************************************************************/
            startTime = System.currentTimeMillis()
            cube.rows.forEach {
                val rowJson = JSONObject()
                cube.names.forEach { name -> rowJson.put(name, it[name]) }
                json.append("raw", rowJson)
                d.measures.forEach { mea ->
                    val redJson = JSONObject()
                    cube.names.forEach { name ->
                        redJson.put(name, it[name])
                        if (name == mea) {
                            redJson.put("measure", name)
                            redJson.put("value", it[name])
                        }
                        json.append("red", redJson)
                    }
                }
            }
            json.put("pivot", getPivot(d, cube))
            d.statistics["pivot_time"] = System.currentTimeMillis() - startTime
        } else {
            d.statistics["pivot_time"] = 1 // set default time to 1 ms (otherwise charts with logarithmic scale won't work)
        }
        cube.writeCSV(File(path + d.filename + "_" + d.sessionStep + "_enhanced.csv"))
        return Triple(json, cube, bestModel)
    }

    fun getPivot(d: Describe, cube: DataFrame): JSONObject {
        val header = cube.names
        val currSchema = cube.names.withIndex().filter { d.attributes.contains(it.value) }.map { it.index }.toIntArray()
        val data: MutableList<List<String>> = Lists.newArrayList()
        for (r in cube.rows) {
            val row: MutableList<String> = Lists.newArrayList()
            r.values.forEach { v: Any? -> row.add(v.toString()) }
            data.add(row)
        }
        return Intention.getPivot(d, currSchema, data, header)
    }

    /**
     * Get maximum value among many columns
     */
    fun myMax(vararg cols: DataCol): Array<Double?> {
        if (cols.isEmpty()) {
            throw IllegalArgumentException("Empty columns")
        }
        if (cols.size == 1) {
            return cols[0].toDoubles()
        }
        return cols
                .map { it.toDoubles().toList() }
                .reduce { a1, a2 -> a1.zip(a2).map { (it.first!!).coerceAtLeast(it.second!!) } }
                .toTypedArray()
    }

    fun parseVal(s: String): Any {
        try {
            if (s.lowercase().equals("true") || s.lowercase().equals("false"))
                return java.lang.Boolean.parseBoolean(s)
            else throw IllegalArgumentException()
        } catch (e: Exception) {
            try {
                return java.lang.Integer.parseInt(s)
            } catch (e: Exception) {
                try {
                    return java.lang.Double.parseDouble(s)
                } catch (e: Exception) {
                    return s
                }
            }
        }
    }
}
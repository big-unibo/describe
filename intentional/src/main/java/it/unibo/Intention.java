package it.unibo;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.opencsv.CSVWriter;
import com.opencsv.ResultSetHelperService;
import it.unibo.conversational.Utils;
import it.unibo.conversational.database.Config;
import it.unibo.conversational.database.Cube;
import it.unibo.conversational.datatypes.DependencyGraph;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static it.unibo.conversational.Utils.quote;
import static it.unibo.conversational.database.DBmanager.executeDataQuery;


/**
 * A generic intentional operator
 */
public abstract class Intention implements IIntention {
    protected static final Logger L = LoggerFactory.getLogger(Intention.class);
    protected Cube cube;
    protected String cubeSyn;
    public static boolean DEBUG = false;
    protected String filename;
    private Set<String> measures = Sets.newLinkedHashSet();
    public final Set<String> prevMeasures;
    private Set<Triple<String, String, List<String>>> clause;
    private Set<String> attributes;
    private Set<String> properties;
    private final Set<String> prevAttributes;
    private final Set<Triple<String, String, List<String>>> prevClause;
    protected final int sessionStep;
    public final Map<String, Object> statistics = Maps.newHashMap();


    /**
     * Create an intention from the previous intention
     *
     * @param i previous intention
     */
    public Intention(final Intention i, final boolean accumulateAttributes) {
        if (i == null) {
            sessionStep = 0;
            setFilename(UUID.randomUUID().toString());
            measures = Sets.newLinkedHashSet();
            clause = Sets.newLinkedHashSet();
            attributes = Sets.newHashSet();
            properties = Sets.newHashSet();
            prevMeasures = Sets.newHashSet();
            prevAttributes = Sets.newHashSet();
            prevClause = Sets.newLinkedHashSet();
        } else {
            sessionStep = i.sessionStep + 1;
            setFilename(i.getFilename());
            prevMeasures = Sets.newLinkedHashSet(i.measures);
            prevAttributes = Sets.newLinkedHashSet(i.attributes);
            prevClause = Sets.newLinkedHashSet(i.clause);
            if (accumulateAttributes) {
                attributes = Sets.newLinkedHashSet(i.attributes);
                clause = Sets.newLinkedHashSet(i.clause);
                properties = Sets.newHashSet(i.properties);
            } else {
                attributes = Sets.newLinkedHashSet();
                clause = Sets.newLinkedHashSet();
                properties = Sets.newHashSet();
            }
            cube = i.getCube();
        }
    }

    public void setFilename(final String filename) {
        this.filename = filename;
    }

    public Set<Triple<String, String, List<String>>> getPrevClause() {
        return prevClause;
    }

    public Set<String> getPreviousAttributes() {
        return prevAttributes;
    }

    public void setCube(final String cube) {
        this.cube = Config.getCube(cube);
        this.cubeSyn = cube;
    }

    public void addMeasures(final Object... measures) {
        this.measures.addAll(Sets.newLinkedHashSet(Arrays.stream(measures).map(m -> m.toString().toLowerCase().replace("benchmark.", "").replace(cube + ".", "")).collect(Collectors.toList())));
    }

    public void setMeasures(final Set<String> measures) {
        setMeasures((Collection<String>) measures);
    }

    public void setMeasures(final Collection<String> measures) {
        this.measures = measures.stream().map(String::toLowerCase).collect(Collectors.toSet());
    }

    public void addClause(Triple<String, String, List<String>> clause) {
        final Iterator<Triple<String, String, List<String>>> iterator = this.clause.iterator();
        attributes.forEach(a -> {
            final Optional<String> lca = DependencyGraph.lca(cube, clause.getLeft(), a);
            if (lca.isPresent() && !lca.get().equalsIgnoreCase(a)) {
                throw new IllegalArgumentException("Internal predicate on " + a);
            }
        });

        while (iterator.hasNext()) {
            final Triple<String, String, List<String>> a = iterator.next();
            final Optional<String> lca = DependencyGraph.lca(cube, a.getLeft(), clause.getLeft());
            if (lca.isPresent() && (lca.get().equals(a.getLeft()) || lca.get().equals(clause.getLeft()))) {
                iterator.remove();
            }
        }
        this.clause.add(clause);
    }

    /**
     * Remove and return a selection clause
     *
     * @param f whether the clause should be removed
     * @return the removed tuple, throws exception otherwise
     */
    public Triple<String, String, List<String>> removeClause(final Function<Triple<String, String, List<String>>, Boolean> f) {
        final Iterator<Triple<String, String, List<String>>> iterator = this.clause.iterator();
        while (iterator.hasNext()) {
            final Triple<String, String, List<String>> t = iterator.next();
            if (f.apply(t)) {
                iterator.remove();
                return t;
            }
        }
        throw new IllegalArgumentException("Could not find the clause");
    }

    public void setAttributes(final Set<String> optAttributes) {
        this.attributes = optAttributes;
    }

    public void setAttributes(final Object... optAttributes) {
        this.attributes = Sets.newHashSet(optAttributes).stream().map(Object::toString).collect(Collectors.toSet());
    }

    /**
     * Add an attribute to the group by set. Check if any dependency exists wrt to previous attributes to remove redundancies.
     *
     * @param optAttributes new attribute
     * @param replace       whether to keep *only* the finest attribute for each hierarchy
     */
    public void addAttribute(final boolean replace, final Object... optAttributes) {
        for (final Object optAttribute : optAttributes) {
            final String attribute = optAttribute.toString().toLowerCase();
            if (attribute.startsWith("all")) {
                // TODO: probably a smarter way to avoid all* (e.g., allCustomers) would be to check the DependencyGraph)
                continue;
            }
            boolean toAdd = true;
            final Iterator<String> iterator = attributes.iterator();
            while (iterator.hasNext()) {
                final String a = iterator.next();
                final Optional<String> lca = DependencyGraph.lca(getCube(), a, attribute);
                // if (lca.isPresent()) {
                //     throw new IllegalArgumentException("Cannot have two (or more) levels (" + a + ", " + attribute + ") from the same hierarchy in the by clause");
                // }
            }
            if (toAdd) {
                attributes.add(attribute.replace("benchmark.", "").replace(cube + ".", ""));
            }
        }
    }

    /**
     * @return intention as JSON object that is used for SQL translation
     */
    public JSONObject getJSON() {
        final JSONObject obj = new JSONObject();
        obj.put(quote(Utils.Type.GC.toString().toUpperCase()), attributes.stream().map(Utils::quote).sorted().collect(Collectors.toList()));
        obj.put(quote("PROPERTIES"), properties.stream().map(Utils::quote).sorted().collect(Collectors.toList()));
        obj.put(quote(Utils.Type.SC.toString().toUpperCase()), clause.stream().map(t -> {
            JSONObject c = new JSONObject();
            c.put(quote(Utils.Type.ATTR.toString().toUpperCase()), quote(t.getLeft()));
            c.put(quote(Utils.Type.COP.toString().toUpperCase()), quote(t.getMiddle()));
            c.put(quote(Utils.Type.VAL.toString().toUpperCase()), t.getRight().stream().map(Utils::quote).collect(Collectors.toList()));
            c.put(quote("SLICE"), t.getMiddle().equals("="));
            c.put(quote("TIME"), t.getLeft().toLowerCase().contains("month") || t.getLeft().toLowerCase().contains("year") || t.getLeft().toLowerCase().contains("date"));
            return c;
        }).collect(Collectors.toList()));
        obj.put(quote(Utils.Type.MC.toString().toUpperCase()), measures.stream().sorted().map(t -> {
            JSONObject c = new JSONObject();
            c.put(quote(Utils.Type.AGG.toString().toUpperCase()), quote("sum"));
            c.put(quote(Utils.Type.MEA.toString().toUpperCase()), quote(t));
            c.put(quote("AS"), quote(t));
            return c;
        }).collect(Collectors.toList()));
        return obj;
    }

    /**
     * @return session step
     */
    public int getSessionStep() {
        return sessionStep;
    }

    /**
     * @return file name
     */
    public String getFilename() {
        if (Intention.DEBUG) {
            return "debug";
        }
        return filename;
//        return "debug";
    }

    @Override
    public Cube getCube() {
        return cube;
    }

    public String getCubeName() {
        return cubeSyn;
    }

    @Override
    public Set<String> getMeasures() {
        return measures;
    }

    @Override
    public Set<String> getAttributes() {
        return attributes;
    }

    @Override
    public Set<Triple<String, String, List<String>>> getClauses() {
        return clause;
    }

    public void setClauses(final Set<Triple<String, String, List<String>>> clauses) {
        this.clause = clauses;
    }

    /**
     * Write a cube to file
     *
     * @param path where to write the cube
     * @throws Exception in case of error
     */
    public long writeMultidimensionalCube(final String path) throws Exception {
        return writeMultidimensionalCube(path, getJSON(), "");
    }

    /**
     * Write a cube to file
     *
     * @param path      where to write
     * @param json      what will be transformed into query
     * @param qualifier file qualifier
     * @throws Exception in case of error
     */
    public long writeMultidimensionalCube(final String path, final JSONObject json, final String qualifier) throws Exception {
        return writeMultidimensionalCube(path, Utils.createQuery(cube, json), qualifier);
    }

    private String replaceQuantifiers(final String s) {
        return s.replace("K", "000")
                .replace("M", "000000")
                .replace("G", "000000000");
    }

    /**
     * Write a cube to file
     *
     * @param path      where to write
     * @param sql       query to execute
     * @param qualifier file qualifier
     */
    public long writeMultidimensionalCube(final String path, final String sql, final String qualifier) {
        L.warn(sql);
        statistics.compute("query_characters", (k, v) -> (v == null ? 0 : (Double) v) + sql.length());
        if (cube.getDbms().equalsIgnoreCase("oracle")) {
            executeDataQuery(cube, "explain plan for " + sql, r -> {
                executeDataQuery(cube, "select plan_table_output from table(dbms_xplan.display())", res -> {
                    int i = 0;
                    String executionPlan = "";
                    while (res.next()) {
                        final String line = res.getString(1);
                        executionPlan += line.trim() + "\n";
                        L.debug(line);
                        if (i++ == 5) {
                            final List<String> result = Arrays.stream(line.split("\\|")).map(String::trim).filter(t -> !t.isEmpty()).collect(Collectors.toList());
                            statistics.compute("query_rows", (k, v) -> (v == null ? 0 : (Double) v) + Double.parseDouble(replaceQuantifiers(result.get(2))));
                            statistics.compute("query_bytes", (k, v) -> (v == null ? 0 : (Double) v) + Double.parseDouble(replaceQuantifiers(result.get(3))));
                            statistics.compute("query_cost", (k, v) -> (v == null ? 0 : (Double) v) + Double.parseDouble(replaceQuantifiers(result.get(4)).split(" ")[0]));
                        }
                    }
                    final String finalExecutionPlan = executionPlan;
                    statistics.compute("executionplan", (k, v) -> (v == null ? "" : v + "\n") + finalExecutionPlan);
                });
            });
        }
        final int sessionStep = getSessionStep();
        final String filename = getFilename();
        final long startTime = System.currentTimeMillis();
        final List<Long> acc = Lists.newLinkedList();
        executeDataQuery(cube, sql, res -> {
            acc.add(System.currentTimeMillis() - startTime);
            final CSVWriter writer = new CSVWriter(new FileWriter(path + filename + "_" + (qualifier.isEmpty() ? "" : qualifier + "_") + sessionStep + ".csv"));
            final ResultSetHelperService resultSetHelperService = new ResultSetHelperService();
            resultSetHelperService.setDateFormat("yyyy-MM-dd");
            resultSetHelperService.setDateTimeFormat("yyyy-MM-dd"); // HH:MI:SS
            writer.setResultService(resultSetHelperService);
            writer.writeAll(res, true);
            writer.close();
        });
        return acc.remove(0);
    }

    public abstract String toPythonCommand(final String commandPath, final String pathToPythonFile);

    /**
     * Compute models for the enhanced cube
     *
     * @param pythonPath   outputPath to python installation (e.g., virtual env with configured libraries)
     * @param outputPath   output path
     * @param pythonModule module to execute
     */
    public long computePython(final String pythonPath, final String outputPath, final String pythonModule) throws IOException, InterruptedException {
        final String commandPath;
        if (new File(pythonPath + "venv/Scripts").exists()) {
            commandPath = pythonPath + "venv/Scripts/python.exe " + pythonPath + pythonModule + " "; //.replace("/", File.separator);
        } else if (new File(pythonPath + "venv/bin").exists()) {
            commandPath = pythonPath + "venv/bin/python " + pythonPath + pythonModule + " ";
        } else {
            commandPath = "python3.6 " + pythonPath + pythonModule + " ";
        }
        long startTime = System.currentTimeMillis();
        final Process proc = Runtime.getRuntime().exec(toPythonCommand(commandPath, outputPath));
        final int ret = proc.waitFor();
        startTime = System.currentTimeMillis() - startTime;

        if (ret != 0) {
            final BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String s;
            String error = "";
            while ((s = stdInput.readLine()) != null) {
                error += s + "\n";
            }
            final BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
            while ((s = stdError.readLine()) != null) {
                error += s + "\n";
            }
            throw new IllegalArgumentException(error);
        }
        return startTime;
    }

    public static String[] toArray(final List<String> row, Object... toAdd) {
        final String[] array = new String[row.size() + toAdd.length];
        int i = 0;
        for (; i < row.size(); i++) {
            array[i] = row.get(i);
        }
        for (; i < array.length; i++) {
            array[i] = toAdd[i - row.size()].toString();
        }
        return array;
    }

    public static JSONObject getPivot(final Intention intention, final int[] currSchema, final List<List<String>> data, final List<String> header) {
        // Sort the columns to get the order of the pivot table
        final List<Pair<Integer, Long>> idx = Lists.newLinkedList();
        final int[] sortedSchema = new int[currSchema.length];
        for (final int i : currSchema) {
            idx.add(Pair.of(i, data.stream().map((List<String> r) -> r.get(i)).distinct().count()));
        }
        idx.sort(Comparator.comparingLong(Pair::getValue));
        final JSONObject headers = new JSONObject();

        int c = 0;
        for (int i = 0; i < idx.size(); i++) {
            final JSONObject dimension = new JSONObject();
            if (i % 2 == 0) {
                sortedSchema[c] = idx.get(i).getKey();
                headers.append("rows", header.get(idx.get(i).getKey()));
            } else {
                sortedSchema[c++ + idx.size() / 2 + idx.size() % 2] = idx.get(i).getKey();
                headers.append("columns", header.get(idx.get(i).getKey()));
            }
            dimension.put("attribute", header.get(idx.get(i).getKey()));
            dimension.put("cardinality", idx.get(i).getValue());
            int finalI = i;
            if (intention.getClauses().stream().noneMatch(t -> t.getLeft().equalsIgnoreCase(header.get(idx.get(finalI).getKey())) && t.getMiddle().equals("="))) {
                headers.append("dimensions", dimension);
            }
        }
        if (!header.contains("rows")) {
            headers.append("rows", new JSONArray());
        }
        List<String> measures = Lists.newArrayList();
        for (String s : header) {
            if (isMeasure(intention, s)) {
                measures.add(s);
            } else if (s.contains("model")) {
                headers.append("models", s);
            }
        }
        headers.put("measures", measures.stream().sorted().collect(Collectors.toList()));
        final Map<List<String>, List<String>> currData = Maps.newLinkedHashMapWithExpectedSize(data.size());
        data.forEach(r -> currData.put(getSortedKey(sortedSchema, r), r));
        final int groupA = sortedSchema.length / 2 + sortedSchema.length % 2;
        final int groupB = sortedSchema.length / 2;

        final Map<List<String>, Long> keyA =
                Streams
                        .mapWithIndex(currData.keySet().stream()
                                        .map(k -> k.subList(0, groupA))
                                        .distinct() //
                                //.sorted((a, b) -> a.toString().compareTo(b.toString()))
                                , Pair::of).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        final Map<List<String>, Long> keyB =
                Streams
                        .mapWithIndex(currData.keySet().stream()
                                        .map(k -> k.subList(groupA, groupA + groupB))
                                        .distinct()
                                //.sorted((a, b) -> a.toString().compareTo(b.toString())),
                                , Pair::of).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        final JSONObject[][] matrix = new JSONObject[keyA.size() + groupB][keyB.size() + groupA];
        currData.forEach((key, value) -> {
            final long row = groupB + keyA.get(key.subList(0, groupA));
            final long col = groupA + keyB.get(key.subList(groupA, groupA + groupB));
            final JSONObject cell = new JSONObject();
            cell.put("type", "cell");
            for (int i = 0; i < value.size(); i++) {
                if (!header.get(i).contains("zscore")
                        && !header.get(i).contains("surprise")
                        && !header.get(i).contains("interest")) {
                    cell.put(header.get(i), value.get(i));
                }
            }
            matrix[(int) row][(int) col] = cell;
        });

        for (final Map.Entry<List<String>, Long> e : keyA.entrySet()) {
            for (int col = 0; col < e.getKey().size(); col++) {
                final long row = e.getValue();
                final JSONObject cell = new JSONObject();
                cell.put("type", "header");
                cell.put("attribute", e.getKey().get((int) col));
                matrix[(int) row + groupB][col] = cell;
            }
        }
        for (final Map.Entry<List<String>, Long> e : keyB.entrySet()) {
            for (int row = 0; row < e.getKey().size(); row++) {
                final long col = e.getValue();
                final JSONObject cell = new JSONObject();
                cell.put("type", "header");
                cell.put("attribute", e.getKey().get((int) row));
                matrix[row][(int) col + groupA] = cell;
            }
        }
        final JSONObject empty = new JSONObject();
        final JSONArray m = new JSONArray();
        for (int i = 0; i < matrix.length; i++) {
            final JSONArray row = new JSONArray();
            for (int j = 0; j < matrix[i].length; j++) {
                row.put(matrix[i][j] == null ? empty : matrix[i][j]);
                matrix[i][j] = null;
            }
            m.put(row);
        }
        final JSONObject res = new JSONObject();
        res.put("table", m);
        res.put("headers", headers);
        return res;
    }

    protected static boolean isMeasure(Intention i, String string) {
        return i.measures.contains(string);
    }

    protected static List<String> getSortedKey(int[] currSchema, final List<String> row) {
        final List<String> key = Lists.newLinkedList();
        for (int i = 0; i < currSchema.length; i++) {
            key.add(row.get(currSchema[i]));
        }
        return key;
    }

    public static JSONArray getRawObject(final String[] header, final List<List<String>> currData) {
        final JSONArray json = new JSONArray();
        for (int i = 0; i < currData.size(); i++) {
            final String[] array = toArray(currData.get(i));
            final JSONObject rowJson = new JSONObject();
            for (int j = 0; j < array.length; j++) {
                rowJson.put(header[j], array[j]);
            }
            json.put(rowJson);
        }
        return json;
    }

    public void setProperties(final Set<String> properties) {
        this.properties = properties;
    }

    public Set<String> getProperties() {
        return properties;
    }
}

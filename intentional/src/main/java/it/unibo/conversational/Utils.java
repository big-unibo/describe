package it.unibo.conversational;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import it.unibo.conversational.database.Cube;
import it.unibo.conversational.database.QueryGenerator;
import it.unibo.conversational.datatypes.Entity;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.RoundingMode;
import java.sql.JDBCType;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * Utility class.
 */
public final class Utils {
    private Utils() {
    }

    /**
     * Decimal formatter.
     */
    public static final DecimalFormat DF = new DecimalFormat();

    static {
        final DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.ENGLISH);
        otherSymbols.setDecimalSeparator('.');
        DF.setMinimumFractionDigits(0);
        DF.setMaximumFractionDigits(2);
        DF.setDecimalFormatSymbols(otherSymbols);
        DF.setRoundingMode(RoundingMode.HALF_DOWN);
        DF.setGroupingUsed(false);
    }

    /**
     * Data type handled by our system.
     */
    public enum DataType {
        /**
         * A number.
         */
        NUMERIC,
        /**
         * A string.
         */
        STRING,
        /**
         * A date.
         */
        DATE,
        /**
         * A MC / SC / GBC clause.
         */
        OTHER
    }

    /**
     * Convert string to DataType type.
     *
     * @param t type in string
     * @return datatype
     */
    public static DataType getDataType(final String t) {
        // mancano: BINARY, VARBINARY, LONGVARBINARY, NULL, OTHER, JAVA_OBJECT, DISTINCT, STRUCT, ARRAY,
        // BLOB, CLOB, REF, DATALINK, BOOLEAN, ROWID, NCLOB, SQLXML, REF_CURSOR, BIGINT
        if (t.equals(JDBCType.CHAR.toString()) || t.equals(JDBCType.LONGVARCHAR.toString()) || t.equals(JDBCType.VARCHAR.toString())
                || t.equals(JDBCType.NCHAR.toString()) || t.equals(JDBCType.NVARCHAR.toString()) || t.equals(JDBCType.LONGNVARCHAR.toString())) {
            return DataType.STRING;
        } else if (t.equals(JDBCType.BIT.toString()) || t.equals(JDBCType.TINYINT.toString()) || t.equals(JDBCType.SMALLINT.toString()) || t.equals(JDBCType.INTEGER.toString())
                || t.equals(JDBCType.FLOAT.toString()) || t.equals(JDBCType.REAL.toString()) || t.equals(JDBCType.DOUBLE.toString())
                || t.equals(JDBCType.NUMERIC.toString()) || t.equals(JDBCType.DECIMAL.toString())) {
            return DataType.NUMERIC;
        } else if (t.equals(JDBCType.DATE.toString()) || t.equals(JDBCType.TIME.toString()) || t.equals(JDBCType.TIMESTAMP.toString())
                || t.equals(JDBCType.TIME_WITH_TIMEZONE.toString()) || t.equals(JDBCType.TIMESTAMP_WITH_TIMEZONE.toString())) {
            return DataType.DATE;
        } else {
            // throw new IllegalArgumentException(t);
            return DataType.OTHER;
        }
    }

    public enum Type {
        DRILL, ROLLUP, SAD, ADD, DROP, REPLACE, ACCESSORY,
        /**
         * Measure clause.
         */
        MC,
        /**
         * Measure.
         */
        MEA,
        /**
         * Fact name.
         */
        FACT,
        /**
         * Hierarchy.
         */
        H,
        /**
         * Measure aggregation.
         */
        AGG,
        /**
         * Group by `by`.
         */
        BY,
        /**
         * Group by `where`.
         */
        WHERE,
        /**
         * Group by clause.
         */
        GC,
        /**
         * Level.
         */
        ATTR,
        /**
         * Comparison operator.
         */
        COP,
        /**
         * Selection clause.
         */
        SC,
        /**
         * Value.
         */
        VAL,
        /**
         * Between operator.
         */
        BETWEEN,
        /**
         * Logical and.
         */
        AND,
        /**
         * Logical or.
         */
        OR,
        /**
         * Logical not.
         */
        NOT,
        /**
         * `Select`.
         */
        SELECT,
        /**
         * Container for not mapped tokens.
         */
        BIN,
        /**
         * Query.
         */
        GPSJ, PARSEFOREST,
        /**
         * Count, count distinct.
         */
        COUNT,
        /**
         * Dummy container for Servlet purpose.
         */
        FOO
    }

    /**
     * Double quote a string
     *
     * @param ss object
     * @return double-quoted string
     */
    public static String quote(final Object ss) {
        final String s = ss.toString();
        if (s.startsWith("\"") || s.endsWith("\"")) {
            throw new IllegalArgumentException("String already begins/ends with quotes " + s);
        }
        if (System.getProperty("os.name").startsWith("Windows")) {
            return "\"" + s + "\"";
        }
        return s;
    }

    /**
     * Remove external quotes in string
     *
     * @param ss string
     * @return string without double quotes
     */
    public static String unquote(final Object ss) {
        final String s = ss.toString();
        if (System.getProperty("os.name").startsWith("Windows")) {
            if (!s.startsWith("\"") || !s.endsWith("\"")) {
                throw new IllegalArgumentException("String does not begin/end with quotes " + s);
            }
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    public static String getFrom(final Cube c, final List<Entity> attributes) {
        Set<String> tabIns = Sets.newHashSet();
        Pair<String, String> ftdet = QueryGenerator.getFactTable(c);
        String from = ftdet.getRight() + " FT";
        for (Entity mde : attributes) {
            final String idT = mde.refToOtherTable();
            if (!tabIns.contains(idT)) {
                Pair<String, String> detTab = QueryGenerator.getTabDetails(c, ftdet.getLeft(), idT);
                // TODO: note that this assume that the attributes name of pk and foreign keys are matching
                from += " INNER JOIN " + detTab.getLeft() + " ON " + detTab.getLeft() + "." + detTab.getRight() + " = FT." + detTab.getRight();
                tabIns.add(idT);
            }
        }
        return from;
    }

    private static String enrichDate(final Cube c, final String attr, final String val) {
        final boolean isDate = attr.toLowerCase().contains("month") || attr.toLowerCase().contains("year") || attr.toLowerCase().contains("date");
        return isDate ? toDate(c, attr, val) : val;
    }

    public static String jsonMeasureToString(final JSONObject mcClause, final boolean rename) {
        final String s;
        if (mcClause.has(quote(Type.AGG.toString()))) {
            final String agg = unquote(mcClause.getString(quote(Type.AGG.toString())));
            if (agg.equalsIgnoreCase("dist")) {
                s = "count(distinct " + unquote(mcClause.getString(quote(Type.MEA.toString()))) + ")";
            } else {
                s = agg + "(" + unquote(mcClause.getString(quote(Type.MEA.toString()))) + ")";
            }
        } else {
            s = unquote(mcClause.getString(quote(Type.MEA.toString())));
        }
        return s + (rename && mcClause.has(Utils.quote("AS")) ? " as " + unquote(mcClause.getString(quote("AS"))) : "");
    }

    /**
     * Cast a Java object into a JSONObject
     * @param obj Java Object
     * @return JSONObject
     */
    private static JSONObject getObject(final Object obj) {
        if (obj instanceof Map) {
            final JSONObject res = new JSONObject();
            ((Map) obj).forEach((k, v) -> {
                if (v instanceof List) {
                    ((List) v).forEach(vv -> res.append(k.toString(), vv));
                } else {
                    res.put(k.toString(), v);
                }
            });
            return res;
        } else {
            return (JSONObject) obj;
        }
    }

    public static void append(JSONObject obj, final Object key, final String value) {
        obj.append(Utils.quote(key.toString()), Utils.quote(value));
    }

    public static boolean has(JSONObject obj, final Object key) {
        return obj.has(Utils.quote(key.toString()));
    }

    public static void append(JSONObject obj, final Object key, final JSONObject value) {
        obj.append(Utils.quote(key.toString()), value);
    }

    public static void append(JSONObject obj, final Object key, final JSONArray value) {
        obj.append(Utils.quote(key.toString()), value);
    }

    /**
     * JSON object to SQL query
     *
     * @param json object to convert
     * @return SQL query
     */
    public static String createQuery(final Cube c, final JSONObject json) {
        // what goes in the group by clause (i.e., attributes + properties)
        final Set<Object> coordinateSet = new HashSet<>();

        // get all the measures
        final JSONArray mc = json.getJSONArray(quote(Type.MC.toString()));

        // get the attributes in the group by, they will also be part of the selection clause
        if (has(json, Type.GC)) {
            json.getJSONArray(quote(Type.GC.toString())).forEach(coordinateSet::add);
        }

        // get the properties, they will also be part of the selection clause
        if (has(json, "PROPERTIES")) {
            json.getJSONArray(quote("PROPERTIES")).forEach(coordinateSet::add);
        }

        // populate the selection clause
        String select = "SELECT " + (has(json, "HINT") ? unquote(json.get(quote("HINT"))) + " " : "");
        final Iterator<Object> mcIterator = mc.iterator();
        while (mcIterator.hasNext()) {
            JSONObject mcClause = getObject(mcIterator.next());
            final String s = jsonMeasureToString(mcClause, true);
            select += s + (mcIterator.hasNext() || !coordinateSet.isEmpty() ? ", " : "");
        }

        final boolean hasNestedQuery = has(json, "FROM");
        // populate the group by clause, if the query has a nested query then it needs to use attribute names without full qualifiers
        // (i.e., use only attributeName and not originaltable.attributeName)
        String groupby = coordinateSet.isEmpty() ? "" : " ";
        final Iterator<Object> gcIterator = coordinateSet.iterator();
        final List<Entity> attributes = Lists.newArrayList();
        while (gcIterator.hasNext()) {
            final Entity attr = QueryGenerator.getLevel(c, unquote(gcIterator.next().toString()));
            attributes.add(attr);
            final String attrString = (hasNestedQuery ? "t1." + attr.nameInTable() : attr.fullQualifier()) + (gcIterator.hasNext() ? ", " : "");
            groupby += attrString;
            select += attrString;
        }

        // populate the where clause
        String where = "";
        if (has(json, Type.SC)) {
            JSONArray sc = json.getJSONArray(quote(Type.SC.toString()));
            final Iterator<Object> scIterator = sc.iterator();
            while (scIterator.hasNext()) {
                final Object tmp = scIterator.next();
                JSONObject scClause = getObject(tmp);
                final Entity attr = QueryGenerator.getLevel(c, unquote(scClause.getString(quote(Type.ATTR))));
                attributes.add(attr);
                final JSONArray values = scClause.getJSONArray(quote(Type.VAL));
                final String value;
                final String attrToAdd = enrichDate(c, attr.nameInTable(), (hasNestedQuery ? "t1." + attr.nameInTable() : attr.fullQualifier()));
                if (unquote(scClause.getString(quote(Type.COP))).equalsIgnoreCase("in")) {
                    value = "(" + values.toList().stream().map(a -> unquote(a.toString())).reduce((a, b) -> a + "," + b).orElse("") + ")";
                } else if (unquote(scClause.getString(quote(Type.COP))).equalsIgnoreCase("between")) {
                    value = unquote(values.getString(0)) + " and " + unquote(values.getString(1));
                } else {
                    value = enrichDate(c, attr.nameInTable(), unquote(values.toList().get(0).toString()));
                }
                where += attrToAdd + " " + unquote(scClause.getString(quote(Type.COP))) + " " + value + (scIterator.hasNext() ? " AND " : "");
            }
        }

        // populate the from clause
        String from = " FROM ";
        if (!hasNestedQuery) {
            from += getFrom(c, attributes);
        } else {
            final LongAdder counter = new LongAdder();
            from += json.getJSONArray(quote("FROM")).toList().stream().map(innerquery -> {
                counter.increment();
                return "(" + Utils.createQuery(c, getObject(innerquery)) + ") t" + counter.intValue();
            }).reduce((a, b) -> a + "," + b).orElse("");
            if (has(json, "JOIN")) {
                where += json.getJSONArray(quote("JOIN")).toList().stream().map(attribute -> "t1." + unquote(attribute) + " = t2." + unquote(attribute)).reduce((a, b) -> a + " and " + b).orElse("");
            }
        }

        // compose the query
        return select
                + from
                + (where.isEmpty() ? "" : " WHERE ") + where
                + (groupby.isEmpty() ? "" : " GROUP BY" + groupby + (has(json, "HAVING") ? " HAVING " + Utils.unquote(json.getString(quote("HAVING"))) : "") + " ORDER BY" + groupby);
    }

    public static String toDate(final Cube cube, final String attribute, final String date) {
        String newDate = date;
        switch (cube.getDbms()) {
            case "mysql":
                if (attribute.toLowerCase().contains("date")) {
                    newDate = date;
                } else if (attribute.toLowerCase().contains("month")) {
                    newDate = "STR_TO_DATE(concat(" + date + ",'-01'),'%Y-%m-%d')";
                } else {
                    newDate = "STR_TO_DATE(concat(\" + date + \",'-01-01'),'%Y-%m-%d')";
                }
                return newDate;
            case "oracle":
                return newDate;
            default:
                throw new IllegalArgumentException(cube.getDbms() + " is not handled");
        }
    }
}

package it.unibo.conversational.database;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import it.unibo.conversational.Procedure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.Set;

/**
 * Handling connection to database.
 */
public final class DBmanager {

    /**
     * Init dbmanager.
     */
    private DBmanager() {
    }

    /**
     * @param s string
     * @return append _id to the string
     */
    protected static String id(final String s) {
        return s + "_id".toUpperCase();
    }

    /**
     * @param s string
     * @return append _name to the string
     */
    protected static String name(final String s) {
        return s + "_name".toUpperCase();
    }

    /**
     * @param s string
     * @return append _type to the string
     */
    protected static String type(final String s) {
        return s + "_type".toUpperCase();
    }

    /**
     * @param s string
     * @return append _synonyms to the string
     */
    protected static String synonyms(final String s) {
        return s + "_synonyms".toUpperCase();
    }

    /**
     * Table types.
     */
    public enum TableTypes {
        /**
         * Fact table.
         */
        FT,
        /**
         * Dimension table.
         */
        DT
    }

    /**
     * Table tabTABLE.
     */
    public static final String tabTABLE = "table".toUpperCase();
    /**
     * Table tabRELATIONSHIP.
     */
    public static final String tabRELATIONSHIP = "relationship".toUpperCase();
    /**
     * Table tabCOLUMN.
     */
    public static final String tabCOLUMN = "column".toUpperCase();
    /**
     * Table tabDATABASE.
     */
    public static final String tabDATABASE = "database".toUpperCase();
    /**
     * Table tabFACT.
     */
    public static final String tabFACT = "fact".toUpperCase();
    /**
     * Table tabHiF.
     */
    public static final String tabHiF = "hierarchy_in_fact".toUpperCase();
    /**
     * Table tabHIERARCHY.
     */
    public static final String tabHIERARCHY = "hierarchy".toUpperCase();
    /**
     * Table tabLEVEL.
     */
    public static final String tabLEVEL = "level".toUpperCase();
    /**
     * Table tabMEMBER.
     */
    public static final String tabMEMBER = "member".toUpperCase();
    /**
     * Table tabMEASURE.
     */
    public static final String tabMEASURE = "measure".toUpperCase();
    /**
     * Table tabGROUPBYOPERATOR.
     */
    public static final String tabGROUPBYOPERATOR = "groupbyoperator".toUpperCase();
    /**
     * Table tabSYNONYM.
     */
    public static final String tabSYNONYM = "synonym".toUpperCase();
    /**
     * Table tabGRBYOPMEASURE.
     */
    public static final String tabGRBYOPMEASURE = "groupbyoperator_of_measure".toUpperCase();
    /**
     * Table tabLANGUAGEPREDICATE.
     */
    public static final String tabLANGUAGEPREDICATE = "language_predicate".toUpperCase();
    /**
     * Table tabLANGUAGEPREDICATE.
     */
    public static final String tabLANGUAGEOPERATOR = "language_operator".toUpperCase();
    /**
     * Table tabLEVELROLLUP.
     */
    public static final String tabLEVELROLLUP = "level_rollup".toUpperCase();
    /**
     * Table tabQuery.
     */
    public static final String tabQuery = "queries".toUpperCase();
    /**
     * Table tabQuery.
     */
    public static final String tabOLAPsession = "OLAPsession".toUpperCase();
    /**
     * List of table containing the synonyms column.
     */
    public static final Set<String> tabsWithSyns = Sets.newHashSet(tabGROUPBYOPERATOR, tabFACT, tabLEVEL, tabMEASURE, tabMEMBER, tabLANGUAGEPREDICATE, tabLANGUAGEOPERATOR);
    public static final String colRELTAB1 = "table1".toUpperCase();
    public static final String colRELTAB2 = "table2".toUpperCase();
    public static final String colCOLISKEY = "isKey".toUpperCase();
    public static final String colDBIP = "IPaddress".toUpperCase();
    public static final String colDBPORT = "port".toUpperCase();
    public static final String colLEVELCARD = "cardinality".toUpperCase();
    public static final String colLEVELMIN = "min".toUpperCase();
    public static final String colLEVELMAX = "max".toUpperCase();
    public static final String colLEVELAVG = "avg".toUpperCase();
    public static final String colLEVELMINDATE = "mindate".toUpperCase();
    public static final String colLEVELMAXDATE = "maxdate".toUpperCase();
    public static final String colLEVELRUSTART = "start".toUpperCase();
    public static final String colLEVELRUTO = "level_to".toUpperCase();
    public static final String colGROUPBYOPNAME = "operator".toUpperCase();
    public static final String colSYNTERM = "term".toUpperCase();
    public static final String colQueryID = "id".toUpperCase();
    public static final String colQueryText = "query".toUpperCase();
    public static final String colQueryGBset = "gc".toUpperCase();
    public static final String colQuerySelClause = "sc".toUpperCase();
    public static final String colQueryMeasClause = "mc".toUpperCase();
    public static final String colQueryGPSJ = "gpsj".toUpperCase();

    private static final Map<Cube, Map<String, Connection>> connectionPool = Maps.newConcurrentMap();
    private static final Logger L = LoggerFactory.getLogger(DBmanager.class);

    public static Connection getConnection(final Cube cube, final boolean isData) throws ClassNotFoundException, SQLException {
        final String ip = cube.getIp();
        final int port = cube.getPort();
        final String username = cube.getUser();
        final String password = cube.getPwd();
        final String schemaDBstringConnection;
        if (cube.getDbms().equals("mysql")) {
            schemaDBstringConnection = "jdbc:" + cube.getDbms() + "://" + ip + ":" + port + "/" + (isData ? cube.getDataMart() : cube.getMetaData()) + "?serverTimezone=UTC&autoReconnect=true";
            Class.forName("com.mysql.cj.jdbc.Driver");
        } else if (cube.getDbms().equals("oracle")) {
            schemaDBstringConnection = "jdbc:oracle:thin:@" + ip + ":" + port + "/" + cube.getDataMart() + "?characterEncoding=utf8";
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } else {
            throw new IllegalArgumentException(cube.getDbms() + " DBMS unknown");
        }
        return DriverManager.getConnection(schemaDBstringConnection, username, password);
    }

    /**
     * Close all database connections.
     */
    public static void closeAllConnections() {
        connectionPool.forEach((k, v) -> v.forEach((kk, c) -> {
            try {
                c.close();
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }));
    }

    private static Connection getMetaConnection(final Cube cube) {
        final Map<String, Connection> pool = connectionPool.computeIfAbsent(cube, c -> Maps.newLinkedHashMap());
        Connection connMetaSchema = pool.get("meta");
        try {
            if (connMetaSchema == null || connMetaSchema.isClosed()) {
                connMetaSchema = getConnection(cube, false);
            }
        } catch (final Exception | Error e) { // connection might have been timed out
            try {
                connMetaSchema = getConnection(cube, false);
            } catch (final Exception e1) {
                e1.printStackTrace();
                connMetaSchema = null;
            }
        }
        pool.put("meta", connMetaSchema);
        return connMetaSchema;
    }

    public static Connection getDataConnection(final Cube cube) {
        final Map<String, Connection> pool = connectionPool.computeIfAbsent(cube, c -> Maps.newLinkedHashMap());
        Connection connDataSchema = pool.get("data");
        try {
            if (connDataSchema == null || connDataSchema.isClosed()) {
                connDataSchema = getConnection(cube, true);
            }
        } catch (final Exception | Error e) { // connection might have been timed out
            try {
                connDataSchema = getConnection(cube, true);
            } catch (final Exception e1) {
                e1.printStackTrace();
                connDataSchema = null;
            }
        }
        pool.put("data", connDataSchema);
        return connDataSchema;
    }

    /**
     * Execute the query and return a result.
     *
     * @param query query to execute
     */
    public static void insertMeta(final Cube cube, final String query, final Procedure<PreparedStatement> continuation) {
        final String sql = fixQuotes(cube, query);
        L.debug(sql);
        try (
                PreparedStatement ps = DBmanager.getMetaConnection(cube).prepareStatement(sql)
        ) {
            continuation.apply(ps);
        } catch (final Throwable e) {
            e.printStackTrace();
            throw new IllegalArgumentException(query + "\n" + e.getMessage());
        }
    }

    /**
     * Execute the query and return a result.
     *
     * @param query query to execute
     */
    public static void executeQuery(final Cube cube, final String query, final boolean isMeta, final Procedure<ResultSet> continuation) {
        final String sql = fixQuotes(cube, query);
        L.debug(sql);
        try (
                Statement stmt = (isMeta ? getMetaConnection(cube) : getDataConnection(cube)).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        ) {
            stmt.setFetchSize(1000000);
            ResultSet res = stmt.executeQuery(sql);
            continuation.apply(res);
            res.close();
        } catch (final Throwable e) {
            e.printStackTrace();
            throw new IllegalArgumentException(query + "\n" + e.getMessage());
        }
    }

    /**
     * Execute the query and return a result.
     *
     * @param query query to execute
     */
    public static void executeMetaQuery(final Cube cube, final String query, final Procedure<ResultSet> continuation) {
        executeQuery(cube, query, true, continuation);
    }

    /**
     * Execute the query and return a result.
     *
     * @param query query to execute
     */
    public static void executeDataQuery(final Cube cube, final String query, final Procedure<ResultSet> continuation) {
        executeQuery(cube, query, false, continuation);
    }

    /**
     * Execute the query and return a result.
     *
     * @param query query to execute
     */
    public static void executeQuery(final Cube cube, final String query) {
        final String sql = fixQuotes(cube, query);
        L.debug(sql);
        try (PreparedStatement pstmt = getMetaConnection(cube).prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.executeUpdate();
        } catch (final SQLException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(query + "\n" + e.getMessage());
        }
    }

    /**
     * Execute the query and return an integer.
     *
     * @param query query to execute
     * @return integer
     */
    public static String executeQueryReturnID(final Cube cube, final String column, final String query) {
        final String sql = fixQuotes(cube, query);
        L.debug(sql);

        try (PreparedStatement pstmt = cube.getDbms().equals("oracle")
                ? getMetaConnection(cube).prepareStatement(sql, new String[]{column})
                : getMetaConnection(cube).prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.executeUpdate();
            final ResultSet generatedKeys = pstmt.getGeneratedKeys();
            generatedKeys.next();
            return generatedKeys.getString(1);
        } catch (final SQLException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(query + "\n" + e.getMessage());
        }
    }

    public static String fixQuotes(final Cube cube, final String sql) {
        switch (cube.getDbms()) {
            case "mysql":
                return sql;
            case "oracle":
                return sql
                        .replace("\"", "'")
                        .replaceAll("`", "\"")
                        .replaceAll(";", "");
            default:
                throw new IllegalArgumentException("Unknown DBMS " + cube.getDbms());
        }
    }
}

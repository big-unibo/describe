package it.unibo.conversational.database;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import it.unibo.conversational.Utils;
import it.unibo.conversational.datatypes.Entity;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static it.unibo.conversational.database.DBmanager.*;

/**
 * Interacting with the database SQL query.
 */
public final class QueryGenerator {

    private QueryGenerator() {
    }

    /**
     * @return get the fact
     */
    public static Pair<String, String> getFactTable(final Cube cube) {
        final List<Pair<String, String>> acc = Lists.newArrayList();
        executeMetaQuery(cube, "SELECT " + id(tabTABLE) + ", " + name(tabTABLE) + " FROM `" + tabTABLE + "` WHERE `" + type(tabTABLE) + "` = \"" + TableTypes.FT + "\"", res -> {
            res.next();
            acc.add(Pair.of(res.getString(id(tabTABLE)), res.getString(name(tabTABLE))));
        });
        return acc.remove(0);
    }

    public static Pair<String, String> getTabDetails(final Cube cube, String idFT, String idTable) {
        final List<Pair<String, String>> acc = Lists.newArrayList();
        executeMetaQuery(cube, "SELECT " + name(tabTABLE) + " FROM `" + tabTABLE + "` WHERE `" + id(tabTABLE) + "` = \"" + idTable + "\"", resDet -> {
            executeMetaQuery(cube, "SELECT " + name(tabCOLUMN) + " FROM `" + tabCOLUMN + "` C INNER JOIN `" + tabRELATIONSHIP + "` R ON C." + id(tabRELATIONSHIP) + " = R." + id(tabRELATIONSHIP) + " WHERE `" + colRELTAB1 + "` = \"" + idFT + "\" AND `" + colRELTAB2 + "` = \"" + idTable + "\"", resCol -> {
                resDet.next();
                resCol.next();
                acc.add(Pair.of(resDet.getString(name(tabTABLE)), resCol.getString(name(tabCOLUMN))));
            });
        });
        return acc.remove(0);
    }


    public static String getTable(final Cube cube, final String... attributes) {
        final List<String> acc = Lists.newArrayList();
        executeMetaQuery(cube, "select distinct table_name " +
                "from `" + tabLEVEL + "` l join `" + tabCOLUMN + "` c on l.level_name = c.column_name join `" + tabTABLE + "` t on c.table_id = t.table_id " +
                "where level_name in (" + Arrays.stream(attributes).map(a -> "'" + (cube.getDbms().equals("mysql")? a : a.toUpperCase()) + "'").reduce((a, b) -> a + "," + b).get() + ")", res -> {
            while (res.next()) {
                final String table = res.getString(name(tabTABLE));
                if (!table.equalsIgnoreCase(cube.getFactTable())) {
                    acc.add(table);
                }
            }
        });
        return acc.remove(0);
    }

    /**
     * Functional dependency
     *
     * @param specific specific attribute
     * @param generic  generic attribute
     * @return Map of <generic value, specific values> entries
     */
    public static Map<String, List<String>> getFunctionalDependency2(final Cube cube, final String specific, final String generic) {
        final Map<String, List<String>> tables = Maps.newLinkedHashMap();
        executeDataQuery(cube, "select distinct " + specific + ", " + generic + " from " + getTable(cube, specific, generic), res -> {
            while (res.next()) {
                if (!tables.containsKey(res.getString(generic))) {
                    tables.put(get(res, 2), Lists.newArrayList(get(res, 1)));
                } else {
                    tables.get(get(res, 2)).add(get(res, 1));
                }
            }
        });
        return tables;
    }

    /**
     * Functional dependency
     *
     * @param specific specific attribute
     * @param generic  generic attribute
     * @return Map of <specific value, generic value> entries
     */
    public static Map<String, String> getFunctionalDependency(final Cube cube, final String specific, final String generic) {
        final Map<String, String> tables = Maps.newLinkedHashMap();
        executeDataQuery(cube, "select distinct " + specific + ", " + generic + " from " + getTable(cube, specific, generic), res -> {
            while (res.next()) {
                if (tables.containsKey(res.getString(specific))) {
                    throw new IllegalArgumentException("Overriding " + res.getString(specific));
                }
                tables.put(get(res, 1), get(res, 2));
            }
        });
        return tables;
    }

    private static String get(ResultSet res, int idx) throws SQLException {
        switch (res.getMetaData().getColumnClassName(idx)) {
            case "java.sql.Timestamp":
                final Date date = new Date();
                date.setTime(res.getTimestamp(idx).getTime());
                return new SimpleDateFormat("yyyy-MM-dd").format(date);
            default:
                return res.getString(idx);
        }
    }

    /**
     * @param level level
     * @return the entity corresponding to the level
     */
    public static Entity getLevel(final Cube cube, String level) {
        return getLevel(cube, level, true);
    }

    /**
     * @param level          level
     * @param throwException whether to throw exception if the level is missing
     * @return the entity corresponding to the level
     */
    public static Entity getLevel(final Cube cube, final String level, final boolean throwException) {
        if (throwException && !getLevels(cube).containsKey(level.toLowerCase())) {
            throw new IllegalArgumentException("Cannot find level " + level);
        }
        return getLevels(cube).get(level.toLowerCase());
    }

    public static Map<String, Entity> getLevels(final Cube cube) {
        final Map<String, Entity> attributes = Maps.newLinkedHashMap();
        executeMetaQuery(cube, "select * from `" + tabLEVEL + "` l, `" + tabCOLUMN + "` c, `" + tabTABLE + "` t where c.table_id = t.table_id and l.column_id = c.column_id", res -> {
            while (res.next()) {
                attributes.put(res.getString(name(tabLEVEL)).toLowerCase(),
                        new Entity(
                                res.getString(id(tabLEVEL)),
                                res.getString(name(tabLEVEL)),
                                res.getString(id(tabTABLE)),
                                res.getString(name(tabCOLUMN)),
                                Utils.getDataType(res.getString(type(tabLEVEL))),
                                tabLEVEL,
                                res.getString(name(tabTABLE))));
            }
        });
        return attributes;
    }

    public static List<String> getLevelsFromMember(final Cube cube, final String member) {
        final List<String> attributes = Lists.newLinkedList();
        executeMetaQuery(cube, "select " + name(tabLEVEL) + " from `" + tabLEVEL + "` l, `" + tabCOLUMN + "` c, `" + tabTABLE + "` t, `" + tabMEMBER + "` m where c.table_id = t.table_id and l.column_id = c.column_id and l.level_id = m.level_id and m." + name(tabMEMBER) + "='" + member + "'", res -> {
            while (res.next()) {
                attributes.add(res.getString(name(tabLEVEL)));
        }});
        return attributes;
    }
}
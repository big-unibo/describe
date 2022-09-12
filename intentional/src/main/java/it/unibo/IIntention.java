package it.unibo;

import it.unibo.conversational.database.Cube;
import org.apache.commons.lang3.tuple.Triple;

import java.util.List;
import java.util.Set;

/**
 * An intentional operator that is applied on a multidimensional cube
 */
public interface IIntention {
    /**
     * @return enhanced cube on which the intention is applied
     */
    Cube getCube();

    /**
     * @return measures on which the intention is applied
     */
    Set<String> getMeasures();

    /**
     * @return attributes on which the intention is applied
     */
    Set<String> getAttributes();

    /**
     * @return clauses on which the intention is applied
     */
    Set<Triple<String, String, List<String>>> getClauses();

    /**
     * @return current session step
     */
    int getSessionStep();
}

package de.kp.core.spade;
/*
 * Class that implements a qualitative abstraction. Two different values are
 * possible: to be with an equal relation with respect to a previous pair (if
 * occurs at the same time), or to be with an after relation with respect to
 * that previous pair (the previous pair have a before relation with respect to
 * this one)
 *
 * Copyright Antonio Gomariz Penalver 2013
 *
 * This file is part of the SPMF DATA MINING SOFTWARE
 * (http://www.philippe-fournier-viger.com/spmf).
 *
 * SPMF is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * SPMF. If not, see <http://www.gnu.org/licenses/>.
 *
 * @author agomariz
 */

import java.util.HashMap;
import java.util.Map;

public class AbstractionQualitative extends AbstractionGeneric {

    public static void clear() {
    }

    /**
     * Abstraction indicating if the item associated has an equal relation with
     * the previous item in the pattern
     */
    private boolean hasEqualRelation;
    private static Map<Boolean, AbstractionQualitative> pool = new HashMap<Boolean, AbstractionQualitative>(4);

    static {
        AbstractionQualitative trueValue = new AbstractionQualitative(true);
        AbstractionQualitative falseValue = new AbstractionQualitative(false);
        pool.put(Boolean.TRUE, trueValue);
        pool.put(Boolean.FALSE, falseValue);
    }

    private AbstractionQualitative(boolean equalRelation) {
        super();
        this.hasEqualRelation = equalRelation;
    }

    /**
     * It creates a new abstraction with the given value.
     * @param hasEqualRelation the value
     * @return the created abstraction
     */
    public static AbstractionQualitative create(boolean hasEqualRelation) {
        return pool.get(hasEqualRelation);
    }

    @Override
    public boolean equals(Object arg) {
        AbstractionQualitative s = (AbstractionQualitative) arg;
        if (hasEqualRelation == s.hasEqualRelation()) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 17 * hash + (this.hasEqualRelation ? 1 : 0);
        return hash;
    }

    @Override
    public int compareTo(AbstractionGeneric o) {
        AbstractionQualitative s = (AbstractionQualitative) o;
        if (hasEqualRelation == s.hasEqualRelation()) {
            return 0;
        } else if (!hasEqualRelation) {
            return -1;
        }

        return 1;
    }

    public boolean hasEqualRelation() {
        return hasEqualRelation;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        if (!hasEqualRelation()) {
            result.append(" ->");
        }
        return result.toString();
    }
    
    /**
     * Get the string representation of this object in SPMF format.
     * @return the string representation
     */
    @Override
    public String toStringToFile() {
        StringBuilder result = new StringBuilder();
        if (!hasEqualRelation()) {
        	/**
        	 * This seems to indicate, that the respective item
        	 * is the first in an itemset
        	 */
            result.append(" -1");
        }
        return result.toString();
    }
}

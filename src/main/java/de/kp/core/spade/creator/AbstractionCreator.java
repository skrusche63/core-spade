package de.kp.core.spade.creator;
/*
 * Abstract class that is thought to make it possible the creation of any kind
 * of abstractions.
 * 
 * Copyright Antonio Gomariz Penalver 2013
 * 
 * This file is part of the SPMF DATA MINING SOFTWARE
 * (http://www.philippe-fournier-viger.com/spmf).
 *
 * SPMF is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SPMF is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with SPMF.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * @author agomariz
 */

import java.util.List;
import java.util.Map;

import de.kp.core.spade.AbstractionGeneric;
import de.kp.core.spade.EquivalenceClass;
import de.kp.core.spade.Item;
import de.kp.core.spade.Pattern;
import de.kp.core.spade.Sequence;

public abstract class AbstractionCreator {

    public abstract AbstractionGeneric createDefaultAbstraction();

    public abstract List<EquivalenceClass> getFrequentSize2Sequences(List<Sequence> sequences, IdListCreator idListCreator);

    public abstract Pattern getSubpattern(Pattern extension, int i);

    public abstract List<EquivalenceClass> getFrequentSize2Sequences(Map<Integer, Map<Item<?>, List<Integer>>> database, Map<Item<?>, EquivalenceClass> frequentItems, IdListCreator idListCreator);

    public abstract void clear();
}

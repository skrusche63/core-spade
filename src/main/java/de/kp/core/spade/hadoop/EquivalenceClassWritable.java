package de.kp.core.spade.hadoop;
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Core-SPADE project
* (https://github.com/skrusche63/core-spade).
* 
* Core-SPADE is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Core-SPADE is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Core-SPADE. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

import de.kp.core.spade.EquivalenceClass;
import de.kp.core.spade.IDList;
import de.kp.core.spade.Pattern;

/**
 * An EquivalenceClass contains Pattern, IDList and list of EquivalenceClasses
 * that represent subordinate members; it implements WritableComparable to
 * read and write instances from Hadoop's HDFS
 */
public class EquivalenceClassWritable implements Writable {

    private PatternWritable patternWritable;    
    private IDListWritable idListWritable;

    /**
     * All the superpatterns of classIdentifier that are obtained by means of 
     * making either an i-extension or s-extension
     */
    private EquivalenceClassListWritable equivClasesWritable;

    public EquivalenceClassWritable() {   
    	
    	this.patternWritable = new PatternWritable();
    	this.equivClasesWritable = new EquivalenceClassListWritable();

    	this.idListWritable = new IDListWritable();
    	
    }
    
    public EquivalenceClassWritable(EquivalenceClass equivClas) {
    	
    	this.patternWritable = new PatternWritable(equivClas.getClassIdentifier());
    	this.equivClasesWritable = new EquivalenceClassListWritable(equivClas.getClassMembers());
    	
    	this.idListWritable = new IDListWritable(equivClas.getIdList());
    	
    }

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		
		patternWritable.readFields(in);
		equivClasesWritable.readFields(in);
		
		idListWritable.readFields(in);
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		
		patternWritable.write(out);
		equivClasesWritable.write(out);
		
		idListWritable.write(out);

	}

	public EquivalenceClass get() {
		
		Pattern pattern = patternWritable.get();
		List<EquivalenceClass> equivClases = equivClasesWritable.get();
		
		IDList idList = idListWritable.get();
		return new EquivalenceClass(pattern, equivClases, idList);
		
	}
}

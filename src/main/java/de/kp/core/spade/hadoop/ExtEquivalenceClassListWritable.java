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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import de.kp.core.spade.EquivalenceClass;

public class ExtEquivalenceClassListWritable implements Writable {

	/**
	 * The minimum support threshold associated with these equivalence classes
	 */
	DoubleWritable minsupp;
	
	/**
	 * The total number of sequences
	 */
	private IntWritable sequences;
	
	/**
	 * The number of equivalence classes
	 */
	private IntWritable size;
	
	/**
	 * The list of equivalence classes
	 */
	private EquivalenceClassListWritable eqClasList;
	
	public ExtEquivalenceClassListWritable() {
		
		this.sequences = new IntWritable();
		this.size = new IntWritable();
		
		this.minsupp = new DoubleWritable();
		this.eqClasList = new EquivalenceClassListWritable();
		
	}
	
	public ExtEquivalenceClassListWritable(Integer sequences, Double minsupp, List<EquivalenceClass> equivClasList) {

    	List<EquivalenceClass> sortedEquivClasList = new ArrayList<EquivalenceClass>(equivClasList);
        Collections.sort(sortedEquivClasList);

		this.sequences = new IntWritable(sequences);
		this.size = new IntWritable(sortedEquivClasList.size());
		
		this.minsupp = new DoubleWritable(minsupp);
		this.eqClasList = new EquivalenceClassListWritable(sortedEquivClasList);
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.sequences.readFields(in);
		this.size.readFields(in);
		
		this.minsupp.readFields(in);
		
		this.eqClasList.readFields(in);
		
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		this.sequences.write(out);
		this.size.write(out);
		
		this.minsupp.write(out);
		this.eqClasList.write(out);
		
	}

	public List<EquivalenceClass> getEqClases() {
		return this.eqClasList.get();
	}
	
	public Integer getSequences() {
		return this.sequences.get();
	}
	
	public Double getMinSupp() {
		return this.minsupp.get();
	}
}

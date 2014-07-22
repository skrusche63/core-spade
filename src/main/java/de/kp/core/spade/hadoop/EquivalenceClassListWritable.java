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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import de.kp.core.spade.EquivalenceClass;

public class EquivalenceClassListWritable implements Writable {

	public ArrayWritable equivClasListWritable;
	
	public EquivalenceClassListWritable() {
		
		List<EquivalenceClassWritable> empty = Collections.<EquivalenceClassWritable> emptyList();		
		this.equivClasListWritable = new ArrayWritable(EquivalenceClassWritable.class, empty.toArray(new Writable[empty.size()]));

	}
	
	public EquivalenceClassListWritable(List<EquivalenceClass> equivClases) {
		
		ArrayList<EquivalenceClassWritable> items = new ArrayList<EquivalenceClassWritable>();
		for (int i=0; i < equivClases.size(); i++) {
			items.add(new EquivalenceClassWritable(equivClases.get(i)));
		}

		this.equivClasListWritable = new ArrayWritable(EquivalenceClassWritable.class, items.toArray(new Writable[items.size()]));

	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		equivClasListWritable.readFields(in);
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		equivClasListWritable.write(out);		
	}
	
	public List<EquivalenceClass> get() {
		
		Writable[] writableArray = equivClasListWritable.get();
		
		ArrayList<EquivalenceClass> list = new ArrayList<EquivalenceClass>();
		for (int i=0; i < writableArray.length; i++) {
			
			EquivalenceClassWritable item = (EquivalenceClassWritable) writableArray[i];			
			list.add(item.get());
			
		}
		
		return list;
		
	}

}

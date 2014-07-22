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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import de.kp.core.spade.Itemset;
import de.kp.core.spade.Sequence;

public class SequenceWritable implements Writable {

    private IntWritable idWritable;
    private ArrayWritable itemsetListWritable;

	public SequenceWritable() {
		
		idWritable = new IntWritable();

		List<ItemsetWritable> empty = Collections.<ItemsetWritable> emptyList();		
		itemsetListWritable = new ArrayWritable(ItemsetWritable.class, empty.toArray(new Writable[empty.size()]));
	    
	}
	
	public SequenceWritable(Sequence sequence) {
		
		idWritable = new IntWritable(sequence.getId());

		List<Itemset> itemsets = sequence.getItemsets();
		ArrayList<ItemsetWritable> itemsetWritableList = new ArrayList<ItemsetWritable>();
		
		for (int i=0; i < itemsets.size(); i++) {
			itemsetWritableList.add(new ItemsetWritable(itemsets.get(i)));
		}

		this.itemsetListWritable = new ArrayWritable(ItemsetWritable.class, itemsetWritableList.toArray(new Writable[itemsetWritableList.size()]));

	}
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		idWritable.readFields(in);
		itemsetListWritable.readFields(in);
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		
		idWritable.write(out);
		itemsetListWritable.write(out);
		
	}
	
	public Sequence get() {
		
		Sequence sequence = new Sequence(idWritable.get());
		
		Writable[] writableArray = itemsetListWritable.get();
		for (int i=0; i < writableArray.length; i++) {
			
			ItemsetWritable itemset = (ItemsetWritable) writableArray[i];			
			sequence.addItemset(itemset.get());
			
		}
		
		return sequence;
		
	}

}

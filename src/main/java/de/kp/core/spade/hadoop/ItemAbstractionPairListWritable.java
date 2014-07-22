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

import de.kp.core.spade.ItemAbstractionPair;

public class ItemAbstractionPairListWritable implements Writable {

	public ArrayWritable itemAbstractionPairListWritable;
	
	public ItemAbstractionPairListWritable() {
		
		List<ItemAbstractionPairWritable> empty = Collections.<ItemAbstractionPairWritable> emptyList();		
		this.itemAbstractionPairListWritable = new ArrayWritable(ItemAbstractionPairWritable.class, empty.toArray(new Writable[empty.size()]));

	}
	
	public ItemAbstractionPairListWritable(List<ItemAbstractionPair> itemAbstractionPairList) {
		
		ArrayList<ItemAbstractionPairWritable> items = new ArrayList<ItemAbstractionPairWritable>();
		for (int i=0; i < itemAbstractionPairList.size(); i++) {
			items.add(new ItemAbstractionPairWritable(itemAbstractionPairList.get(i)));
		}

		this.itemAbstractionPairListWritable = new ArrayWritable(ItemAbstractionPairWritable.class, items.toArray(new Writable[items.size()]));

	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		itemAbstractionPairListWritable.readFields(in);
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		itemAbstractionPairListWritable.write(out);		
	}
	
	public List<ItemAbstractionPair> get() {
		
		Writable[] writableArray = itemAbstractionPairListWritable.get();
		
		ArrayList<ItemAbstractionPair> list = new ArrayList<ItemAbstractionPair>();
		for (int i=0; i < writableArray.length; i++) {
			
			ItemAbstractionPairWritable item = (ItemAbstractionPairWritable) writableArray[i];			
			list.add(item.get());
			
		}
		
		return list;
		
	}

}

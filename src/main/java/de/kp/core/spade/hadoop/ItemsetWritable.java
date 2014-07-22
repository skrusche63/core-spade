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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import de.kp.core.spade.Item;
import de.kp.core.spade.Itemset;

public class ItemsetWritable implements Writable {

	private LongWritable timestampWritable;
	private ArrayWritable itemlistWritable;
	
	public ItemsetWritable() {
		
		timestampWritable = new LongWritable();

		List<ItemWritable> empty = Collections.<ItemWritable> emptyList();		
		itemlistWritable = new ArrayWritable(ItemWritable.class, empty.toArray(new Writable[empty.size()]));

	}
	
	public ItemsetWritable(Itemset itemset) {
		
		timestampWritable = new LongWritable(itemset.getTimestamp());

		List<Item<?>> items = itemset.getItems();
		ArrayList<ItemWritable> itemWritableList = new ArrayList<ItemWritable>();
		
		for (int i=0; i < items.size(); i++) {
			itemWritableList.add(new ItemWritable(items.get(i)));
		}

		this.itemlistWritable = new ArrayWritable(ItemWritable.class, itemWritableList.toArray(new Writable[itemWritableList.size()]));
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		timestampWritable.readFields(in);
		itemlistWritable.readFields(in);		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		timestampWritable.write(out);
		itemlistWritable.write(out);		
	}
	
	public Itemset get() {
		
		Itemset itemset = new Itemset();
		itemset.setTimestamp(timestampWritable.get());
		
		Writable[] writableArray = itemlistWritable.get();
		for (int i=0; i < writableArray.length; i++) {
			
			ItemWritable item = (ItemWritable) writableArray[i];			
			itemset.addItem(item.get());
			
		}
		
		return itemset;

	}

}

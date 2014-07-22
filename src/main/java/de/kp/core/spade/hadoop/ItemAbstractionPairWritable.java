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

import org.apache.hadoop.io.Writable;

import de.kp.core.spade.AbstractionGeneric;
import de.kp.core.spade.Item;
import de.kp.core.spade.ItemAbstractionPair;

public class ItemAbstractionPairWritable implements Writable {

	private ItemWritable itemWritable;
	private AbstractionWritable abstractionWritable;
	
	public ItemAbstractionPairWritable() {		
		
		this.itemWritable = new ItemWritable();
		this.abstractionWritable = new AbstractionWritable();
		
	}
	
	public ItemAbstractionPairWritable(ItemAbstractionPair itemAbstractionPair) {
		
		this.itemWritable = new ItemWritable(itemAbstractionPair.getItem());
		this.abstractionWritable = new AbstractionWritable(itemAbstractionPair.getAbstraction());
		
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		itemWritable.readFields(in);
		abstractionWritable.readFields(in);		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		itemWritable.write(out);
		abstractionWritable.write(out);	
	}

	@SuppressWarnings("rawtypes")
	public ItemAbstractionPair get() {
		
		Item item = itemWritable.get();
		AbstractionGeneric abstraction = abstractionWritable.get();
		
		return new ItemAbstractionPair(item,abstraction);

	}
}

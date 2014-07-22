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
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import de.kp.core.spade.IDList;
import de.kp.core.spade.IDListBitmap;

/**
 * This IDListWritable is actually restricted to IDListBitmap
 * 
 * @author Dr. Stefan Krusche (Dr. Krusche & Partner PartG) 
 *
 */
public class IDListWritable implements Writable {

	private MapWritable mapWritable;
	
	public IDListWritable() {
		mapWritable = new MapWritable();
	}
	
	public IDListWritable(IDList idList) {
		
		mapWritable = new MapWritable();
		
		Map<Integer,BitSet> seqItemsetEntries = ((IDListBitmap)idList).getSeqItemsetEntries();
		for (Map.Entry<Integer, BitSet> entry : seqItemsetEntries.entrySet()) {

			Integer k = entry.getKey();
			BitSet v  = entry.getValue();
			
			mapWritable.put(new IntWritable(k), new BitSetWritable(v));
			
		}
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		mapWritable.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		mapWritable.write(out);
		
	}

	public IDList get() {
		
		Map<Integer,BitSet> seqItemsetEntries = new HashMap<Integer, BitSet>();
		for (Map.Entry<Writable, Writable> entry : mapWritable.entrySet()) {
			
			IntWritable k = (IntWritable)entry.getKey();
			BitSetWritable v = (BitSetWritable)entry.getValue();
			
			seqItemsetEntries.put(k.get(), v.get());
			
		}
		
		return new IDListBitmap(seqItemsetEntries);
	}
	
}

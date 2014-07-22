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
import java.util.List;

import org.apache.hadoop.io.Writable;

import de.kp.core.spade.ItemAbstractionPair;
import de.kp.core.spade.Pattern;

public class PatternWritable implements Writable {

	private BitSetWritable appearingInWritable;
    private ItemAbstractionPairListWritable itemAbstractionPairListWritable;
	
	public PatternWritable() {
		
		this.appearingInWritable = new BitSetWritable();
		this.itemAbstractionPairListWritable = new ItemAbstractionPairListWritable();
	}
	
	public PatternWritable(Pattern pattern) {
		
		this.appearingInWritable = new BitSetWritable(pattern.getAppearingIn());
		this.itemAbstractionPairListWritable = new ItemAbstractionPairListWritable(pattern.getElements());
		
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		appearingInWritable.readFields(in);
		itemAbstractionPairListWritable.readFields(in);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		
		appearingInWritable.write(out);
		itemAbstractionPairListWritable.write(out);
		
	}

	public Pattern get() {
		
		BitSet appearingIn = appearingInWritable.get();
		List<ItemAbstractionPair> elements = itemAbstractionPairListWritable.get();
		
		Pattern pattern = new Pattern(elements);
		pattern.setAppearingIn(appearingIn);
		
		return pattern;
		
	}
}

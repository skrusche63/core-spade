package de.kp.core.spade;
/*
 * This is an implementation of a class implementing the Saver interface. By 
 * means of these lines, the user choose to keep his patterns in the memory.
 * 
 * NOTE: This implementation saves the pattern  to a file as soon 
 * as they are found or can keep the pattern into memory, depending
 * on what the user choose.
 *
 * Copyright Antonio Gomariz Penalver 2013
 *
 * This file is part of the SPMF DATA MINING SOFTWARE
 * (http://www.philippe-fournier-viger.com/spmf).
 *
 * SPMF is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * SPMF. If not, see <http://www.gnu.org/licenses/>.
 *
 * @author agomariz
 */

import java.util.ArrayList;
import java.util.Collection;

public class MemorySaver implements Saver {
    
    private Sequences sequences = null;    
    /**
     * This data structure represents the 'raw' patterns
     * and has been introduced by Dr. Krusche & Partner PartG
     */
    private ArrayList<Pattern> patterns = null;
    
    public MemorySaver(){
    	
    	patterns = new ArrayList<Pattern>();       
    	sequences = new Sequences("FREQUENT SEQUENTIAL PATTERNS");
    }
    
    public MemorySaver(String name){
       sequences = new Sequences(name);
    }
    
    @Override
    public void savePattern(Pattern p) {
    	
    	patterns.add(p);
        sequences.addSequence(p, p.size());
        
    }
    
    @Override
    public void finish() {
       sequences.sort();
    }

    @Override
    public void clear() {
       sequences.clear();
       sequences=null;
    }
    
    @Override
    public String print() {
        return sequences.toStringToFile();
    }
    
    @Override
    public void savePatterns(Collection<Pattern> patterns) {
        for(Pattern pattern:patterns){
            this.savePattern(pattern);
        }
    }
    
    public ArrayList<Pattern> getPatterns() {
    	return this.patterns;
    }

}

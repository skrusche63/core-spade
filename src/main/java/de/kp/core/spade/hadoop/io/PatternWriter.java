package de.kp.core.spade.hadoop.io;
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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import de.kp.core.spade.Pattern;
import de.kp.core.spade.hadoop.PatternWritable;

public class PatternWriter {

    private TupleEntryCollector collector;
    
    public PatternWriter(String output) throws IOException {    	
        this(output, new JobConf());
    }

    @SuppressWarnings("rawtypes")
	public PatternWriter(String output, JobConf jobConf) throws IOException {
    	
    	Tap tap = new Hfs(new SequenceFile(new Fields("pattern")),output, SinkMode.REPLACE);		
	    collector = new HadoopFlowProcess(jobConf).openTapForWrite(tap);
    	
    }

    public void write(List<Pattern> patterns) {

    	Fields fields = new Fields("pattern");
    	Class<?>[] types = new Class<?>[] {PatternWritable.class};

    	for (Pattern pattern : patterns) { 
    		TupleEntry entry = new TupleEntry(fields, new Tuple(new Object[types.length]));			
    		entry.setObject("pattern",new PatternWritable(pattern));

    		collector.add(entry);

    	}

    	collector.close();
    }

}

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
import java.util.ArrayList;
import java.util.Collections;
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
import de.kp.core.spade.EquivalenceClass;
import de.kp.core.spade.hadoop.EquivalenceClassWritable;

/**
 * EquivalenceClassListWriter is a helper class to write a list of 
 * SPADE (frequent sequence mining) equivalence classes to HDFS.
 * 
 * This class is appropriate for first level integration of the SMPF
 * library with Hadoop & Cascading
 * 
 * @author Dr. Stefan Krusche (Dr. Krusche & Partner PartG)
 *
 */
public class EquivalenceClassListWriter {

	public EquivalenceClassListWriter() {		
	}
	
    /**
     * This method writes a list of equivalence classes to HDFS
     * @throws IOException 
     */
    @SuppressWarnings("rawtypes")
	public void write(List<EquivalenceClass> equivClasList, String output, JobConf jobConf) throws IOException {
        
    	List<EquivalenceClass> sortedEquivClasList = new ArrayList<EquivalenceClass>(equivClasList);
        Collections.sort(sortedEquivClasList);

		Tap tap = new Hfs(new SequenceFile(new Fields("eqc")),output, SinkMode.REPLACE);		
	    TupleEntryCollector collector = new HadoopFlowProcess(jobConf).openTapForWrite(tap);

	    for (EquivalenceClass equivClas : sortedEquivClasList) {
	    	
	    	EquivalenceClassWritable equivClasWritable = new EquivalenceClassWritable(equivClas);
	    	
			Fields fields = new Fields("eqc");
		    Class<?>[] types = new Class<?>[] {EquivalenceClassWritable.class};

		    TupleEntry entry = new TupleEntry(fields, new Tuple(new Object[types.length]));			
			entry.setObject("eqc",equivClasWritable);
			
			collector.add(entry);
	    }
	    collector.close();

    }
	
	
}

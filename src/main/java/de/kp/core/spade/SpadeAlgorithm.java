package de.kp.core.spade;
/*
 * This is an implementation of the SPADE. SPADE was proposed by ZAKI in 2001.
 *
 * NOTE: This implementation saves the pattern to a file as soon as they are
 * found or can keep the pattern into memory, depending on what the user choose.
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

import java.io.IOException;
import java.util.*;

public class SpadeAlgorithm {

    public long joinCount; // PFV 2013
    
    /**
     * the minimum support threshold
     */
    protected double minSup;
    /**
     * The minimum support relative threshold, i.e. the minimum number of
     * sequences where the patterns have to be
     */
    protected double minSupRelative;
    /**
     * Flag indicating if we want a depth-first search when true. Otherwise we
     * say that we want a breadth-first search
     */
    protected boolean dfs;
    /**
     * Saver variable to decide where the user want to save the results, if it
     * the case
     */
    Saver saver = null;
    /**
     * Start and end points in order to calculate the overall time taken by the
     * algorithm
     */
    public long start, end;
    /**
     * Number of frequent patterns found by the algorithm
     */
    private int numberOfFrequentPatterns;

    /**
     * Constructor of the class that calls SPADE algorithm.
     *
     * @param support Minimum support (from 0 up to 1)
     * @param dfs Flag for indicating if we want a depth first search. If false,
     * we indicate that we want a breath-first search.
     */
    public SpadeAlgorithm(double support, boolean dfs) {
        this.minSup = support;
        this.dfs = dfs;
    }

    /**
     * Actual call to SPADE algorithm.
     *
     * @param freqEqClases The frequent equivalence classes (1-pattern) we want to
     * search for frequent pattern.
     * @param noSequences
     * @param keepPatterns Flag indicating if we want to keep the output or not
     * @param verbose Flag for debugging purposes
     * @throws IOException
     */
    public void runAlgorithm(List<EquivalenceClass> freqEqClases, int noSequences, boolean keepPatterns, boolean verbose) throws IOException {

    	/**
    	 * Initiate MemorySaver for detected patterns
    	 */
    	saver = new MemorySaver();

		this.minSupRelative = (int) Math.ceil(noSequences * minSup);

		if (this.minSupRelative == 0) {
            this.minSupRelative = 1;
        }

		/**
		 * Reset the stats about memory usage, and 
		 * determine the starting time
		 */
        MemoryLogger.getInstance().reset();
        start = System.currentTimeMillis();

        runSPADE(freqEqClases, (long) minSupRelative, dfs, keepPatterns, verbose);

        /**
         * Register the ending time
         */
        end = System.currentTimeMillis();
        saver.finish();
        
    }

    public ArrayList<Pattern> getResult() {
    	return ((MemorySaver)saver).getPatterns();
    }
    
    /**
     *
     * The actual method for extracting frequent sequences.
     *
     * @param freqEqClases The frequent equivalence classes (1-pattern) we want to
     * search for frequent pattern.
     * @param minSupportCount The minimum relative support
     * @param dfs Flag for indicating if we want a depth first search. If false,
     * we indicate that we want a breath-first search.
     * @param keepPatterns flag indicating if we are interested in keeping the
     * output of the algorithm
     * @param verbose Flag for debugging purposes
     */
    protected void runSPADE(List<EquivalenceClass> freqEqClases, long minSupportCount, boolean dfs, boolean keepPatterns, boolean verbose) {

        /**
         * Extract patterns from frequent equivalence classes
         */
        Collection<Pattern> size1Patterns = getPatterns(freqEqClases);
        /**
         * If we want to keep the output
         */
        if (keepPatterns) {
        	
            for (Pattern size1Pattern : size1Patterns) {
                /**
                 * We keep all the frequent 1-patterns
                 */
                saver.savePattern(size1Pattern);
            }
        
        }
        
        /**
         * Define the root equivalence class, which is the starting point of the SPADE algorithm, 
         * and insert the equivalence classes corresponding to the frequent 1-patterns as its members
         * 
         * This is different to the parallized version (see below), which starts from the frequent 
         * 1-patterns
         */
        EquivalenceClass rootEqClas = new EquivalenceClass(null);
        for (EquivalenceClass freqEqClas : freqEqClases) {
            rootEqClas.addClassMember(freqEqClas);
        }
        /**
         * Inizialitation of the class that is in charge of find the frequent patterns
         */              
        CandidateGenerator candidateGenerator = CandidateGeneratorQualitative.getInstance();                
        FrequentPatternFinder finder = new FrequentPatternFinder(candidateGenerator, minSupRelative, saver);        
        /**
         * We set the number of frequent items to the number of frequent items
         */
        finder.setFrequentPatterns(freqEqClases.size());
        /**
         * We execute the search
         */
        finder.execute(rootEqClas, dfs, keepPatterns, verbose, null, null);

        /**
         * Once we had finished, we keep the number of frequent patterns 
         * that we finally found and check the memory usage for statistics
         */
        numberOfFrequentPatterns = finder.getFrequentPatterns();
        MemoryLogger.getInstance().checkMemory();

		joinCount = FrequentPatternFinder.INTERSECTION_COUNTER;
		
    }


    /**
     * It gets the patterns that are the identifiers of the given equivalence classes
     * @param eqClases The set of equivalence classes from where we want
     * to obtain their class identifiers
     * @return 
     */
    private Collection<Pattern> getPatterns(List<EquivalenceClass> eqClases) {
        
    	ArrayList<Pattern> patterns = new ArrayList<Pattern>();
        
    	for (EquivalenceClass eqClas : eqClases) {
            Pattern frequentPattern = eqClas.getClassIdentifier();
            patterns.add(frequentPattern);
        }
        
        return patterns;
    }

    public String printStatistics() {
        StringBuilder sb = new StringBuilder(200);
        sb.append("=============  Algorithm - STATISTICS =============\n Total time ~ ");
        sb.append(getRunningTime());
        sb.append(" ms\n");
        sb.append(" Frequent sequences count : ");
        sb.append(numberOfFrequentPatterns);
        sb.append('\n');
        sb.append(" Join count : ");
        sb.append(joinCount);
        sb.append('\n');
        sb.append(" Max memory (mb):");
	sb.append(MemoryLogger.getInstance().getMaxMemory());
        sb.append('\n');
        sb.append(saver.print());
        sb.append("\n===================================================\n");
        return sb.toString();
    }

    public int getNumberOfFrequentPatterns() {
        return numberOfFrequentPatterns;
    }

    /**
     * It gets the time spent by the algoritm in its execution.
     * @return 
     */
    public long getRunningTime() {
        return (end - start);
    }

    /**
     * It gets the minimum relative support, i.e. the minimum number of database
     * sequences where a pattern has to appear
     * @return 
     */
    public double getMinSupRelative() {
        return minSupRelative;
    }

}

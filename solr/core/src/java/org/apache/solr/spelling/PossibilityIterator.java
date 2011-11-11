package org.apache.solr.spelling;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import org.apache.lucene.analysis.Token;

/**
 * <p>
 * Given a list of possible Spelling Corrections for multiple mis-spelled words
 * in a query, This iterator returns Possible Correction combinations ordered by
 * reasonable probability that such a combination will return actual hits if
 * re-queried. This implementation simply ranks the Possible Combinations by the
 * sum of their component ranks.
 * </p>
 * 
 */
public class PossibilityIterator implements Iterator<RankedSpellPossibility> {
	private List<List<SpellCheckCorrection>> possibilityList = new ArrayList<List<SpellCheckCorrection>>();
	private Iterator<RankedSpellPossibility> rankedPossibilityIterator = null;
	private int correctionIndex[];
	private boolean done = false;

	@SuppressWarnings("unused")
	private PossibilityIterator() {
		throw new AssertionError("You shan't go here.");
	}

	/**
	 * <p>
	 * We assume here that the passed-in inner LinkedHashMaps are already sorted
	 * in order of "Best Possible Correction".
	 * </p>
	 * 
	 * @param suggestions
	 */
	public PossibilityIterator(Map<Token, LinkedHashMap<String, Integer>> suggestions, int maximumRequiredSuggestions, int maxEvaluations) {
		for (Map.Entry<Token, LinkedHashMap<String, Integer>> entry : suggestions.entrySet()) {
			Token token = entry.getKey();
			if(entry.getValue().size()==0) {
			  continue;
			}
			List<SpellCheckCorrection> possibleCorrections = new ArrayList<SpellCheckCorrection>();
			for (Map.Entry<String, Integer> entry1 : entry.getValue().entrySet()) {
				SpellCheckCorrection correction = new SpellCheckCorrection();
				correction.setOriginal(token);
				correction.setCorrection(entry1.getKey());
				correction.setNumberOfOccurences(entry1.getValue());
				possibleCorrections.add(correction);
			}
			possibilityList.add(possibleCorrections);
		}

		int wrapSize = possibilityList.size();
		if (wrapSize == 0) {
			done = true;
		} else {
			correctionIndex = new int[wrapSize];
			for (int i = 0; i < wrapSize; i++) {
				int suggestSize = possibilityList.get(i).size();
				if (suggestSize == 0) {
					done = true;
					break;
				}
				correctionIndex[i] = 0;
			}
		}
		
		long count = 0;
		PriorityQueue<RankedSpellPossibility> rankedPossibilities = new PriorityQueue<RankedSpellPossibility>();		
		while (count < maxEvaluations && internalHasNext()) {
			RankedSpellPossibility rsp = internalNext();
			count++;			
			
			if(rankedPossibilities.size() >= maximumRequiredSuggestions && rsp.getRank() >= rankedPossibilities.peek().getRank()) {
				continue;
			}
			rankedPossibilities.offer(rsp);
			if(rankedPossibilities.size() > maximumRequiredSuggestions) {
				rankedPossibilities.poll();
			}
		}
		
		RankedSpellPossibility[] rpArr = new RankedSpellPossibility[rankedPossibilities.size()];
		for(int i=rankedPossibilities.size() - 1  ; i>=0 ; i--) {
			rpArr[i] = rankedPossibilities.remove();
		}
		rankedPossibilityIterator = Arrays.asList(rpArr).iterator();		
	}

	private boolean internalHasNext() {
		return !done;
	}

	/**
	 * <p>
	 * This method is converting the independent LinkHashMaps containing various
	 * (silo'ed) suggestions for each mis-spelled word into individual
	 * "holistic query corrections", aka. "Spell Check Possibility"
	 * </p>
	 * <p>
	 * Rank here is the sum of each selected term's position in its respective
	 * LinkedHashMap.
	 * </p>
	 * 
	 * @return
	 */
	private RankedSpellPossibility internalNext() {
		if (done) {
			throw new NoSuchElementException();
		}

		List<SpellCheckCorrection> possibleCorrection = new ArrayList<SpellCheckCorrection>();
		int rank = 0;
		for (int i = 0; i < correctionIndex.length; i++) {
			List<SpellCheckCorrection> singleWordPossibilities = possibilityList.get(i);
			SpellCheckCorrection singleWordPossibility = singleWordPossibilities.get(correctionIndex[i]);
			rank += correctionIndex[i];

			if (i == correctionIndex.length - 1) {
				correctionIndex[i]++;
				if (correctionIndex[i] == singleWordPossibilities.size()) {
					correctionIndex[i] = 0;
					if (correctionIndex.length == 1) {
						done = true;
					}
					for (int ii = i - 1; ii >= 0; ii--) {
						correctionIndex[ii]++;
						if (correctionIndex[ii] >= possibilityList.get(ii).size() && ii > 0) {
							correctionIndex[ii] = 0;
						} else {
							break;
						}
					}
				}
			}
			possibleCorrection.add(singleWordPossibility);
		}
		
		if(correctionIndex[0] == possibilityList.get(0).size())
		{
			done = true;
		}

		RankedSpellPossibility rsl = new RankedSpellPossibility();
		rsl.setCorrections(possibleCorrection);
		rsl.setRank(rank);
		return rsl;
	}

	public boolean hasNext() {
		return rankedPossibilityIterator.hasNext();
	}

	public RankedSpellPossibility next() {
		return rankedPossibilityIterator.next();
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

}

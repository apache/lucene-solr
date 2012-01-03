package org.apache.lucene.analysis.kuromoji.viterbi;

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

import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.kuromoji.Tokenizer.Mode;
import org.apache.lucene.analysis.kuromoji.dict.CharacterDefinition;
import org.apache.lucene.analysis.kuromoji.dict.ConnectionCosts;
import org.apache.lucene.analysis.kuromoji.dict.TokenInfoDictionary;
import org.apache.lucene.analysis.kuromoji.dict.UnknownDictionary;
import org.apache.lucene.analysis.kuromoji.dict.UserDictionary;
import org.apache.lucene.analysis.kuromoji.dict.CharacterDefinition.CharacterClass;
import org.apache.lucene.analysis.kuromoji.trie.DoubleArrayTrie;
import org.apache.lucene.analysis.kuromoji.viterbi.ViterbiNode.Type;

public class Viterbi {

	private final DoubleArrayTrie trie;
	
	private final TokenInfoDictionary dictionary;
	
	private final UnknownDictionary unkDictionary;
	
	private final ConnectionCosts costs;
	
	private final UserDictionary userDictionary;
	
	private final CharacterDefinition characterDefinition;
	
	private final boolean useUserDictionary;

	private final boolean searchMode;
	
	private final boolean extendedMode;
	
	private static final int DEFAULT_COST = 10000000;

	private static final int SEARCH_MODE_LENGTH_KANJI = 3;

	private static final int SEARCH_MODE_LENGTH = 7;

	private static final int SEARCH_MODE_PENALTY = 10000;
		
	private static final String BOS = "BOS";
	
	private static final String EOS = "EOS";

	/**
	 * Constructor
	 * @param trie
	 * @param targetMap
	 * @param dictionary
	 * @param unkDictionary
	 * @param costs
	 * @param userDictionary
	 */
	public Viterbi(DoubleArrayTrie trie,
				   TokenInfoDictionary dictionary,
				   UnknownDictionary unkDictionary,
				   ConnectionCosts costs,
				   UserDictionary userDictionary,
				   Mode mode) {
		this.trie = trie;
		this.dictionary = dictionary;
		this.unkDictionary = unkDictionary;
		this.costs = costs;
		this.userDictionary = userDictionary;
		if(userDictionary == null) {
			this.useUserDictionary = false;
		} else {
			this.useUserDictionary = true;
		}

		switch(mode){
		case SEARCH:
			searchMode = true;
			extendedMode = false;
			break;
		case EXTENDED:
			searchMode = true;
			extendedMode = true;
			break;
		default:
			searchMode = false;
			extendedMode = false;
			break;
		}

		this.characterDefinition = unkDictionary.getCharacterDefinition();
	}

	/**
	 * Find best path from input lattice.
	 * @param lattice the result of build method
	 * @return	List of ViterbiNode which consist best path 
	 */
	public List<ViterbiNode> search(ViterbiNode[][][] lattice) {
		ViterbiNode[][] startIndexArr = lattice[0];
		ViterbiNode[][] endIndexArr = lattice[1];
		
		for (int i = 1; i < startIndexArr.length; i++){

			if (startIndexArr[i] == null || endIndexArr[i] == null){	// continue since no array which contains ViterbiNodes exists. Or no previous node exists.
				continue;
			}

			for (ViterbiNode node : startIndexArr[i]) {
				if (node == null){	// If array doesn't contain ViterbiNode any more, continue to next index
					break;
				}

				int backwardConnectionId = node.getLeftId();
				int wordCost = node.getWordCost();
				int leastPathCost = DEFAULT_COST;
				for (ViterbiNode leftNode : endIndexArr[i]) {
					if (leftNode == null){ // If array doesn't contain ViterbiNode any more, continue to next index
						break;
					}
					
					int pathCost = leftNode.getPathCost() + costs.get(leftNode.getRightId(), backwardConnectionId) + wordCost;	// cost = [total cost from BOS to previous node] + [connection cost between previous node and current node] + [word cost]

					// "Search mode". Add extra costs if it is long node.
					if (searchMode) {
//						System.out.print(""); // If this line exists, kuromoji runs faster for some reason when searchMode == false.
						String surfaceForm = node.getSurfaceForm();
						int length = surfaceForm.length();
						if (length > SEARCH_MODE_LENGTH_KANJI) {
							boolean allKanji = true;
							// check if node consists of only kanji
							for (int pos = 0; pos < length; pos++) {
								if (!characterDefinition.isKanji(surfaceForm.charAt(pos))){
									allKanji = false;
									break;
								}				
							}
							
							if (allKanji) {	// Process only Kanji keywords
								pathCost += (length - SEARCH_MODE_LENGTH_KANJI) * SEARCH_MODE_PENALTY;
							} else if (length > SEARCH_MODE_LENGTH) {
								pathCost += (length - SEARCH_MODE_LENGTH) * SEARCH_MODE_PENALTY;								
							}
						}
					}
					
					if (pathCost < leastPathCost){	// If total cost is lower than before, set current previous node as best left node (previous means left).
						leastPathCost = pathCost;
						node.setPathCost(leastPathCost);
						node.setLeftNode(leftNode);
					}					
				}
			}
		}

		// track best path
		ViterbiNode node = endIndexArr[0][0];	// EOS
		LinkedList<ViterbiNode> result = new LinkedList<ViterbiNode>();
		result.add(node);
		while (true) {
			ViterbiNode leftNode = node.getLeftNode();
			if (leftNode == null) {
				break;
			}
			
			// EXTENDED mode convert unknown word into unigram node
			if (extendedMode && leftNode.getType() == Type.UNKNOWN) {
				int unigramWordId = CharacterClass.NGRAM.getId();
				int unigramLeftId = unkDictionary.getLeftId(unigramWordId); // isn't required
				int unigramRightId = unkDictionary.getLeftId(unigramWordId); // isn't required
				int unigramWordCost = unkDictionary.getWordCost(unigramWordId); // isn't required
				String surfaceForm = leftNode.getSurfaceForm();
				for (int i = surfaceForm.length(); i > 0; i--) {
					ViterbiNode uniGramNode = new ViterbiNode(unigramWordId, surfaceForm.substring(i - 1, i), unigramLeftId, unigramRightId, unigramWordCost, leftNode.getStartIndex() + i - 1, Type.UNKNOWN);
					result.addFirst(uniGramNode);
				}
			} else {
				result.addFirst(leftNode);		
			}
			node = leftNode;
		}
		
		return result;
	}
	

	/**
	 * Build lattice from input text
	 * @param text
	 * @return
	 */
	public ViterbiNode[][][] build(String text) {
		int textLength = text.length();
		ViterbiNode[][] startIndexArr = new ViterbiNode[textLength + 2][];  // text length + BOS and EOS
		ViterbiNode[][] endIndexArr = new ViterbiNode[textLength + 2][];  // text length + BOS and EOS
		int[] startSizeArr = new int[textLength + 2]; // array to keep ViterbiNode count in startIndexArr
		int[] endSizeArr = new int[textLength + 2];   // array to keep ViterbiNode count in endIndexArr
		
		ViterbiNode bosNode = new ViterbiNode(0, BOS, 0, 0, 0, -1, Type.KNOWN);
		addToArrays(bosNode, 0, 1, startIndexArr, endIndexArr, startSizeArr, endSizeArr);

		// Process user dictionary;
		if (useUserDictionary) {
			processUserDictionary(text, startIndexArr, endIndexArr, startSizeArr, endSizeArr);
		}
		
		int unknownWordEndIndex = -1;	// index of the last character of unknown word

		for (int startIndex = 0; startIndex < textLength; startIndex++) {
			// If no token ends where current token starts, skip this index
			if (endSizeArr[startIndex + 1] == 0) {
				continue;
			}
			
			String suffix = text.substring(startIndex);

			boolean found = false;
			for (int endIndex = 1; endIndex < suffix.length() + 1; endIndex++) {
				String prefix = suffix.substring(0, endIndex);
				
				int result = trie.lookup(prefix);

				if (result > 0) {	// Found match in double array trie
					found = true;	// Don't produce unknown word starting from this index
					for (int wordId : dictionary.lookupWordIds(result)) {
						ViterbiNode node = new ViterbiNode(wordId, prefix, dictionary.getLeftId(wordId), dictionary.getRightId(wordId), dictionary.getWordCost(wordId), startIndex, Type.KNOWN);
						addToArrays(node, startIndex + 1, startIndex + 1 + endIndex, startIndexArr, endIndexArr, startSizeArr, endSizeArr);
					}
				} else if(result < 0) {	// If result is less than zero, continue to next position
						break;						
				}
			}

			// In the case of normal mode, it doesn't process unknown word greedily.
			if(!searchMode && unknownWordEndIndex > startIndex){
				continue;
			}
			
			// Process Unknown Word
			int unknownWordLength = 0;
			char firstCharacter = suffix.charAt(0);
			boolean isInvoke = characterDefinition.isInvoke(firstCharacter);
			if (isInvoke){	// Process "invoke"
				unknownWordLength = unkDictionary.lookup(suffix);
			} else if (found == false){	// Process not "invoke"
				unknownWordLength = unkDictionary.lookup(suffix);				
			}
			
			if (unknownWordLength > 0) {      // found unknown word
				String unkWord = suffix.substring(0, unknownWordLength);
				int characterId = characterDefinition.lookup(firstCharacter);
				int[] wordIds = unkDictionary.lookupWordIds(characterId); // characters in input text are supposed to be the same
				
				for (int wordId : wordIds) {
					ViterbiNode node = new ViterbiNode(wordId, unkWord, unkDictionary.getLeftId(wordId), unkDictionary.getRightId(wordId), unkDictionary.getWordCost(wordId), startIndex, Type.UNKNOWN);
					addToArrays(node, startIndex + 1, startIndex + 1 + unknownWordLength, startIndexArr, endIndexArr, startSizeArr, endSizeArr);
				}
				unknownWordEndIndex = startIndex + unknownWordLength;
			}
		}
		
		ViterbiNode eosNode = new ViterbiNode(0, EOS, 0, 0, 0, textLength + 1, Type.KNOWN);
		addToArrays(eosNode, textLength + 1, 0, startIndexArr, endIndexArr, startSizeArr, endSizeArr); //Add EOS node to endIndexArr at index 0
		
		ViterbiNode[][][] result = new ViterbiNode[][][]{startIndexArr, endIndexArr};
		
		return result;
	}

	/**
	 * Find token(s) in input text and set found token(s) in arrays as normal tokens
	 * @param text	
	 * @param startIndexArr
	 * @param endIndexArr
	 * @param startSizeArr
	 * @param endSizeArr
	 */
	private void processUserDictionary(String text, ViterbiNode[][] startIndexArr, ViterbiNode[][] endIndexArr, int[] startSizeArr, int[] endSizeArr) {
		int[][] result = userDictionary.lookup(text);
		for(int[] segmentation : result) {
			int wordId = segmentation[0];
			int index = segmentation[1];
			int length = segmentation[2];
			ViterbiNode node = new ViterbiNode(wordId, text.substring(index, index + length), userDictionary.getLeftId(wordId), userDictionary.getRightId(wordId), userDictionary.getWordCost(wordId), index, Type.USER);
			addToArrays(node, index + 1, index + 1 + length, startIndexArr, endIndexArr, startSizeArr, endSizeArr);
		}
	}
	
	/**
	 * Add node to arrays and increment count in size array
	 * @param node
	 * @param startIndex
	 * @param endIndex
	 * @param startIndexArr
	 * @param endIndexArr
	 * @param startSizeArr
	 * @param endSizeArr
	 */
	private void addToArrays(ViterbiNode node, int startIndex, int endIndex, ViterbiNode[][] startIndexArr, ViterbiNode[][] endIndexArr, int[] startSizeArr, int[] endSizeArr ) {
		int startNodesCount = startSizeArr[startIndex];
		int endNodesCount = endSizeArr[endIndex];

		if (startNodesCount == 0) {
			startIndexArr[startIndex] = new ViterbiNode[10];
		}

		if (endNodesCount == 0) {
			endIndexArr[endIndex] = new ViterbiNode[10];
		}

		if (startIndexArr[startIndex].length <= startNodesCount){
			startIndexArr[startIndex] = extendArray(startIndexArr[startIndex]);
		}
		
		if (endIndexArr[endIndex].length <= endNodesCount){
			endIndexArr[endIndex] = extendArray(endIndexArr[endIndex]);
		}
				
		startIndexArr[startIndex][startNodesCount] = node;
		endIndexArr[endIndex][endNodesCount] = node;
		
		startSizeArr[startIndex] = startNodesCount + 1;
		endSizeArr[endIndex] = endNodesCount + 1;
	}
	

	/**
	 * Return twice as big array which contains value of input array
	 * @param array
	 * @return
	 */
	private ViterbiNode[] extendArray(ViterbiNode[] array) {
		//extend array
		ViterbiNode[] newArray = new ViterbiNode[array.length * 2];
		System.arraycopy(array, 0, newArray, 0, array.length);
		return newArray;
	}
}

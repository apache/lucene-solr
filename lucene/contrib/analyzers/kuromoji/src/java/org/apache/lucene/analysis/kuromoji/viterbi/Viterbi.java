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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.kuromoji.Segmenter.Mode;
import org.apache.lucene.analysis.kuromoji.dict.CharacterDefinition;
import org.apache.lucene.analysis.kuromoji.dict.ConnectionCosts;
import org.apache.lucene.analysis.kuromoji.dict.TokenInfoDictionary;
import org.apache.lucene.analysis.kuromoji.dict.TokenInfoFST;
import org.apache.lucene.analysis.kuromoji.dict.UnknownDictionary;
import org.apache.lucene.analysis.kuromoji.dict.UserDictionary;
import org.apache.lucene.analysis.kuromoji.viterbi.ViterbiNode.Type;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.FST;

public class Viterbi {
  
  private final TokenInfoFST fst;

  private final TokenInfoDictionary dictionary;
  
  private final UnknownDictionary unkDictionary;
  
  private final ConnectionCosts costs;
  
  private final UserDictionary userDictionary;
  
  private final CharacterDefinition characterDefinition;
  
  private final boolean useUserDictionary;
  
  private final boolean searchMode;
  
  private final boolean extendedMode;
  
  private static final int DEFAULT_COST = 10000000;
  
  private static final int SEARCH_MODE_KANJI_LENGTH = 2;

  private static final int SEARCH_MODE_OTHER_LENGTH = 7; // Must be >= SEARCH_MODE_KANJI_LENGTH

  private static final int SEARCH_MODE_KANJI_PENALTY = 3000;

  private static final int SEARCH_MODE_OTHER_PENALTY = 1700;
  
  private static final char[] BOS = "BOS".toCharArray();
  
  private static final char[] EOS = "EOS".toCharArray();
  
  /**
   * Constructor
   */
  public Viterbi(TokenInfoDictionary dictionary,
      UnknownDictionary unkDictionary,
      ConnectionCosts costs,
      UserDictionary userDictionary,
      Mode mode) {
    this.dictionary = dictionary;
    this.fst = dictionary.getFST();
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
            char[] surfaceForm = node.getSurfaceForm();
            int offset = node.getOffset();
            int length = node.getLength();
            if (length > SEARCH_MODE_KANJI_LENGTH) {
              boolean allKanji = true;
              // check if node consists of only kanji
              for (int pos = 0; pos < length; pos++) {
                if (!characterDefinition.isKanji(surfaceForm[offset+pos])){
                  allKanji = false;
                  break;
                }				
              }
              
              if (allKanji) {	// Process only Kanji keywords
                pathCost += (length - SEARCH_MODE_KANJI_LENGTH) * SEARCH_MODE_KANJI_PENALTY;
              } else if (length > SEARCH_MODE_OTHER_LENGTH) {
                pathCost += (length - SEARCH_MODE_OTHER_LENGTH) * SEARCH_MODE_OTHER_PENALTY;								
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
        byte unigramWordId = CharacterDefinition.NGRAM;
        int unigramLeftId = unkDictionary.getLeftId(unigramWordId); // isn't required
        int unigramRightId = unkDictionary.getLeftId(unigramWordId); // isn't required
        int unigramWordCost = unkDictionary.getWordCost(unigramWordId); // isn't required
        char[] surfaceForm = leftNode.getSurfaceForm();
        int offset = leftNode.getOffset();
        int length = leftNode.getLength();
        for (int i = length - 1; i >= 0; i--) {
          int charLen = 1;
          if (i > 0 && Character.isLowSurrogate(surfaceForm[offset+i])) {
            i--;
            charLen = 2;
          }
          ViterbiNode uniGramNode = new ViterbiNode(unigramWordId, surfaceForm, offset + i, charLen, unigramLeftId, unigramRightId, unigramWordCost, leftNode.getStartIndex() + i, Type.UNKNOWN);
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
   */
  public ViterbiNode[][][] build(char text[], int offset, int length) throws IOException {
    ViterbiNode[][] startIndexArr = new ViterbiNode[length + 2][];  // text length + BOS and EOS
    ViterbiNode[][] endIndexArr = new ViterbiNode[length + 2][];  // text length + BOS and EOS
    int[] startSizeArr = new int[length + 2]; // array to keep ViterbiNode count in startIndexArr
    int[] endSizeArr = new int[length + 2];   // array to keep ViterbiNode count in endIndexArr
    FST.Arc<Long> arc = new FST.Arc<Long>();
    ViterbiNode bosNode = new ViterbiNode(-1, BOS, 0, BOS.length, 0, 0, 0, -1, Type.KNOWN);
    addToArrays(bosNode, 0, 1, startIndexArr, endIndexArr, startSizeArr, endSizeArr);
    
    final FST.BytesReader fstReader = fst.getBytesReader(0);

    // Process user dictionary;
    if (useUserDictionary) {
      processUserDictionary(text, offset, length, startIndexArr, endIndexArr, startSizeArr, endSizeArr);
    }
    
    int unknownWordEndIndex = -1;	// index of the last character of unknown word
    
    final IntsRef wordIdRef = new IntsRef();
    
    for (int startIndex = 0; startIndex < length; startIndex++) {
      // If no token ends where current token starts, skip this index
      if (endSizeArr[startIndex + 1] == 0) {
        continue;
      }
      
      int suffixStart = offset + startIndex;
      int suffixLength = length - startIndex;
      
      boolean found = false;
      arc = fst.getFirstArc(arc);
      int output = 0;
      for (int endIndex = 1; endIndex < suffixLength + 1; endIndex++) {
        int ch = text[suffixStart + endIndex - 1];
        
        if (fst.findTargetArc(ch, arc, arc, endIndex == 1, fstReader) == null) {
          break; // continue to next position
        }
        output += arc.output.intValue();

        if (arc.isFinal()) {
          output += arc.nextFinalOutput.intValue();
          found = true; // Don't produce unknown word starting from this index
          dictionary.lookupWordIds(output, wordIdRef);
          for (int ofs = 0; ofs < wordIdRef.length; ofs++) {
            final int wordId = wordIdRef.ints[wordIdRef.offset + ofs];
            ViterbiNode node = new ViterbiNode(wordId, text, suffixStart, endIndex, dictionary.getLeftId(wordId), dictionary.getRightId(wordId), dictionary.getWordCost(wordId), startIndex, Type.KNOWN);
            addToArrays(node, startIndex + 1, startIndex + 1 + endIndex, startIndexArr, endIndexArr, startSizeArr, endSizeArr);
          }
        }
      }
      
      // In the case of normal mode, it doesn't process unknown word greedily.
      if(!searchMode && unknownWordEndIndex > startIndex){
        continue;
      }
      
      // Process Unknown Word: hmm what is this isInvoke logic (same no matter what)
      int unknownWordLength = 0;
      char firstCharacter = text[suffixStart];
      boolean isInvoke = characterDefinition.isInvoke(firstCharacter);
      if (isInvoke){	// Process "invoke"
        unknownWordLength = unkDictionary.lookup(text, suffixStart, suffixLength);
      } else if (found == false){	// Process not "invoke"
        unknownWordLength = unkDictionary.lookup(text, suffixStart, suffixLength);				
      }
      
      if (unknownWordLength > 0) {      // found unknown word
        final int characterId = characterDefinition.getCharacterClass(firstCharacter);
        unkDictionary.lookupWordIds(characterId, wordIdRef); // characters in input text are supposed to be the same
        for (int ofs = 0; ofs < wordIdRef.length; ofs++) {
          final int wordId = wordIdRef.ints[wordIdRef.offset + ofs];
          ViterbiNode node = new ViterbiNode(wordId, text, suffixStart, unknownWordLength, unkDictionary.getLeftId(wordId), unkDictionary.getRightId(wordId), unkDictionary.getWordCost(wordId), startIndex, Type.UNKNOWN);
          addToArrays(node, startIndex + 1, startIndex + 1 + unknownWordLength, startIndexArr, endIndexArr, startSizeArr, endSizeArr);
        }
        unknownWordEndIndex = startIndex + unknownWordLength;
      }
    }
    
    ViterbiNode eosNode = new ViterbiNode(-1, EOS, 0, EOS.length, 0, 0, 0, length + 1, Type.KNOWN);
    addToArrays(eosNode, length + 1, 0, startIndexArr, endIndexArr, startSizeArr, endSizeArr); //Add EOS node to endIndexArr at index 0
    
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
  private void processUserDictionary(char text[], int offset, int len, ViterbiNode[][] startIndexArr, ViterbiNode[][] endIndexArr, int[] startSizeArr, int[] endSizeArr) throws IOException {
    int[][] result = userDictionary.lookup(text, offset, len);
    for(int[] segmentation : result) {
      int wordId = segmentation[0];
      int index = segmentation[1];
      int length = segmentation[2];
      ViterbiNode node = new ViterbiNode(wordId, text, offset + index, length, userDictionary.getLeftId(wordId), userDictionary.getRightId(wordId), userDictionary.getWordCost(wordId), index, Type.USER);
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

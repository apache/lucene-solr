package org.apache.lucene.analysis.kuromoji;

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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

import org.apache.lucene.analysis.kuromoji.dict.ConnectionCosts;
import org.apache.lucene.analysis.kuromoji.dict.Dictionary;
import org.apache.lucene.analysis.kuromoji.dict.TokenInfoDictionary;
import org.apache.lucene.analysis.kuromoji.dict.UnknownDictionary;
import org.apache.lucene.analysis.kuromoji.dict.UserDictionary;
import org.apache.lucene.analysis.kuromoji.viterbi.GraphvizFormatter;
import org.apache.lucene.analysis.kuromoji.viterbi.Viterbi;
import org.apache.lucene.analysis.kuromoji.viterbi.ViterbiNode;
import org.apache.lucene.analysis.kuromoji.viterbi.ViterbiNode.Type;

/**
 * Tokenizer main class.
 * Thread safe.
 */
public class Segmenter {
  public static enum Mode {
    NORMAL, SEARCH, EXTENDED
  }
  
  public static final Mode DEFAULT_MODE = Mode.SEARCH;
  
  private final Viterbi viterbi;
  
  private final EnumMap<Type, Dictionary> dictionaryMap = new EnumMap<Type, Dictionary>(Type.class);
  
  private final boolean split;
  
  public Segmenter() {
    this(null, DEFAULT_MODE, false);
  }

  public Segmenter(Mode mode) {
    this(null, mode, false);
  }

  public Segmenter(UserDictionary userDictionary) {
    this(userDictionary, DEFAULT_MODE, false);
  }

  public Segmenter(UserDictionary userDictionary, Mode mode) {
    this(userDictionary, mode, false);
  }

  public Segmenter(UserDictionary userDictionary, Mode mode, boolean split) {
    final TokenInfoDictionary dict = TokenInfoDictionary.getInstance();
    final UnknownDictionary unknownDict = UnknownDictionary.getInstance();
    this.viterbi = new Viterbi(dict, unknownDict, ConnectionCosts.getInstance(), userDictionary, mode);
    this.split = split;
    
    dictionaryMap.put(Type.KNOWN, dict);
    dictionaryMap.put(Type.UNKNOWN, unknownDict);
    dictionaryMap.put(Type.USER, userDictionary);
  }
  
  /**
   * Tokenize input text
   * @param text
   * @return list of Token
   */
  public List<Token> tokenize(String text) {
    
    if (!split) {
      return doTokenize(0, text);			
    }
    
    List<Integer> splitPositions = getSplitPositions(text);
    
    if(splitPositions.size() == 0) {
      return doTokenize(0, text);
    }
    
    ArrayList<Token> result = new ArrayList<Token>();
    int offset = 0;
    for(int position : splitPositions) {
      result.addAll(doTokenize(offset, text.substring(offset, position + 1)));
      offset = position + 1;
    }
    
    if(offset < text.length()) {
      result.addAll(doTokenize(offset, text.substring(offset)));
    }
    
    return result;
  }
  
  /**
   * Split input text at 句読点, which is 。 and 、
   * @param text
   * @return list of split position
   */
  private List<Integer> getSplitPositions(String text) {
    ArrayList<Integer> splitPositions = new ArrayList<Integer>();
    
    int position = 0;
    int currentPosition = 0;
    
    while(true) {
      int indexOfMaru = text.indexOf("。", currentPosition);
      int indexOfTen = text.indexOf("、", currentPosition);
      
      if(indexOfMaru < 0 || indexOfTen < 0) {
        position = Math.max(indexOfMaru, indexOfTen);;
      } else {
        position = Math.min(indexOfMaru, indexOfTen);				
      }
      
      if(position >= 0) {
        splitPositions.add(position);
        currentPosition = position + 1;
      } else {
        break;
      }
    }
    
    return splitPositions;
  }
  
  private List<Token> doTokenize(int offset, String sentence) {
    char text[] = sentence.toCharArray();
    return doTokenize(offset, text, 0, text.length, false);
  }
  
  /**
   * Tokenize input sentence.
   * @param offset offset of sentence in original input text
   * @param sentence sentence to tokenize
   * @return list of Token
   */
  public List<Token> doTokenize(int offset, char[] sentence, int sentenceOffset, int sentenceLength, boolean discardPunctuation) {
    ArrayList<Token> result = new ArrayList<Token>();
    
    ViterbiNode[][][] lattice;
    try {
      lattice = viterbi.build(sentence, sentenceOffset, sentenceLength);
    } catch (IOException impossible) {
      throw new RuntimeException(impossible);
    }
    List<ViterbiNode> bestPath = viterbi.search(lattice);
    for (ViterbiNode node : bestPath) {
      int wordId = node.getWordId();
      if (node.getType() == Type.KNOWN && wordId == -1){ // Do not include BOS/EOS 
        continue;
      } else if (discardPunctuation && node.getLength() > 0 && isPunctuation(node.getSurfaceForm()[node.getOffset()])) {
        continue; // Do not emit punctuation
      }
      Token token = new Token(wordId, node.getSurfaceForm(), node.getOffset(), node.getLength(), node.getType(), offset + node.getStartIndex(), dictionaryMap.get(node.getType()));	// Pass different dictionary based on the type of node
      result.add(token);
    }
    
    return result;
  }
  
  /** returns a Graphviz String */
  public String debugTokenize(String text) {
    ViterbiNode[][][] lattice;
    try {
      lattice = this.viterbi.build(text.toCharArray(), 0, text.length());
    } catch (IOException impossible) {
      throw new RuntimeException(impossible);
    }
    List<ViterbiNode> bestPath = this.viterbi.search(lattice);
    
    return new GraphvizFormatter(ConnectionCosts.getInstance())
      .format(lattice[0], lattice[1], bestPath);
  }
  
  static final boolean isPunctuation(char ch) {
    switch(Character.getType(ch)) {
      case Character.SPACE_SEPARATOR:
      case Character.LINE_SEPARATOR:
      case Character.PARAGRAPH_SEPARATOR:
      case Character.CONTROL:
      case Character.FORMAT:
      case Character.DASH_PUNCTUATION:
      case Character.START_PUNCTUATION:
      case Character.END_PUNCTUATION:
      case Character.CONNECTOR_PUNCTUATION:
      case Character.OTHER_PUNCTUATION:
      case Character.MATH_SYMBOL:
      case Character.CURRENCY_SYMBOL:
      case Character.MODIFIER_SYMBOL:
      case Character.OTHER_SYMBOL:
      case Character.INITIAL_QUOTE_PUNCTUATION:
      case Character.FINAL_QUOTE_PUNCTUATION:
        return true;
      default:
        return false;
    }
  }
}

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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

import org.apache.lucene.analysis.kuromoji.dict.*;
import org.apache.lucene.analysis.kuromoji.viterbi.Viterbi;
import org.apache.lucene.analysis.kuromoji.viterbi.ViterbiNode;
import org.apache.lucene.analysis.kuromoji.viterbi.ViterbiNode.Type;

/**
 * Tokenizer main class.
 * Thread safe.
 */
public class Tokenizer {
  public enum Mode {
    NORMAL, SEARCH, EXTENDED
  }
  
  private final Viterbi viterbi;
  
  private final EnumMap<Type, Dictionary> dictionaryMap = new EnumMap<Type, Dictionary>(Type.class);
  
  private final boolean split;
  
  /**
   * Constructor
   */
  protected Tokenizer(UserDictionary userDictionary, Mode mode, boolean split) {
    
    final TokenInfoDictionary dict = TokenInfoDictionary.getInstance();
    final UnknownDictionary unknownDict = UnknownDictionary.getInstance();
    this.viterbi = new Viterbi(dict,
        unknownDict,
        ConnectionCosts.getInstance(),
        userDictionary,
        mode);
    
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
    return doTokenize(offset, text, 0, text.length);
  }
  
  /**
   * Tokenize input sentence.
   * @param offset offset of sentence in original input text
   * @param sentence sentence to tokenize
   * @return list of Token
   */
  public List<Token> doTokenize(int offset, char[] sentence, int sentenceOffset, int sentenceLength) {
    ArrayList<Token> result = new ArrayList<Token>();
    
    ViterbiNode[][][] lattice = viterbi.build(sentence, sentenceOffset, sentenceLength);
    List<ViterbiNode> bestPath = viterbi.search(lattice);
    for (ViterbiNode node : bestPath) {
      int wordId = node.getWordId();
      if (node.getType() == Type.KNOWN && wordId == 0){ // Do not include BOS/EOS 
        continue;
      }
      Token token = new Token(wordId, node.getSurfaceForm(), node.getOffset(), node.getLength(), node.getType(), offset + node.getStartIndex(), dictionaryMap.get(node.getType()));	// Pass different dictionary based on the type of node
      result.add(token);
    }
    
    return result;
  }
  
  /**
   * Get Builder to create Tokenizer instance.
   * @return Builder
   */
  public static Builder builder() {
    return new Builder();
  }
  
  /**
   * Builder class used to create Tokenizer instance.
   */
  public static class Builder {
    
    private Mode mode = Mode.NORMAL;
    
    // this is true, for other use.
    // lucene's tokenizer uses a breakiterator and doTokenize directly.
    private boolean split = true;
    
    private UserDictionary userDictionary = null;
    
    /**
     * Set tokenization mode
     * Default: NORMAL
     * @param mode tokenization mode
     * @return Builder
     */
    public Builder mode(Mode mode) {
      this.mode = mode;
      return this;
    }
    
    /**
     * Set if tokenizer should split input string at "。" and "、" before tokenize to increase performance.
     * Splitting shouldn't change the result of tokenization most of the cases.
     * Default: true
     * 
     * @param split whether tokenizer should split input string
     * @return Builder
     */
    public Builder split(boolean split) {
      this.split = split;
      return this;
    }
    
    /**
     * Set user dictionary input stream
     * @param userDictionaryReader dictionary file as {@link Reader}
     * @return Builder
     * @throws IOException 
     */
    public Builder userDictionary(Reader userDictionaryReader) throws IOException {
      this.userDictionary = new UserDictionary(userDictionaryReader);
      return this;
    }
    
    /**
     * Set user dictionary path
     * @param userDictionaryPath path to dictionary file
     * @return Builder
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    public Builder userDictionary(String userDictionaryPath) throws FileNotFoundException, IOException {
      this.userDictionary = new UserDictionary(userDictionaryPath);
      return this;
    }
    
    /**
     * Create Tokenizer instance
     * @return Tokenizer
     */
    public Tokenizer build() {
      return new Tokenizer(userDictionary, mode, split);
    }
  }
}

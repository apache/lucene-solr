package org.apache.lucene.analysis.compound;

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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Version;

/**
 * Base class for decomposition token filters. <a name="version"/>
 * <p>
 * You must specify the required {@link Version} compatibility when creating
 * CompoundWordTokenFilterBase:
 * <ul>
 * <li>As of 3.1, CompoundWordTokenFilterBase correctly handles Unicode 4.0
 * supplementary characters in strings and char arrays provided as compound word
 * dictionaries.
 * </ul>
 */
public abstract class CompoundWordTokenFilterBase extends TokenFilter {
  /**
   * The default for minimal word length that gets decomposed
   */
  public static final int DEFAULT_MIN_WORD_SIZE = 5;

  /**
   * The default for minimal length of subwords that get propagated to the output of this filter
   */
  public static final int DEFAULT_MIN_SUBWORD_SIZE = 2;

  /**
   * The default for maximal length of subwords that get propagated to the output of this filter
   */
  public static final int DEFAULT_MAX_SUBWORD_SIZE = 15;
  
  protected final CharArraySet dictionary;
  protected final LinkedList<Token> tokens;
  protected final int minWordSize;
  protected final int minSubwordSize;
  protected final int maxSubwordSize;
  protected final boolean onlyLongestMatch;
  
  private TermAttribute termAtt;
  private OffsetAttribute offsetAtt;
  private FlagsAttribute flagsAtt;
  private PositionIncrementAttribute posIncAtt;
  private TypeAttribute typeAtt;
  private PayloadAttribute payloadAtt;
  
  private final Token wrapper = new Token();
  /**
   * @deprecated use {@link #CompoundWordTokenFilterBase(Version, TokenStream, String[], int, int, int, boolean) instead
   */
  protected CompoundWordTokenFilterBase(TokenStream input, String[] dictionary, int minWordSize, int minSubwordSize, int maxSubwordSize, boolean onlyLongestMatch) {
    this(Version.LUCENE_30, input, makeDictionary(dictionary),minWordSize,minSubwordSize,maxSubwordSize, onlyLongestMatch);
  }
  
  /**
   * @deprecated use {@link #CompoundWordTokenFilterBase(Version, TokenStream, String[], boolean) instead
   */
  protected CompoundWordTokenFilterBase(TokenStream input, String[] dictionary, boolean onlyLongestMatch) {
    this(Version.LUCENE_30, input, makeDictionary(dictionary),DEFAULT_MIN_WORD_SIZE,DEFAULT_MIN_SUBWORD_SIZE,DEFAULT_MAX_SUBWORD_SIZE, onlyLongestMatch);
  }
  
  /**
   * @deprecated use {@link #CompoundWordTokenFilterBase(Version, TokenStream, Set, boolean) instead
   */
  protected CompoundWordTokenFilterBase(TokenStream input, Set<?> dictionary, boolean onlyLongestMatch) {
    this(Version.LUCENE_30, input, dictionary,DEFAULT_MIN_WORD_SIZE,DEFAULT_MIN_SUBWORD_SIZE,DEFAULT_MAX_SUBWORD_SIZE, onlyLongestMatch);
  }
  
  /**
   * @deprecated use {@link #CompoundWordTokenFilterBase(Version, TokenStream, String[]) instead
   */
  protected CompoundWordTokenFilterBase(TokenStream input, String[] dictionary) {
    this(Version.LUCENE_30, input, makeDictionary(dictionary),DEFAULT_MIN_WORD_SIZE,DEFAULT_MIN_SUBWORD_SIZE,DEFAULT_MAX_SUBWORD_SIZE, false);
  }
  
  /**
   * @deprecated use {@link #CompoundWordTokenFilterBase(Version, TokenStream, Set) instead
   */
  protected CompoundWordTokenFilterBase(TokenStream input, Set<?> dictionary) {
    this(Version.LUCENE_30, input, dictionary,DEFAULT_MIN_WORD_SIZE,DEFAULT_MIN_SUBWORD_SIZE,DEFAULT_MAX_SUBWORD_SIZE, false);
  }

  /**
   * @deprecated use {@link #CompoundWordTokenFilterBase(Version, TokenStream, Set[], int, int, int, boolean) instead
   */
  protected CompoundWordTokenFilterBase(TokenStream input, Set<?> dictionary, int minWordSize, int minSubwordSize, int maxSubwordSize, boolean onlyLongestMatch) {
    this(Version.LUCENE_30, input, dictionary, minWordSize, minSubwordSize, maxSubwordSize, onlyLongestMatch);
  }
  
  protected CompoundWordTokenFilterBase(Version matchVersion, TokenStream input, String[] dictionary, int minWordSize, int minSubwordSize, int maxSubwordSize, boolean onlyLongestMatch) {
    this(matchVersion, input,makeDictionary(dictionary),minWordSize,minSubwordSize,maxSubwordSize, onlyLongestMatch);
  }
  
  protected CompoundWordTokenFilterBase(Version matchVersion, TokenStream input, String[] dictionary, boolean onlyLongestMatch) {
    this(matchVersion, input,makeDictionary(dictionary),DEFAULT_MIN_WORD_SIZE,DEFAULT_MIN_SUBWORD_SIZE,DEFAULT_MAX_SUBWORD_SIZE, onlyLongestMatch);
  }

  protected CompoundWordTokenFilterBase(Version matchVersion, TokenStream input, Set dictionary, boolean onlyLongestMatch) {
    this(matchVersion, input,dictionary,DEFAULT_MIN_WORD_SIZE,DEFAULT_MIN_SUBWORD_SIZE,DEFAULT_MAX_SUBWORD_SIZE, onlyLongestMatch);
  }

  protected CompoundWordTokenFilterBase(Version matchVersion, TokenStream input, String[] dictionary) {
    this(matchVersion, input,makeDictionary(dictionary),DEFAULT_MIN_WORD_SIZE,DEFAULT_MIN_SUBWORD_SIZE,DEFAULT_MAX_SUBWORD_SIZE, false);
  }

  protected CompoundWordTokenFilterBase(Version matchVersion, TokenStream input, Set dictionary) {
    this(matchVersion, input,dictionary,DEFAULT_MIN_WORD_SIZE,DEFAULT_MIN_SUBWORD_SIZE,DEFAULT_MAX_SUBWORD_SIZE, false);
  }

  protected CompoundWordTokenFilterBase(Version matchVersion, TokenStream input, Set dictionary, int minWordSize, int minSubwordSize, int maxSubwordSize, boolean onlyLongestMatch) {
    super(input);
    
    this.tokens=new LinkedList<Token>();
    this.minWordSize=minWordSize;
    this.minSubwordSize=minSubwordSize;
    this.maxSubwordSize=maxSubwordSize;
    this.onlyLongestMatch=onlyLongestMatch;
    
    if (dictionary instanceof CharArraySet) {
      this.dictionary = (CharArraySet) dictionary;
    } else {
      this.dictionary = new CharArraySet(matchVersion, dictionary.size(), false);
      addAllLowerCase(this.dictionary, dictionary);
    }
    
    termAtt = addAttribute(TermAttribute.class);
    offsetAtt = addAttribute(OffsetAttribute.class);
    flagsAtt = addAttribute(FlagsAttribute.class);
    posIncAtt = addAttribute(PositionIncrementAttribute.class);
    typeAtt = addAttribute(TypeAttribute.class);
    payloadAtt = addAttribute(PayloadAttribute.class);
  }

  /**
   * Create a set of words from an array
   * The resulting Set does case insensitive matching
   * TODO We should look for a faster dictionary lookup approach.
   * @param dictionary 
   * @return {@link Set} of lowercased terms 
   */
  public static final Set<?> makeDictionary(final String[] dictionary) {
    return makeDictionary(Version.LUCENE_30, dictionary);
  }
  
  public static final Set<?> makeDictionary(final Version matchVersion, final String[] dictionary) {
    // is the below really case insensitive? 
    CharArraySet dict = new CharArraySet(matchVersion, dictionary.length, false);
    addAllLowerCase(dict, Arrays.asList(dictionary));
    return dict;
  }
  
  private final void setToken(final Token token) throws IOException {
    termAtt.setTermBuffer(token.termBuffer(), 0, token.termLength());
    flagsAtt.setFlags(token.getFlags());
    typeAtt.setType(token.type());
    offsetAtt.setOffset(token.startOffset(), token.endOffset());
    posIncAtt.setPositionIncrement(token.getPositionIncrement());
    payloadAtt.setPayload(token.getPayload());
  }
  
  @Override
  public final boolean incrementToken() throws IOException {
    if (tokens.size() > 0) {
      setToken(tokens.removeFirst());
      return true;
    }

    if (!input.incrementToken())
      return false;
    
    wrapper.setTermBuffer(termAtt.termBuffer(), 0, termAtt.termLength());
    wrapper.setStartOffset(offsetAtt.startOffset());
    wrapper.setEndOffset(offsetAtt.endOffset());
    wrapper.setFlags(flagsAtt.getFlags());
    wrapper.setType(typeAtt.type());
    wrapper.setPositionIncrement(posIncAtt.getPositionIncrement());
    wrapper.setPayload(payloadAtt.getPayload());
    
    decompose(wrapper);

    if (tokens.size() > 0) {
      setToken(tokens.removeFirst());
      return true;
    } else {
      return false;
    }
  }
  
  protected static final void addAllLowerCase(Set<Object> target, Collection<String> col) {
    for (String string : col) {
      target.add(string.toLowerCase());
    }
  }
  
  protected static char[] makeLowerCaseCopy(final char[] buffer) {
    char[] result=new char[buffer.length];
    System.arraycopy(buffer, 0, result, 0, buffer.length);
    
    for (int i=0;i<buffer.length;++i) {
       result[i]=Character.toLowerCase(buffer[i]);
    }
    
    return result;
  }
  
  protected final Token createToken(final int offset, final int length,
      final Token prototype) {
    int newStart = prototype.startOffset() + offset;
    Token t = prototype.clone(prototype.termBuffer(), offset, length, newStart, newStart+length);
    t.setPositionIncrement(0);
    return t;
  }

  protected void decompose(final Token token) {
    // In any case we give the original token back
    tokens.add((Token) token.clone());

    // Only words longer than minWordSize get processed
    if (token.termLength() < this.minWordSize) {
      return;
    }
    
    decomposeInternal(token);
  }
  
  protected abstract void decomposeInternal(final Token token);

  @Override
  public void reset() throws IOException {
    super.reset();
    tokens.clear();
  }
}

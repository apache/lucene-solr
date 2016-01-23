package org.apache.lucene.analysis.miscellaneous;

/*
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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;

/**
 * Provides the ability to override any {@link KeywordAttribute} aware stemmer
 * with custom dictionary-based stemming.
 */
public final class StemmerOverrideFilter extends TokenFilter {
  private final StemmerOverrideMap stemmerOverrideMap;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);
  private final BytesReader fstReader;
  private final Arc<BytesRef> scratchArc = new FST.Arc<>();
  private char[] spare = new char[0];
  
  /**
   * Create a new StemmerOverrideFilter, performing dictionary-based stemming
   * with the provided <code>dictionary</code>.
   * <p>
   * Any dictionary-stemmed terms will be marked with {@link KeywordAttribute}
   * so that they will not be stemmed with stemmers down the chain.
   * </p>
   */
  public StemmerOverrideFilter(final TokenStream input, final StemmerOverrideMap stemmerOverrideMap) {
    super(input);
    this.stemmerOverrideMap = stemmerOverrideMap;
    fstReader = stemmerOverrideMap.getBytesReader();
  }
  
  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (fstReader == null) {
        // No overrides
        return true;
      }
      if (!keywordAtt.isKeyword()) { // don't muck with already-keyworded terms
        final BytesRef stem = stemmerOverrideMap.get(termAtt.buffer(), termAtt.length(), scratchArc, fstReader);
        if (stem != null) {
          spare = ArrayUtil.grow(termAtt.buffer(), stem.length);
          final int length = UnicodeUtil.UTF8toUTF16(stem, spare);
          if (spare != termAtt.buffer()) {
            termAtt.copyBuffer(spare, 0, length);
          } else {
            termAtt.setLength(length);
          }
          keywordAtt.setKeyword(true);
        }
      }
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * A read-only 4-byte FST backed map that allows fast case-insensitive key
   * value lookups for {@link StemmerOverrideFilter}
   */
  // TODO maybe we can generalize this and reuse this map somehow?
  public final static class StemmerOverrideMap {
    private final FST<BytesRef> fst;
    private final boolean ignoreCase;
    
    /**
     * Creates a new {@link StemmerOverrideMap} 
     * @param fst the fst to lookup the overrides
     * @param ignoreCase if the keys case should be ingored
     */
    public StemmerOverrideMap(FST<BytesRef> fst, boolean ignoreCase) {
      this.fst = fst;
      this.ignoreCase = ignoreCase;
    }
    
    /**
     * Returns a {@link BytesReader} to pass to the {@link #get(char[], int, FST.Arc, FST.BytesReader)} method.
     */
    public BytesReader getBytesReader() {
      if (fst == null) {
        return null;
      } else {
        return fst.getBytesReader();
      }
    }

    /**
     * Returns the value mapped to the given key or <code>null</code> if the key is not in the FST dictionary.
     */
    public BytesRef get(char[] buffer, int bufferLen, Arc<BytesRef> scratchArc, BytesReader fstReader) throws IOException {
      BytesRef pendingOutput = fst.outputs.getNoOutput();
      BytesRef matchOutput = null;
      int bufUpto = 0;
      fst.getFirstArc(scratchArc);
      while (bufUpto < bufferLen) {
        final int codePoint = Character.codePointAt(buffer, bufUpto, bufferLen);
        if (fst.findTargetArc(ignoreCase ? Character.toLowerCase(codePoint) : codePoint, scratchArc, scratchArc, fstReader) == null) {
          return null;
        }
        pendingOutput = fst.outputs.add(pendingOutput, scratchArc.output);
        bufUpto += Character.charCount(codePoint);
      }
      if (scratchArc.isFinal()) {
        matchOutput = fst.outputs.add(pendingOutput, scratchArc.nextFinalOutput);
      }
      return matchOutput;
    }
    
  }
  /**
   * This builder builds an {@link FST} for the {@link StemmerOverrideFilter}
   */
  public static class Builder {
    private final BytesRefHash hash = new BytesRefHash();
    private final BytesRefBuilder spare = new BytesRefBuilder();
    private final ArrayList<CharSequence> outputValues = new ArrayList<>();
    private final boolean ignoreCase;
    private final CharsRefBuilder charsSpare = new CharsRefBuilder();
    
    /**
     * Creates a new {@link Builder} with ignoreCase set to <code>false</code> 
     */
    public Builder() {
      this(false);
    }
    
    /**
     * Creates a new {@link Builder}
     * @param ignoreCase if the input case should be ignored.
     */
    public Builder(boolean ignoreCase) {
      this.ignoreCase = ignoreCase;
    }
    
    /**
     * Adds an input string and its stemmer override output to this builder.
     * 
     * @param input the input char sequence 
     * @param output the stemmer override output char sequence
     * @return <code>false</code> iff the input has already been added to this builder otherwise <code>true</code>.
     */
    public boolean add(CharSequence input, CharSequence output) {
      final int length = input.length();
      if (ignoreCase) {
        // convert on the fly to lowercase
        charsSpare.grow(length);
        final char[] buffer = charsSpare.chars();
        for (int i = 0; i < length; ) {
          i += Character.toChars(
                  Character.toLowerCase(
                      Character.codePointAt(input, i)), buffer, i);
        }
        spare.copyChars(buffer, 0, length);
      } else {
        spare.copyChars(input, 0, length);
      }
      if (hash.add(spare.get()) >= 0) {
        outputValues.add(output);
        return true;
      }
      return false;
    }
    
    /**
     * Returns an {@link StemmerOverrideMap} to be used with the {@link StemmerOverrideFilter}
     * @return an {@link StemmerOverrideMap} to be used with the {@link StemmerOverrideFilter}
     * @throws IOException if an {@link IOException} occurs;
     */
    public StemmerOverrideMap build() throws IOException {
      ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
      org.apache.lucene.util.fst.Builder<BytesRef> builder = new org.apache.lucene.util.fst.Builder<>(
          FST.INPUT_TYPE.BYTE4, outputs);
      final int[] sort = hash.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
      IntsRefBuilder intsSpare = new IntsRefBuilder();
      final int size = hash.size();
      BytesRef spare = new BytesRef();
      for (int i = 0; i < size; i++) {
        int id = sort[i];
        BytesRef bytesRef = hash.get(id, spare);
        intsSpare.copyUTF8Bytes(bytesRef);
        builder.add(intsSpare.get(), new BytesRef(outputValues.get(id)));
      }
      return new StemmerOverrideMap(builder.finish(), ignoreCase);
    }
    
  }
}

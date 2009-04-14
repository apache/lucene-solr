package org.apache.lucene.search.trie;

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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * This class provides a {@link TokenStream} for indexing <code>long</code> values
 * that can be queried by {@link LongTrieRangeFilter}. This stream is not intended
 * to be used in analyzers, its more for iterating the different precisions during
 * indexing a specific numeric value.
 * <p>A <code>long</code> value is indexed as multiple string encoded terms, each reduced
 * by zeroing bits from the right. Each value is also prefixed (in the first char) by the
 * <code>shift</code> value (number of bits removed) used during encoding.
 * <p>The number of bits removed from the right for each trie entry is called
 * <code>precisionStep</code> in this API. For comparing the different step values, see the
 * {@linkplain org.apache.lucene.search.trie package description}.
 * <p>The usage pattern is (it is recommened to switch off norms and term frequencies
 * for numeric fields; it does not make sense to have them):
 * <pre>
 *  Field field = new Field(name, new LongTrieTokenStream(value, precisionStep));
 *  field.setOmitNorms(true);
 *  field.setOmitTermFreqAndPositions(true);
 *  document.add(field);
 * </pre>
 * <p>For optimal performance, re-use the TokenStream and Field instance
 * for more than one document:
 * <pre>
 *  <em>// init</em>
 *  TokenStream stream = new LongTrieTokenStream(precisionStep);
 *  Field field = new Field(name, stream);
 *  field.setOmitNorms(true);
 *  field.setOmitTermFreqAndPositions(true);
 *  Document doc = new Document();
 *  document.add(field);
 *  <em>// use this code to index many documents:</em>
 *  stream.setValue(value1)
 *  writer.addDocument(document);
 *  stream.setValue(value2)
 *  writer.addDocument(document);
 *  ...
 * </pre>
 * <p><em>Please note:</em> Token streams are read, when the document is added to index.
 * If you index more than one numeric field, use a separate instance for each.
 * <p>For more information, how trie fields work, see the
 * {@linkplain org.apache.lucene.search.trie package description}.
 */
public class LongTrieTokenStream extends TokenStream {

  /** The full precision token gets this token type assigned. */
  public static final String TOKEN_TYPE_FULL_PREC  = "fullPrecTrieLong";

  /** The lower precision tokens gets this token type assigned. */
  public static final String TOKEN_TYPE_LOWER_PREC = "lowerPrecTrieLong";

  /**
   * Creates a token stream for indexing <code>value</code> with the given
   * <code>precisionStep</code>. As instance creating is a major cost,
   * consider using a {@link #LongTrieTokenStream(int)} instance once for
   * indexing a large number of documents and assign a value with
   * {@link #setValue} for each document.
   * To index double values use the converter {@link TrieUtils#doubleToSortableLong}.
   */
  public LongTrieTokenStream(final long value, final int precisionStep) {
    if (precisionStep<1 || precisionStep>64)
      throw new IllegalArgumentException("precisionStep may only be 1..64");
    this.value = value;
    this.precisionStep = precisionStep;
    termAtt = (TermAttribute) addAttribute(TermAttribute.class);
    typeAtt = (TypeAttribute) addAttribute(TypeAttribute.class);
    posIncrAtt = (PositionIncrementAttribute) addAttribute(PositionIncrementAttribute.class);
    shiftAtt = (ShiftAttribute) addAttribute(ShiftAttribute.class);
  }
  
  /**
   * Creates a token stream for indexing values with the given
   * <code>precisionStep</code>. This stream is initially &quot;empty&quot;
   * (using a numeric value of 0), assign a value before indexing
   * each document using {@link #setValue}.
   */
  public LongTrieTokenStream(final int precisionStep) {
    this(0L, precisionStep);
  }

  /**
   * Resets the token stream to deliver prefix encoded values
   * for <code>value</code>. Use this method to index the same
   * numeric field for a large number of documents and reuse the
   * current stream instance.
   * To index double values use the converter {@link TrieUtils#doubleToSortableLong}.
   */
  public void setValue(final long value) {
    this.value = value;
    reset();
  }
  
  // @Override
  public void reset() {
    shift = 0;
  }

  // @Override
  public boolean incrementToken() {
    if (shift>=64) return false;
    final char[] buffer = termAtt.resizeTermBuffer(TrieUtils.LONG_BUF_SIZE);
    termAtt.setTermLength(TrieUtils.longToPrefixCoded(value, shift, buffer));
    shiftAtt.setShift(shift);
    if (shift==0) {
      typeAtt.setType(TOKEN_TYPE_FULL_PREC);
      posIncrAtt.setPositionIncrement(1);
    } else {
      typeAtt.setType(TOKEN_TYPE_LOWER_PREC);
      posIncrAtt.setPositionIncrement(0);
    }
    shift += precisionStep;
    return true;
  }

  // @Override
  /** @deprecated */
  public Token next(final Token reusableToken) {
    if (shift>=64) return null;
    reusableToken.clear();
    final char[] buffer = reusableToken.resizeTermBuffer(TrieUtils.LONG_BUF_SIZE);
    reusableToken.setTermLength(TrieUtils.longToPrefixCoded(value, shift, buffer));
    if (shift==0) {
      reusableToken.setType(TOKEN_TYPE_FULL_PREC);
      reusableToken.setPositionIncrement(1);
    } else {
      reusableToken.setType(TOKEN_TYPE_LOWER_PREC);
      reusableToken.setPositionIncrement(0);
    }
    shift += precisionStep;
    return reusableToken;
  }
  
  // @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("(trie-long,value=").append(value);
    sb.append(",precisionStep=").append(precisionStep).append(')');
    return sb.toString();
  }

  // members
  private final TermAttribute termAtt;
  private final TypeAttribute typeAtt;
  private final PositionIncrementAttribute posIncrAtt;
  private final ShiftAttribute shiftAtt;
  
  private int shift = 0;
  private long value;
  private final int precisionStep;
}

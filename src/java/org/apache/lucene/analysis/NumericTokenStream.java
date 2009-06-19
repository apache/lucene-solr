package org.apache.lucene.analysis;

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

import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.search.NumericRangeQuery; // for javadocs
import org.apache.lucene.search.NumericRangeFilter; // for javadocs
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * This class provides a {@link TokenStream} for indexing numeric values
 * that can be used by {@link NumericRangeQuery}/{@link NumericRangeFilter}.
 * For more information, how to use this class and its configuration properties
 * (<a href="../search/NumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>)
 * read the docs of {@link NumericRangeQuery}.
 *
 * <p>This stream is not intended to be used in analyzers, its more for iterating the
 * different precisions during indexing a specific numeric value.
 * A numeric value is indexed as multiple string encoded terms, each reduced
 * by zeroing bits from the right. Each value is also prefixed (in the first char) by the
 * <code>shift</code> value (number of bits removed) used during encoding.
 * The number of bits removed from the right for each trie entry is called
 * <code>precisionStep</code> in this API.
 *
 * <p>The usage pattern is (it is recommened to switch off norms and term frequencies
 * for numeric fields; it does not make sense to have them):
 * <pre>
 *  Field field = new Field(name, new NumericTokenStream(precisionStep).set<em>???</em>Value(value));
 *  field.setOmitNorms(true);
 *  field.setOmitTermFreqAndPositions(true);
 *  document.add(field);
 * </pre>
 * <p>For optimal performance, re-use the TokenStream and Field instance
 * for more than one document:
 * <pre>
 *  <em>// init</em>
 *  NumericTokenStream stream = new NumericTokenStream(precisionStep);
 *  Field field = new Field(name, stream);
 *  field.setOmitNorms(true);
 *  field.setOmitTermFreqAndPositions(true);
 *  Document doc = new Document();
 *  document.add(field);
 *  <em>// use this code to index many documents:</em>
 *  stream.set<em>???</em>Value(value1)
 *  writer.addDocument(document);
 *  stream.set<em>???</em>Value(value2)
 *  writer.addDocument(document);
 *  ...
 * </pre>
 * <p><em>Please note:</em> Token streams are read, when the document is added to index.
 * If you index more than one numeric field, use a separate instance for each.
 *
 * <p>Values indexed by this stream can be sorted on or loaded into the field cache.
 * For that factories like {@link NumericUtils#getLongSortField} are available,
 * as well as parsers for filling the field cache (e.g., {@link NumericUtils#FIELD_CACHE_LONG_PARSER})
 *
 * @since 2.9
 */
public final class NumericTokenStream extends TokenStream {

  /** The full precision 64 bit token gets this token type assigned. */
  public static final String TOKEN_TYPE_FULL_PREC_64  = "fullPrecNumeric64";

  /** The lower precision 64 bit tokens gets this token type assigned. */
  public static final String TOKEN_TYPE_LOWER_PREC_64 = "lowerPrecNumeric64";

  /** The full precision 32 bit token gets this token type assigned. */
  public static final String TOKEN_TYPE_FULL_PREC_32  = "fullPrecNumeric32";

  /** The lower precision 32 bit tokens gets this token type assigned. */
  public static final String TOKEN_TYPE_LOWER_PREC_32 = "lowerPrecNumeric32";

  /**
   * Creates a token stream for numeric values. The stream is not yet initialized,
   * before using set a value using the various set<em>???</em>Value() methods.
   */
  public NumericTokenStream(final int precisionStep) {
    this.precisionStep = precisionStep;
    termAtt = (TermAttribute) addAttribute(TermAttribute.class);
    typeAtt = (TypeAttribute) addAttribute(TypeAttribute.class);
    posIncrAtt = (PositionIncrementAttribute) addAttribute(PositionIncrementAttribute.class);
  }

  /**
   * Initializes the token stream with the supplied <code>long</code> value.
   * @param value the value, for which this TokenStream should enumerate tokens.
   * @return this instance, because of this you can use it the following way:
   * <code>new Field(name, new NumericTokenStream(precisionStep).setLongValue(value))</code>
   */
  public NumericTokenStream setLongValue(final long value) {
    this.value = value;
    valSize = 64;
    shift = 0;
    return this;
  }
  
  /**
   * Initializes the token stream with the supplied <code>int</code> value.
   * @param value the value, for which this TokenStream should enumerate tokens.
   * @return this instance, because of this you can use it the following way:
   * <code>new Field(name, new NumericTokenStream(precisionStep).setIntValue(value))</code>
   */
  public NumericTokenStream setIntValue(final int value) {
    this.value = (long) value;
    valSize = 32;
    shift = 0;
    return this;
  }
  
  /**
   * Initializes the token stream with the supplied <code>double</code> value.
   * @param value the value, for which this TokenStream should enumerate tokens.
   * @return this instance, because of this you can use it the following way:
   * <code>new Field(name, new NumericTokenStream(precisionStep).setDoubleValue(value))</code>
   */
  public NumericTokenStream setDoubleValue(final double value) {
    this.value = NumericUtils.doubleToSortableLong(value);
    valSize = 64;
    shift = 0;
    return this;
  }
  
  /**
   * Initializes the token stream with the supplied <code>float</code> value.
   * @param value the value, for which this TokenStream should enumerate tokens.
   * @return this instance, because of this you can use it the following way:
   * <code>new Field(name, new NumericTokenStream(precisionStep).setFloatValue(value))</code>
   */
  public NumericTokenStream setFloatValue(final float value) {
    this.value = (long) NumericUtils.floatToSortableInt(value);
    valSize = 32;
    shift = 0;
    return this;
  }
  
  // @Override
  public void reset() {
    if (valSize == 0)
      throw new IllegalStateException("call set???Value() before usage");
    if (precisionStep < 1 || precisionStep > valSize)
      throw new IllegalArgumentException("precisionStep may only be 1.."+valSize);
    shift = 0;
  }

  // @Override
  public boolean incrementToken() {
    if (valSize == 0)
      throw new IllegalStateException("call set???Value() before usage");
    if (shift >= valSize)
      return false;

    final char[] buffer;
    switch (valSize) {
      case 64:
        buffer = termAtt.resizeTermBuffer(NumericUtils.LONG_BUF_SIZE);
        termAtt.setTermLength(NumericUtils.longToPrefixCoded(value, shift, buffer));
        typeAtt.setType((shift == 0) ? TOKEN_TYPE_FULL_PREC_64 : TOKEN_TYPE_LOWER_PREC_64);
        break;
      
      case 32:
        buffer = termAtt.resizeTermBuffer(NumericUtils.INT_BUF_SIZE);
        termAtt.setTermLength(NumericUtils.intToPrefixCoded((int) value, shift, buffer));
        typeAtt.setType((shift == 0) ? TOKEN_TYPE_FULL_PREC_32 : TOKEN_TYPE_LOWER_PREC_32);
        break;
      
      default:
        // should not happen
        throw new IllegalArgumentException("valSize must be 32 or 64");
    }
    
    posIncrAtt.setPositionIncrement((shift == 0) ? 1 : 0);
    shift += precisionStep;
    return true;
  }

  // @Override
  /** @deprecated Will be removed in Lucene 3.0 */
  public Token next(final Token reusableToken) {
    assert reusableToken != null;
    if (valSize == 0)
      throw new IllegalStateException("call set???Value() before usage");
    if (shift >= valSize)
      return null;
    
    reusableToken.clear();

    final char[] buffer;
    switch (valSize) {
      case 64:
        buffer = reusableToken.resizeTermBuffer(NumericUtils.LONG_BUF_SIZE);
        reusableToken.setTermLength(NumericUtils.longToPrefixCoded(value, shift, buffer));
        reusableToken.setType((shift == 0) ? TOKEN_TYPE_FULL_PREC_64 : TOKEN_TYPE_LOWER_PREC_64);
        break;
      
      case 32:
        buffer = reusableToken.resizeTermBuffer(NumericUtils.INT_BUF_SIZE);
        reusableToken.setTermLength(NumericUtils.intToPrefixCoded((int) value, shift, buffer));
        reusableToken.setType((shift == 0) ? TOKEN_TYPE_FULL_PREC_32 : TOKEN_TYPE_LOWER_PREC_32);
        break;
      
      default:
        // should not happen
        throw new IllegalArgumentException("valSize must be 32 or 64");
    }

    reusableToken.setPositionIncrement((shift == 0) ? 1 : 0);
    shift += precisionStep;
    return reusableToken;
  }
  
  // @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("(numeric,valSize=").append(valSize);
    sb.append(",precisionStep=").append(precisionStep).append(')');
    return sb.toString();
  }

  // members
  private final TermAttribute termAtt;
  private final TypeAttribute typeAtt;
  private final PositionIncrementAttribute posIncrAtt;
  
  private int shift = 0, valSize = 0; // valSize==0 means not initialized
  private final int precisionStep;
  
  private long value = 0L;
}

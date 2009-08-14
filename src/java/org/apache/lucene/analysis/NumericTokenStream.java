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
import org.apache.lucene.document.NumericField; // for javadocs
import org.apache.lucene.search.NumericRangeQuery; // for javadocs
import org.apache.lucene.search.NumericRangeFilter; // for javadocs
import org.apache.lucene.search.SortField; // for javadocs
import org.apache.lucene.search.FieldCache; // javadocs
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * <b>Expert:</b> This class provides a {@link TokenStream} for indexing numeric values
 * that can be used by {@link NumericRangeQuery}/{@link NumericRangeFilter}.
 * For more information, how to use this class and its configuration properties
 * (<a href="../search/NumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>)
 * read the docs of {@link NumericRangeQuery}.
 *
 * <p><b>For easy usage during indexing, there is a {@link NumericField}, that uses the optimal
 * indexing settings (no norms, no term freqs). {@link NumericField} is a wrapper around this
 * expert token stream.</b>
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
 *  Document document = new Document();
 *  document.add(field);
 *  <em>// use this code to index many documents:</em>
 *  stream.set<em>???</em>Value(value1)
 *  writer.addDocument(document);
 *  stream.set<em>???</em>Value(value2)
 *  writer.addDocument(document);
 *  ...
 * </pre>
 *
 * <p><em>Please note:</em> Token streams are read, when the document is added to index.
 * If you index more than one numeric field, use a separate instance for each.
 *
 * <p>Values indexed by this stream can be loaded into the {@link FieldCache}
 * and can be sorted (use {@link SortField}{@code .TYPE} to specify the correct
 * type; {@link SortField#AUTO} does not work with this type of field).
 * Values solely used for sorting can be indexed using a <code>precisionStep</code>
 * of {@link Integer#MAX_VALUE} (at least &ge;64), because this step only produces
 * one value token with highest precision.
 *
 * <p><font color="red"><b>NOTE:</b> This API is experimental and
 * might change in incompatible ways in the next release.</font>
 *
 * @since 2.9
 */
public final class NumericTokenStream extends TokenStream {

  /** The full precision token gets this token type assigned. */
  public static final String TOKEN_TYPE_FULL_PREC  = "fullPrecNumeric";

  /** The lower precision tokens gets this token type assigned. */
  public static final String TOKEN_TYPE_LOWER_PREC = "lowerPrecNumeric";

  /**
   * Creates a token stream for numeric values using the default <code>precisionStep</code>
   * {@link NumericUtils#PRECISION_STEP_DEFAULT} (4). The stream is not yet initialized,
   * before using set a value using the various set<em>???</em>Value() methods.
   */
  public NumericTokenStream() {
    this(NumericUtils.PRECISION_STEP_DEFAULT);
  }
  
  /**
   * Creates a token stream for numeric values with the specified
   * <code>precisionStep</code>. The stream is not yet initialized,
   * before using set a value using the various set<em>???</em>Value() methods.
   */
  public NumericTokenStream(final int precisionStep) {
    this.precisionStep = precisionStep;
    if (precisionStep < 1)
      throw new IllegalArgumentException("precisionStep must be >=1");
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
    shift = 0;
  }

  // @Override
  public boolean incrementToken() {
    if (valSize == 0)
      throw new IllegalStateException("call set???Value() before usage");
    if (shift >= valSize)
      return false;

    clearAttributes();
    final char[] buffer;
    switch (valSize) {
      case 64:
        buffer = termAtt.resizeTermBuffer(NumericUtils.BUF_SIZE_LONG);
        termAtt.setTermLength(NumericUtils.longToPrefixCoded(value, shift, buffer));
        break;
      
      case 32:
        buffer = termAtt.resizeTermBuffer(NumericUtils.BUF_SIZE_INT);
        termAtt.setTermLength(NumericUtils.intToPrefixCoded((int) value, shift, buffer));
        break;
      
      default:
        // should not happen
        throw new IllegalArgumentException("valSize must be 32 or 64");
    }
    
    typeAtt.setType((shift == 0) ? TOKEN_TYPE_FULL_PREC : TOKEN_TYPE_LOWER_PREC);
    posIncrAtt.setPositionIncrement((shift == 0) ? 1 : 0);
    shift += precisionStep;
    return true;
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

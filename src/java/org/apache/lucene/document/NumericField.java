package org.apache.lucene.document;

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

import java.io.Reader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.search.NumericRangeQuery; // javadocs
import org.apache.lucene.search.NumericRangeFilter; // javadocs
import org.apache.lucene.search.SortField; // javadocs
import org.apache.lucene.search.FieldCache; // javadocs

/**
 * This class provides a {@link Field} for indexing numeric values
 * that can be used by {@link NumericRangeQuery}/{@link NumericRangeFilter}.
 * For more information, how to use this class and its configuration properties
 * (<a href="../search/NumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>)
 * read the docs of {@link NumericRangeQuery}.
 *
 * <p>A numeric value is indexed as multiple string encoded terms, each reduced
 * by zeroing bits from the right. Each value is also prefixed (in the first char) by the
 * <code>shift</code> value (number of bits removed) used during encoding.
 * The number of bits removed from the right for each trie entry is called
 * <code>precisionStep</code> in this API.
 *
 * <p>The usage pattern is:
 * <pre>
 *  document.add(
 *   new NumericField(name, precisionStep, Field.Store.XXX, true).set<em>???</em>Value(value)
 *  );
 * </pre>
 * <p>For optimal performance, re-use the NumericField and {@link Document} instance
 * for more than one document:
 * <pre>
 *  <em>// init</em>
 *  NumericField field = new NumericField(name, precisionStep, Field.Store.XXX, true);
 *  Document document = new Document();
 *  document.add(field);
 *  <em>// use this code to index many documents:</em>
 *  field.set<em>???</em>Value(value1)
 *  writer.addDocument(document);
 *  field.set<em>???</em>Value(value2)
 *  writer.addDocument(document);
 *  ...
 * </pre>
 *
 * <p>More advanced users can instead use {@link NumericTokenStream} directly, when
 * indexing numbers. This class is a wrapper around this token stream type for easier,
 * more intuitive usage.
 *
 * <p><b>Please note:</b> This class is only used during indexing. You can also create
 * numeric stored fields with it, but when retrieving the stored field value
 * from a {@link Document} instance after search, you will get a conventional
 * {@link Fieldable} instance where the numeric values are returned as {@link String}s
 * (according to <code>toString(value)</code> of the used data type).
 *
 * <p>Values indexed by this field can be loaded into the {@link FieldCache}
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
public final class NumericField extends AbstractField {

  private final NumericTokenStream tokenStream;

  /**
   * Creates a field for numeric values using the default <code>precisionStep</code>
   * {@link NumericUtils#PRECISION_STEP_DEFAULT} (4). The instance is not yet initialized with
   * a numeric value, before indexing a document containing this field,
   * set a value using the various set<em>???</em>Value() methods.
   * This constrcutor creates an indexed, but not stored field.
   * @param name the field name
   */
  public NumericField(String name) {
    this(name, NumericUtils.PRECISION_STEP_DEFAULT, Field.Store.NO, true);
  }
  
  /**
   * Creates a field for numeric values using the default <code>precisionStep</code>
   * {@link NumericUtils#PRECISION_STEP_DEFAULT} (4). The instance is not yet initialized with
   * a numeric value, before indexing a document containing this field,
   * set a value using the various set<em>???</em>Value() methods.
   * @param name the field name
   * @param store if the field should be stored in plain text form
   *  (according to <code>toString(value)</code> of the used data type)
   * @param index if the field should be indexed using {@link NumericTokenStream}
   */
  public NumericField(String name, Field.Store store, boolean index) {
    this(name, NumericUtils.PRECISION_STEP_DEFAULT, store, index);
  }
  
  /**
   * Creates a field for numeric values with the specified
   * <code>precisionStep</code>. The instance is not yet initialized with
   * a numeric value, before indexing a document containing this field,
   * set a value using the various set<em>???</em>Value() methods.
   * This constrcutor creates an indexed, but not stored field.
   * @param name the field name
   * @param precisionStep the used <a href="../search/NumericRangeQuery.html#precisionStepDesc">precision step</a>
   */
  public NumericField(String name, int precisionStep) {
    this(name, precisionStep, Field.Store.NO, true);
  }

  /**
   * Creates a field for numeric values with the specified
   * <code>precisionStep</code>. The instance is not yet initialized with
   * a numeric value, before indexing a document containing this field,
   * set a value using the various set<em>???</em>Value() methods.
   * @param name the field name
   * @param precisionStep the used <a href="../search/NumericRangeQuery.html#precisionStepDesc">precision step</a>
   * @param store if the field should be stored in plain text form
   *  (according to <code>toString(value)</code> of the used data type)
   * @param index if the field should be indexed using {@link NumericTokenStream}
   */
  public NumericField(String name, int precisionStep, Field.Store store, boolean index) {
    super(name, store, index ? Field.Index.ANALYZED_NO_NORMS : Field.Index.NO, Field.TermVector.NO);
    setOmitTermFreqAndPositions(true);
    tokenStream = new NumericTokenStream(precisionStep);
  }

  /** Returns a {@link NumericTokenStream} for indexing the numeric value. */
  public TokenStream tokenStreamValue()   {
    return isIndexed() ? tokenStream : null;
  }
  
  /** Returns always <code>null</code> for numeric fields */
  public byte[] binaryValue() {
    return null;
  }
  
  /** Returns always <code>null</code> for numeric fields */
  public byte[] getBinaryValue(byte[] result){
    return null;
  }

  /** Returns always <code>null</code> for numeric fields */
  public Reader readerValue() {
    return null;
  }
    
  /** Returns the numeric value as a string (how it is stored, when {@link Field.Store#YES} is choosen). */
  public String stringValue()   {
    return (fieldsData == null) ? null : fieldsData.toString();
  }
  
  /** Returns the current numeric value as a subclass of {@link Number}, <code>null</code> if not yet initialized. */
  public Number getNumericValue() {
    return (Number) fieldsData;
  }
  
  /**
   * Initializes the field with the supplied <code>long</code> value.
   * @param value the numeric value
   * @return this instance, because of this you can use it the following way:
   * <code>document.add(new NumericField(name, precisionStep).setLongValue(value))</code>
   */
  public NumericField setLongValue(final long value) {
    tokenStream.setLongValue(value);
    fieldsData = new Long(value);
    return this;
  }
  
  /**
   * Initializes the field with the supplied <code>int</code> value.
   * @param value the numeric value
   * @return this instance, because of this you can use it the following way:
   * <code>document.add(new NumericField(name, precisionStep).setIntValue(value))</code>
   */
  public NumericField setIntValue(final int value) {
    tokenStream.setIntValue(value);
    fieldsData = new Integer(value);
    return this;
  }
  
  /**
   * Initializes the field with the supplied <code>double</code> value.
   * @param value the numeric value
   * @return this instance, because of this you can use it the following way:
   * <code>document.add(new NumericField(name, precisionStep).setDoubleValue(value))</code>
   */
  public NumericField setDoubleValue(final double value) {
    tokenStream.setDoubleValue(value);
    fieldsData = new Double(value);
    return this;
  }
  
  /**
   * Initializes the field with the supplied <code>float</code> value.
   * @param value the numeric value
   * @return this instance, because of this you can use it the following way:
   * <code>document.add(new NumericField(name, precisionStep).setFloatValue(value))</code>
   */
  public NumericField setFloatValue(final float value) {
    tokenStream.setFloatValue(value);
    fieldsData = new Float(value);
    return this;
  }

}

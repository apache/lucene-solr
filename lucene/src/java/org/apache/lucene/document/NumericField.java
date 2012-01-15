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


import org.apache.lucene.analysis.NumericTokenStream; // javadocs
import org.apache.lucene.document.NumericField.DataType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.FieldCache; // javadocs
import org.apache.lucene.search.NumericRangeFilter; // javadocs
import org.apache.lucene.search.NumericRangeQuery; // javadocs
import org.apache.lucene.util.NumericUtils;

/**
 * <p>
 * This class provides a {@link Field} that enables indexing of numeric values
 * for efficient range filtering and sorting. Here's an example usage, adding an
 * int value:
 * 
 * <pre>
 * document.add(new NumericField(name, value));
 * </pre>
 * 
 * For optimal performance, re-use the <code>NumericField</code> and
 * {@link Document} instance for more than one document:
 * 
 * <pre>
 *  NumericField field = new NumericField(name, NumericField.DataType.INT);
 *  Document document = new Document();
 *  document.add(field);
 * 
 *  for(all documents) {
 *    ...
 *    field.setValue(value)
 *    writer.addDocument(document);
 *    ...
 *  }
 * </pre>
 *
 * <p>The java native types <code>int</code>, <code>long</code>,
 * <code>float</code> and <code>double</code> are
 * directly supported.  However, any value that can be
 * converted into these native types can also be indexed.
 * For example, date/time values represented by a
 * {@link java.util.Date} can be translated into a long
 * value using the {@link java.util.Date#getTime} method.  If you
 * don't need millisecond precision, you can quantize the
 * value, either by dividing the result of
 * {@link java.util.Date#getTime} or using the separate getters
 * (for year, month, etc.) to construct an <code>int</code> or
 * <code>long</code> value.</p>
 *
 * <p>To perform range querying or filtering against a
 * <code>NumericField</code>, use {@link NumericRangeQuery} or {@link
 * NumericRangeFilter}.  To sort according to a
 * <code>NumericField</code>, use the normal numeric sort types, eg
 * {@link org.apache.lucene.search.SortField.Type#INT}. <code>NumericField</code> 
 * values can also be loaded directly from {@link FieldCache}.</p>
 *
 * <p>By default, a <code>NumericField</code>'s value is not stored but
 * is indexed for range filtering and sorting.  You can use
 * {@link Field#Field(String,int,FieldType)}, etc.,
 * if you need to change these defaults.</p>
 *
 * <p>You may add the same field name as a <code>NumericField</code> to
 * the same document more than once.  Range querying and
 * filtering will be the logical OR of all values; so a range query
 * will hit all documents that have at least one value in
 * the range. However sort behavior is not defined.  If you need to sort,
 * you should separately index a single-valued <code>NumericField</code>.</p>
 *
 * <p>A <code>NumericField</code> will consume somewhat more disk space
 * in the index than an ordinary single-valued field.
 * However, for a typical index that includes substantial
 * textual content per document, this increase will likely
 * be in the noise. </p>
 *
 * <p>Within Lucene, each numeric value is indexed as a
 * <em>trie</em> structure, where each term is logically
 * assigned to larger and larger pre-defined brackets (which
 * are simply lower-precision representations of the value).
 * The step size between each successive bracket is called the
 * <code>precisionStep</code>, measured in bits.  Smaller
 * <code>precisionStep</code> values result in larger number
 * of brackets, which consumes more disk space in the index
 * but may result in faster range search performance.  The
 * default value, 4, was selected for a reasonable tradeoff
 * of disk space consumption versus performance.  You can
 * create a custom {@link FieldType} and invoke the {@link
 * FieldType#setNumericPrecisionStep} method if you'd
 * like to change the value.  Note that you must also
 * specify a congruent value when creating {@link
 * NumericRangeQuery} or {@link NumericRangeFilter}.
 * For low cardinality fields larger precision steps are good.
 * If the cardinality is &lt; 100, it is fair
 * to use {@link Integer#MAX_VALUE}, which produces one
 * term per value.
 *
 * <p>For more information on the internals of numeric trie
 * indexing, including the <a
 * href="../search/NumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>
 * configuration, see {@link NumericRangeQuery}. The format of
 * indexed values is described in {@link NumericUtils}.
 *
 * <p>If you only need to sort by numeric value, and never
 * run range querying/filtering, you can index using a
 * <code>precisionStep</code> of {@link Integer#MAX_VALUE}.
 * This will minimize disk space consumed. </p>
 *
 * <p>More advanced users can instead use {@link
 * NumericTokenStream} directly, when indexing numbers. This
 * class is a wrapper around this token stream type for
 * easier, more intuitive usage.</p>
 *
 * @since 2.9
 */
public final class NumericField extends Field {
  
  /** Data type of the value in {@link NumericField}.
   * @since 3.2
   */
  public static enum DataType {INT, LONG, FLOAT, DOUBLE}

  /** @lucene.experimental */
  public static FieldType getFieldType(DataType type, boolean stored) {
    final FieldType ft = new FieldType();
    ft.setIndexed(true);
    ft.setStored(stored);
    ft.setTokenized(true);
    ft.setOmitNorms(true);
    ft.setIndexOptions(IndexOptions.DOCS_ONLY);
    ft.setNumericType(type);
    ft.freeze();
    return ft;
  }

  private static final FieldType INT_TYPE = getFieldType(DataType.INT, false);
  private static final FieldType LONG_TYPE = getFieldType(DataType.LONG, false);
  private static final FieldType FLOAT_TYPE = getFieldType(DataType.FLOAT, false);
  private static final FieldType DOUBLE_TYPE = getFieldType(DataType.DOUBLE, false);

  /** Creates an int NumericField with the provided value
   *  and default <code>precisionStep</code> {@link
   *  NumericUtils#PRECISION_STEP_DEFAULT} (4). */
  public NumericField(String name, int value) {
    super(name, INT_TYPE);
    fieldsData = Integer.valueOf(value);
  }

  /** Creates a long NumericField with the provided value.
   *  and default <code>precisionStep</code> {@link
   *  NumericUtils#PRECISION_STEP_DEFAULT} (4). */
  public NumericField(String name, long value) {
    super(name, LONG_TYPE);
    fieldsData = Long.valueOf(value);
  }

  /** Creates a float NumericField with the provided value.
   *  and default <code>precisionStep</code> {@link
   *  NumericUtils#PRECISION_STEP_DEFAULT} (4). */
  public NumericField(String name, float value) {
    super(name, FLOAT_TYPE);
    fieldsData = Float.valueOf(value);
  }

  /** Creates a double NumericField with the provided value.
   *  and default <code>precisionStep</code> {@link
   *  NumericUtils#PRECISION_STEP_DEFAULT} (4). */
  public NumericField(String name, double value) {
    super(name, DOUBLE_TYPE);
    fieldsData = Double.valueOf(value);
  }
  
  public NumericField(String name, Number value, FieldType type) {
    super(name, type);
    final NumericField.DataType numericType = type.numericType();
    if (numericType == null) {
      throw new IllegalArgumentException("FieldType.numericType() cannot be null");
    }

    switch(numericType) {
    case INT:
      if (!(value instanceof Integer)) {
        throw new IllegalArgumentException("value must be an Integer but got " + value);
      }
      break;
    case LONG:
      if (!(value instanceof Long)) {
        throw new IllegalArgumentException("value must be a Long but got " + value);
      }
      break;
    case FLOAT:
      if (!(value instanceof Float)) {
        throw new IllegalArgumentException("value must be a Float but got " + value);
      }
      break;
    case DOUBLE:
      if (!(value instanceof Double)) {
        throw new IllegalArgumentException("value must be a Double but got " + value);
      }
      break;
    default:
      assert false : "Should never get here";
    }

    fieldsData = value;
  }
}

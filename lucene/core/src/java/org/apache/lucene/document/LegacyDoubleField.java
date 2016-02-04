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
package org.apache.lucene.document;


import org.apache.lucene.index.IndexOptions;


/**
 * <p>
 * Field that indexes <code>double</code> values
 * for efficient range filtering and sorting. Here's an example usage:
 * 
 * <pre class="prettyprint">
 * document.add(new LegacyDoubleField(name, 6.0, Field.Store.NO));
 * </pre>
 * 
 * For optimal performance, re-use the <code>LegacyDoubleField</code> and
 * {@link Document} instance for more than one document:
 * 
 * <pre class="prettyprint">
 *  LegacyDoubleField field = new LegacyDoubleField(name, 0.0, Field.Store.NO);
 *  Document document = new Document();
 *  document.add(field);
 * 
 *  for(all documents) {
 *    ...
 *    field.setDoubleValue(value)
 *    writer.addDocument(document);
 *    ...
 *  }
 * </pre>
 *
 * See also {@link LegacyIntField}, {@link LegacyLongField}, {@link
 * LegacyFloatField}.
 *
 * <p>To perform range querying or filtering against a
 * <code>LegacyDoubleField</code>, use {@link org.apache.lucene.search.LegacyNumericRangeQuery}.
 * To sort according to a
 * <code>LegacyDoubleField</code>, use the normal numeric sort types, eg
 * {@link org.apache.lucene.search.SortField.Type#DOUBLE}. <code>LegacyDoubleField</code>
 * values can also be loaded directly from {@link org.apache.lucene.index.LeafReader#getNumericDocValues}.</p>
 *
 * <p>You may add the same field name as an <code>LegacyDoubleField</code> to
 * the same document more than once.  Range querying and
 * filtering will be the logical OR of all values; so a range query
 * will hit all documents that have at least one value in
 * the range. However sort behavior is not defined.  If you need to sort,
 * you should separately index a single-valued <code>LegacyDoubleField</code>.</p>
 *
 * <p>A <code>LegacyDoubleField</code> will consume somewhat more disk space
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
 * default value, 16, was selected for a reasonable tradeoff
 * of disk space consumption versus performance.  You can
 * create a custom {@link FieldType} and invoke the {@link
 * FieldType#setNumericPrecisionStep} method if you'd
 * like to change the value.  Note that you must also
 * specify a congruent value when creating {@link
 * org.apache.lucene.search.LegacyNumericRangeQuery}.
 * For low cardinality fields larger precision steps are good.
 * If the cardinality is &lt; 100, it is fair
 * to use {@link Integer#MAX_VALUE}, which produces one
 * term per value.
 *
 * <p>For more information on the internals of numeric trie
 * indexing, including the <a
 * href="../search/LegacyNumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>
 * configuration, see {@link org.apache.lucene.search.LegacyNumericRangeQuery}. The format of
 * indexed values is described in {@link org.apache.lucene.util.LegacyNumericUtils}.
 *
 * <p>If you only need to sort by numeric value, and never
 * run range querying/filtering, you can index using a
 * <code>precisionStep</code> of {@link Integer#MAX_VALUE}.
 * This will minimize disk space consumed. </p>
 *
 * <p>More advanced users can instead use {@link
 * org.apache.lucene.analysis.LegacyNumericTokenStream} directly, when indexing numbers. This
 * class is a wrapper around this token stream type for
 * easier, more intuitive usage.</p>
 *
 * @deprecated Please use {@link DoublePoint} instead
 *
 * @since 2.9
 */

@Deprecated
public final class LegacyDoubleField extends Field {
  
  /** 
   * Type for a LegacyDoubleField that is not stored:
   * normalization factors, frequencies, and positions are omitted.
   */
  public static final FieldType TYPE_NOT_STORED = new FieldType();
  static {
    TYPE_NOT_STORED.setTokenized(true);
    TYPE_NOT_STORED.setOmitNorms(true);
    TYPE_NOT_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_NOT_STORED.setNumericType(FieldType.LegacyNumericType.DOUBLE);
    TYPE_NOT_STORED.freeze();
  }

  /** 
   * Type for a stored LegacyDoubleField:
   * normalization factors, frequencies, and positions are omitted.
   */
  public static final FieldType TYPE_STORED = new FieldType();
  static {
    TYPE_STORED.setTokenized(true);
    TYPE_STORED.setOmitNorms(true);
    TYPE_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_STORED.setNumericType(FieldType.LegacyNumericType.DOUBLE);
    TYPE_STORED.setStored(true);
    TYPE_STORED.freeze();
  }

  /** Creates a stored or un-stored LegacyDoubleField with the provided value
   *  and default <code>precisionStep</code> {@link
   *  org.apache.lucene.util.LegacyNumericUtils#PRECISION_STEP_DEFAULT} (16).
   *  @param name field name
   *  @param value 64-bit double value
   *  @param stored Store.YES if the content should also be stored
   *  @throws IllegalArgumentException if the field name is null. 
   */
  public LegacyDoubleField(String name, double value, Store stored) {
    super(name, stored == Store.YES ? TYPE_STORED : TYPE_NOT_STORED);
    fieldsData = Double.valueOf(value);
  }
  
  /** Expert: allows you to customize the {@link
   *  FieldType}. 
   *  @param name field name
   *  @param value 64-bit double value
   *  @param type customized field type: must have {@link FieldType#numericType()}
   *         of {@link org.apache.lucene.document.FieldType.LegacyNumericType#DOUBLE}.
   *  @throws IllegalArgumentException if the field name or type is null, or
   *          if the field type does not have a DOUBLE numericType()
   */
  public LegacyDoubleField(String name, double value, FieldType type) {
    super(name, type);
    if (type.numericType() != FieldType.LegacyNumericType.DOUBLE) {
      throw new IllegalArgumentException("type.numericType() must be DOUBLE but got " + type.numericType());
    }
    fieldsData = Double.valueOf(value);
  }
}

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
package org.apache.solr.legacy;


import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexOptions;


/**
 * <p>
 * Field that indexes <code>long</code> values
 * for efficient range filtering and sorting. Here's an example usage:
 * 
 * <pre class="prettyprint">
 * document.add(new LegacyLongField(name, 6L, Field.Store.NO));
 * </pre>
 * 
 * For optimal performance, re-use the <code>LegacyLongField</code> and
 * {@link Document} instance for more than one document:
 * 
 * <pre class="prettyprint">
 *  LegacyLongField field = new LegacyLongField(name, 0L, Field.Store.NO);
 *  Document document = new Document();
 *  document.add(field);
 * 
 *  for(all documents) {
 *    ...
 *    field.setLongValue(value)
 *    writer.addDocument(document);
 *    ...
 *  }
 * </pre>
 *
 * See also {@link LegacyIntField}, {@link LegacyFloatField}, {@link
 * LegacyDoubleField}.
 *
 * Any type that can be converted to long can also be
 * indexed.  For example, date/time values represented by a
 * {@link java.util.Date} can be translated into a long
 * value using the {@link java.util.Date#getTime} method.  If you
 * don't need millisecond precision, you can quantize the
 * value, either by dividing the result of
 * {@link java.util.Date#getTime} or using the separate getters
 * (for year, month, etc.) to construct an <code>int</code> or
 * <code>long</code> value.
 *
 * <p>To perform range querying or filtering against a
 * <code>LegacyLongField</code>, use {@link org.apache.solr.legacy.LegacyNumericRangeQuery}.
 * To sort according to a
 * <code>LegacyLongField</code>, use the normal numeric sort types, eg
 * {@link org.apache.lucene.search.SortField.Type#LONG}. <code>LegacyLongField</code>
 * values can also be loaded directly from {@link org.apache.lucene.index.LeafReader#getNumericDocValues}.
 *
 * <p>You may add the same field name as an <code>LegacyLongField</code> to
 * the same document more than once.  Range querying and
 * filtering will be the logical OR of all values; so a range query
 * will hit all documents that have at least one value in
 * the range. However sort behavior is not defined.  If you need to sort,
 * you should separately index a single-valued <code>LegacyLongField</code>.
 *
 * <p>A <code>LegacyLongField</code> will consume somewhat more disk space
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
 * create a custom {@link LegacyFieldType} and invoke the {@link
 * LegacyFieldType#setNumericPrecisionStep} method if you'd
 * like to change the value.  Note that you must also
 * specify a congruent value when creating {@link
 * org.apache.solr.legacy.LegacyNumericRangeQuery}.
 * For low cardinality fields larger precision steps are good.
 * If the cardinality is &lt; 100, it is fair
 * to use {@link Integer#MAX_VALUE}, which produces one
 * term per value.
 *
 * <p>For more information on the internals of numeric trie
 * indexing, including the <a
 * href="LegacyNumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>
 * configuration, see {@link org.apache.solr.legacy.LegacyNumericRangeQuery}. The format of
 * indexed values is described in {@link org.apache.solr.legacy.LegacyNumericUtils}.
 *
 * <p>If you only need to sort by numeric value, and never
 * run range querying/filtering, you can index using a
 * <code>precisionStep</code> of {@link Integer#MAX_VALUE}.
 * This will minimize disk space consumed.
 *
 * <p>More advanced users can instead use {@link
 * org.apache.solr.legacy.LegacyNumericTokenStream} directly, when indexing numbers. This
 * class is a wrapper around this token stream type for
 * easier, more intuitive usage.</p>
 *
 * @deprecated Please use {@link LongPoint} instead
 *
 * @since 2.9
 */

@Deprecated
public final class LegacyLongField extends LegacyField {
  
  /** 
   * Type for a LegacyLongField that is not stored:
   * normalization factors, frequencies, and positions are omitted.
   */
  public static final LegacyFieldType TYPE_NOT_STORED = new LegacyFieldType();
  static {
    TYPE_NOT_STORED.setTokenized(true);
    TYPE_NOT_STORED.setOmitNorms(true);
    TYPE_NOT_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_NOT_STORED.setNumericType(LegacyNumericType.LONG);
    TYPE_NOT_STORED.freeze();
  }

  /** 
   * Type for a stored LegacyLongField:
   * normalization factors, frequencies, and positions are omitted.
   */
  public static final LegacyFieldType TYPE_STORED = new LegacyFieldType();
  static {
    TYPE_STORED.setTokenized(true);
    TYPE_STORED.setOmitNorms(true);
    TYPE_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_STORED.setNumericType(LegacyNumericType.LONG);
    TYPE_STORED.setStored(true);
    TYPE_STORED.freeze();
  }

  /** Creates a stored or un-stored LegacyLongField with the provided value
   *  and default <code>precisionStep</code> {@link
   *  org.apache.solr.legacy.LegacyNumericUtils#PRECISION_STEP_DEFAULT} (16).
   *  @param name field name
   *  @param value 64-bit long value
   *  @param stored Store.YES if the content should also be stored
   *  @throws IllegalArgumentException if the field name is null.
   */
  public LegacyLongField(String name, long value, Store stored) {
    super(name, stored == Store.YES ? TYPE_STORED : TYPE_NOT_STORED);
    fieldsData = Long.valueOf(value);
  }
  
  /** Expert: allows you to customize the {@link
   *  LegacyFieldType}. 
   *  @param name field name
   *  @param value 64-bit long value
   *  @param type customized field type: must have {@link LegacyFieldType#numericType()}
   *         of {@link LegacyNumericType#LONG}.
   *  @throws IllegalArgumentException if the field name or type is null, or
   *          if the field type does not have a LONG numericType()
   */
  public LegacyLongField(String name, long value, LegacyFieldType type) {
    super(name, type);
    if (type.numericType() != LegacyNumericType.LONG) {
      throw new IllegalArgumentException("type.numericType() must be LONG but got " + type.numericType());
    }
    fieldsData = Long.valueOf(value);
  }
}

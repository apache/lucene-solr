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
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.FieldCache; // javadocs
import org.apache.lucene.search.NumericRangeFilter; // javadocs
import org.apache.lucene.search.NumericRangeQuery; // javadocs
import org.apache.lucene.util.NumericUtils;

/**
 * <p>
 * This class provides a {@link Field} that enables indexing of integer values
 * for efficient range filtering and sorting. Here's an example usage:
 * 
 * <pre>
 * document.add(new IntField(name, 6));
 * </pre>
 * 
 * For optimal performance, re-use the <code>IntField</code> and
 * {@link Document} instance for more than one document:
 * 
 * <pre>
 *  IntField field = new IntField(name, 6);
 *  Document document = new Document();
 *  document.add(field);
 * 
 *  for(all documents) {
 *    ...
 *    field.setIntValue(value)
 *    writer.addDocument(document);
 *    ...
 *  }
 * </pre>
 *
 * See also {@link LongField}, {@link FloatField}, {@link
 * DoubleField}.
 *
 * <p>To perform range querying or filtering against a
 * <code>IntField</code>, use {@link NumericRangeQuery} or {@link
 * NumericRangeFilter}.  To sort according to a
 * <code>IntField</code>, use the normal numeric sort types, eg
 * {@link org.apache.lucene.search.SortField.Type#INT}. <code>IntField</code> 
 * values can also be loaded directly from {@link FieldCache}.</p>
 *
 * <p>By default, a <code>IntField</code>'s value is not stored but
 * is indexed for range filtering and sorting.  You can use
 * {@link StoredField} to also store the value.
 *
 * <p>You may add the same field name as an <code>IntField</code> to
 * the same document more than once.  Range querying and
 * filtering will be the logical OR of all values; so a range query
 * will hit all documents that have at least one value in
 * the range. However sort behavior is not defined.  If you need to sort,
 * you should separately index a single-valued <code>IntField</code>.</p>
 *
 * <p>An <code>IntField</code> will consume somewhat more disk space
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

public final class IntField extends Field {
  
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setIndexed(true);
    TYPE.setTokenized(true);
    TYPE.setOmitNorms(true);
    TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
    TYPE.setNumericType(FieldType.NumericType.INT);
    TYPE.freeze();
  }

  /** Creates an IntField with the provided value
   *  and default <code>precisionStep</code> {@link
   *  NumericUtils#PRECISION_STEP_DEFAULT} (4). */
  public IntField(String name, int value) {
    super(name, TYPE);
    fieldsData = Integer.valueOf(value);
  }
  
  /** Expert: allows you to customize the {@link
   *  FieldType}. */
  public IntField(String name, int value, FieldType type) {
    super(name, type);
    if (type.numericType() != FieldType.NumericType.INT) {
      throw new IllegalArgumentException("type.numericType() must be INT but got " + type.numericType());
    }
    fieldsData = Integer.valueOf(value);
  }
}

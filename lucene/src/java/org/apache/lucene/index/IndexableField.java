package org.apache.lucene.index;

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
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.values.PerDocFieldValues;
import org.apache.lucene.index.values.ValueType;
import org.apache.lucene.util.BytesRef;

// TODO: how to handle versioning here...?

// TODO: we need to break out separate StoredField...

/** Represents a single field for indexing.  IndexWriter
 *  consumes Iterable<IndexableField> as a document.
 *
 *  @lucene.experimental */

public interface IndexableField {

  // TODO: add attrs to this API?

  /* Field name */
  public String name();

  // NOTE: if doc/field impl has the notion of "doc level boost"
  // it must be multiplied in w/ this field's boost

  /** Field boost (you must pre-multiply in any doc boost). */
  public float boost();
  
  /* Non-null if this field has a binary value */
  public BytesRef binaryValue();

  /* Non-null if this field has a string value */
  public String stringValue();

  /* Non-null if this field has a Reader value */
  public Reader readerValue();

  /* Non-null if this field has a pre-tokenized ({@link TokenStream}) value */
  public TokenStream tokenStreamValue();

  // Numeric field:
  /* True if this field is numeric */
  public boolean numeric();

  /* Numeric {@link NumericField.DataType}; only used if
   * the field is numeric */
  public NumericField.DataType numericDataType();

  /* Numeric value; only used if the field is numeric */
  public Number numericValue();

  /**
   * Returns the IndexableFieldType describing the properties of this field
   *
   * @return IndexableFieldType for this field
   */
  public IndexableFieldType fieldType();
  
  /* Non-null if doc values should be indexed */
  public PerDocFieldValues docValues();

  /* DocValues type; only used if docValues is non-null */
  public ValueType docValuesType();
}

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
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.values.PerDocFieldValues;
import org.apache.lucene.index.values.ValueType;
import org.apache.lucene.util.BytesRef;

// TODO: how to handle versioning here...?

/** Represents a single field for indexing.  IndexWriter
 *  consumes Iterable<IndexableField> as a document.
 *
 *  @lucene.experimental */

public interface IndexableField {

  // TODO: add attrs to this API?

  public String name();

  // NOTE: if doc/field impl has the notion of "doc level boost"
  // it must be multiplied in w/ this field's boost
  public float boost();
  
  public boolean stored();

  public BytesRef binaryValue(BytesRef reuse);
  public String stringValue();
  public Reader readerValue();

  public TokenStream tokenStreamValue();

  // Numeric field:
  public boolean numeric();
  public NumericField.DataType numericDataType();
  public Number numericValue();

  // If this returns true then we index this field:
  public boolean indexed();

  public boolean tokenized();
  public boolean omitNorms();
  public IndexOptions indexOptions();

  public boolean storeTermVectors();
  public boolean storeTermVectorOffsets();
  public boolean storeTermVectorPositions();
  
  // doc values
  public boolean hasDocValues();  
  public PerDocFieldValues docValues();
  public ValueType docValuesType();
}

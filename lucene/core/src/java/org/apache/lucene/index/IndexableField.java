package org.apache.lucene.index;

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

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.search.similarities.DefaultSimilarity; // javadocs
import org.apache.lucene.search.similarities.Similarity; // javadocs
import org.apache.lucene.util.BytesRef;

// TODO: how to handle versioning here...?

// TODO: we need to break out separate StoredField...

/** Represents a single field for indexing.  IndexWriter
 *  consumes Iterable&lt;IndexableField&gt; as a document.
 *
 *  @lucene.experimental */

public interface IndexableField {

  /** Field name */
  public String name();

  /** {@link IndexableFieldType} describing the properties
   * of this field. */
  public IndexableFieldType fieldType();
  
  /** 
   * Returns the field's index-time boost.
   * <p>
   * Only fields can have an index-time boost, if you want to simulate
   * a "document boost", then you must pre-multiply it across all the
   * relevant fields yourself. 
   * <p>The boost is used to compute the norm factor for the field.  By
   * default, in the {@link Similarity#computeNorm(FieldInvertState)} method, 
   * the boost value is multiplied by the length normalization factor and then
   * rounded by {@link DefaultSimilarity#encodeNormValue(float)} before it is stored in the
   * index.  One should attempt to ensure that this product does not overflow
   * the range of that encoding.
   * <p>
   * It is illegal to return a boost other than 1.0f for a field that is not
   * indexed ({@link IndexableFieldType#indexed()} is false) or omits normalization values
   * ({@link IndexableFieldType#omitNorms()} returns true).
   *
   * @see Similarity#computeNorm(FieldInvertState)
   * @see DefaultSimilarity#encodeNormValue(float)
   */
  public float boost();

  /** Non-null if this field has a binary value */
  public BytesRef binaryValue();

  /** Non-null if this field has a string value */
  public String stringValue();

  /** Non-null if this field has a Reader value */
  public Reader readerValue();

  /** Non-null if this field has a numeric value */
  public Number numericValue();

  /**
   * Creates the TokenStream used for indexing this field.  If appropriate,
   * implementations should use the given Analyzer to create the TokenStreams.
   *
   * @param analyzer Analyzer that should be used to create the TokenStreams from
   * @return TokenStream value for indexing the document.  Should always return
   *         a non-null value if the field is to be indexed
   * @throws IOException Can be thrown while creating the TokenStream
   */
  public TokenStream tokenStream(Analyzer analyzer) throws IOException;
}

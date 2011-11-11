package org.apache.lucene.index.codecs;

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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermVectorMapper;

/**
 * Codec API for reading term vectors:
 * 
 * @lucene.experimental
 */
public abstract class TermVectorsReader implements Cloneable,Closeable {
  // TODO: it would be nice to present an improved API here, 
  // such as flex API (Fields + Terms etc) over the document
  public abstract TermFreqVector[] get(int doc) throws IOException;
  
  public abstract void get(int doc, TermVectorMapper mapper) throws IOException;
  public abstract void get(int doc, String field, TermVectorMapper mapper) throws IOException;

  public abstract TermVectorsReader clone();

  /**
   * Retrieve the term vector for the given document and field
   * @param docNum The document number to retrieve the vector for
   * @param field The field within the document to retrieve
   * @return The TermFreqVector for the document and field or null if there is no termVector for this field.
   * @throws IOException if there is an error reading the term vector files
   */
  // TODO: does this method belong here? This seems like sugar...
  public TermFreqVector get(int doc, String field) throws IOException {
    // Check if no term vectors are available for this segment at all
    ParallelArrayTermVectorMapper mapper = new ParallelArrayTermVectorMapper();
    get(doc, field, mapper);

    return mapper.materializeVector();
  }
}

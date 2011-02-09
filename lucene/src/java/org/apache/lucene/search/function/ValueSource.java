package org.apache.lucene.search.function;

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

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader.CompositeReaderContext;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.search.function.DocValues;

import java.io.IOException;

/**
 * Expert: source of values for basic function queries.
 * <P>At its default/simplest form, values - one per doc - are used as the score of that doc.
 * <P>Values are instantiated as 
 * {@link org.apache.lucene.search.function.DocValues DocValues} for a particular reader.
 * <P>ValueSource implementations differ in RAM requirements: it would always be a factor
 * of the number of documents, but for each document the number of bytes can be 1, 2, 4, or 8. 
 *
 * @lucene.experimental
 *
 *
 */
public abstract class ValueSource {

  /**
   * Return the DocValues used by the function query.
   * @param context the IndexReader used to read these values.
   * If any caching is involved, that caching would also be IndexReader based.  
   * @throws IOException for any error.
   */
  public abstract DocValues getValues(AtomicReaderContext context) throws IOException;
  
  /**
   * Return the DocValues used by the function query.
   * @deprecated (4.0) This method is temporary, to ease the migration to segment-based
   * searching. Please change your code to not pass {@link CompositeReaderContext} to these
   * APIs. Use {@link #getValues(IndexReader.AtomicReaderContext)} instead
   */
  @Deprecated
  public DocValues getValues(ReaderContext context) throws IOException {
    return getValues((AtomicReaderContext) context);
  }


  /** 
   * description of field, used in explain() 
   */
  public abstract String description();

  /* (non-Javadoc) @see java.lang.Object#toString() */
  @Override
  public String toString() {
    return description();
  }

  /**
   * Needed for possible caching of query results - used by {@link ValueSourceQuery#equals(Object)}.
   * @see Object#equals(Object)
   */
  @Override
  public abstract boolean equals(Object o);

  /**
   * Needed for possible caching of query results - used by {@link ValueSourceQuery#hashCode()}.
   * @see Object#hashCode()
   */
  @Override
  public abstract int hashCode();
  
}

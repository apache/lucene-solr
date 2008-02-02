package org.apache.lucene.search;

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

import java.util.BitSet;
import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.DocIdBitSet;

/** Abstract base class providing a mechanism to use a subset of an index
 *  for restriction or permission of index search results.
 *  <p>
 *  <b>Note:</b> In Lucene 3.0 {@link #bits(IndexReader)} will be removed
 *  and {@link #getDocIdSet(IndexReader)} will be defined as abstract.
 *  All implementing classes must therefore implement {@link #getDocIdSet(IndexReader)}
 *  in order to work with Lucene 3.0.
 */
public abstract class Filter implements java.io.Serializable {
  /**
   * @return A BitSet with true for documents which should be permitted in
   * search results, and false for those that should not.
   * @deprecated Use {@link #getDocIdSet(IndexReader)} instead.
   */
  public BitSet bits(IndexReader reader) throws IOException {
    return null;
  }
	
  /**
   * @return a DocIdSet that provides the documents which should be
   * permitted or prohibited in search results.
   * @see DocIdBitSet
   */
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    return new DocIdBitSet(bits(reader));
  }
}

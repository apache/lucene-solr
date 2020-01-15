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
package org.apache.lucene.search;

import java.util.List;
import java.util.concurrent.Executor;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;

/**
 * An {@link IndexSearcher} that has a smaller size limit for slices, allowing higher slice count with lesser
 * number of documents
 */
public class SmallSliceSizeIndexSearcher extends IndexSearcher {

  private static final int MAX_DOCS_PER_SLICE = 10;
  private static final int MAX_SEGMENTS_PER_SLICE = 5;

  /** Creates a searcher searching the provided index. Search on individual
   *  segments will be run in the provided {@link Executor}.
   * @see IndexSearcher#IndexSearcher(IndexReader, Executor) */
  public SmallSliceSizeIndexSearcher(IndexReader r, Executor executor) {
    super(r, executor);
  }

  @Override
  protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
    return slices(leaves, MAX_DOCS_PER_SLICE, MAX_SEGMENTS_PER_SLICE);
  }

}

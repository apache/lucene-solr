package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.IntsRef;

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

/**
 * Aggregates the categories of documents given to
 * {@link #aggregate(int, float, IntsRef)}. Note that the document IDs are local
 * to the reader given to {@link #setNextReader(AtomicReaderContext)}.
 * 
 * @lucene.experimental
 */
public interface Aggregator {

  /**
   * Sets the {@link AtomicReaderContext} for which
   * {@link #aggregate(int, float, IntsRef)} calls will be made. If this method
   * returns false, {@link #aggregate(int, float, IntsRef)} should not be called
   * for this reader.
   */
  public boolean setNextReader(AtomicReaderContext context) throws IOException;
  
  /**
   * Aggregate the ordinals of the given document ID (and its score). The given
   * ordinals offset is always zero.
   */
  public void aggregate(int docID, float score, IntsRef ordinals) throws IOException;
  
}

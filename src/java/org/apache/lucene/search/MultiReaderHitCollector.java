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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

/**
 * Expert: A HitCollector that can be used to collect hits
 * across sequential IndexReaders.  For a Multi*Reader, this
 * collector advances through each of the sub readers, in an
 * arbitrary order. This results in a higher performance
 * means of collection.
 *
 * <b>NOTE:</b> The doc that is passed to the collect method
 * is relative to the current reader.  You must re-base the
 * doc, by recording the docBase from the last setNextReader
 * call, to map it to the docID space of the
 * Multi*Reader.
 *
 * <b>NOTE:</b> This API is experimental and might change in
 * incompatible ways in the next release.
 */
public abstract class MultiReaderHitCollector extends HitCollector {
  /**
   * Called before collecting from each IndexReader. All doc 
   * ids in {@link #collect(int, float)} will correspond to reader.
   * 
   * Add docBase to the current IndexReaders internal document id to
   * re-base ids in {@link #collect(int, float)}.
   * 
   * @param reader next IndexReader
   * @param docBase
   * @throws IOException
   */
  public abstract void setNextReader(IndexReader reader, int docBase) throws IOException;
}

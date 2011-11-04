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

import java.util.ArrayList;

import org.apache.lucene.util.ReaderUtil;

/**
 * Acts like Lucene 4.x's SlowMultiReaderWrapper for testing 
 * of top-level MultiTermEnum, MultiTermDocs, ...
 */
public class SlowMultiReaderWrapper extends MultiReader {

  public SlowMultiReaderWrapper(IndexReader reader) {
    super(subReaders(reader));
  }
  
  private static IndexReader[] subReaders(IndexReader reader) {
    ArrayList<IndexReader> list = new ArrayList<IndexReader>();
    ReaderUtil.gatherSubReaders(list, reader);
    return list.toArray(new IndexReader[list.size()]);
  }

  @Override
  public IndexReader[] getSequentialSubReaders() {
    return null;
  }

  @Override
  public String toString() {
    return "SlowMultiReaderWrapper(" + super.toString() + ")";
  }
}

package org.apache.lucene.search.suggest.fst;

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
import java.util.Comparator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/**
 * Collects {@link BytesRef} and then allows one to iterate over their sorted order. Implementations
 * of this interface will be called in a single-threaded scenario.
 * @lucene.experimental
 * @lucene.internal  
 */
public interface BytesRefSorter {
  /**
   * Adds a single suggestion entry (possibly compound with its bucket).
   * 
   * @throws IOException If an I/O exception occurs.
   * @throws IllegalStateException If an addition attempt is performed after
   * a call to {@link #iterator()} has been made.
   */
  void add(BytesRef utf8) throws IOException, IllegalStateException;

  /**
   * Sorts the entries added in {@link #add(BytesRef)} and returns 
   * an iterator over all sorted entries.
   * 
   * @throws IOException If an I/O exception occurs.
   */
   BytesRefIterator iterator() throws IOException;
   
  /**
   * Comparator used to determine the sort order of entries.
   */
   Comparator<BytesRef> getComparator();
}

package org.apache.lucene.util;

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
import java.util.List;

import org.apache.lucene.index.IndexReader;

/**
 * Common util methods for dealing with {@link IndexReader}s.
 *
 */
public class ReaderUtil {

  /**
   * Gathers sub-readers from reader into a List.
   * 
   * @param allSubReaders
   * @param reader
   */
  public static void gatherSubReaders(List allSubReaders, IndexReader reader) {
    IndexReader[] subReaders = reader.getSequentialSubReaders();
    if (subReaders == null) {
      // Add the reader itself, and do not recurse
      allSubReaders.add(reader);
    } else {
      for (int i = 0; i < subReaders.length; i++) {
        gatherSubReaders(allSubReaders, subReaders[i]);
      }
    }
  }

  /**
   * Returns sub IndexReader that contains the given document id.
   *    
   * @param doc id of document
   * @param reader parent reader
   * @return sub reader of parent which contains the specified doc id
   */
  public static IndexReader subReader(int doc, IndexReader reader) {
    List subReadersList = new ArrayList();
    ReaderUtil.gatherSubReaders(subReadersList, reader);
    IndexReader[] subReaders = (IndexReader[]) subReadersList
        .toArray(new IndexReader[subReadersList.size()]);
    int[] docStarts = new int[subReaders.length];
    int maxDoc = 0;
    for (int i = 0; i < subReaders.length; i++) {
      docStarts[i] = maxDoc;
      maxDoc += subReaders[i].maxDoc();
    }
    return subReaders[ReaderUtil.subIndex(doc, docStarts)];
  }
  
  /**
   * Returns sub-reader subIndex from reader.
   * 
   * @param reader parent reader
   * @param subIndex index of desired sub reader
   * @return the subreader at subIndex
   */
  public static IndexReader subReader(IndexReader reader, int subIndex) {
    List subReadersList = new ArrayList();
    ReaderUtil.gatherSubReaders(subReadersList, reader);
    IndexReader[] subReaders = (IndexReader[]) subReadersList
        .toArray(new IndexReader[subReadersList.size()]);
    return subReaders[subIndex];
  }


  /**
   * Returns index of the searcher/reader for document <code>n</code> in the
   * array used to construct this searcher/reader.
   */
  public static int subIndex(int n, int[] docStarts) { // find
    // searcher/reader for doc n:
    int size = docStarts.length;
    int lo = 0; // search starts array
    int hi = size - 1; // for first element less than n, return its index
    while (hi >= lo) {
      int mid = (lo + hi) >>> 1;
      int midValue = docStarts[mid];
      if (n < midValue)
        hi = mid - 1;
      else if (n > midValue)
        lo = mid + 1;
      else { // found a match
        while (mid + 1 < size && docStarts[mid + 1] == midValue) {
          mid++; // scan to last match
        }
        return mid;
      }
    }
    return hi;
  }
}

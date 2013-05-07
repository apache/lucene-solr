package org.apache.lucene.index.sorter;

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
import java.util.Arrays;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util._TestUtil;
import org.junit.BeforeClass;

public class SortingAtomicReaderTest extends SorterTestBase {
  
  @BeforeClass
  public static void beforeClassSortingAtomicReaderTest() throws Exception {
    // build the mapping from the reader, since we deleted documents, some of
    // them might have disappeared from the index (e.g. if an entire segment is
    // dropped b/c all its docs are deleted)
    final int[] values = new int[reader.maxDoc()];
    for (int i = 0; i < reader.maxDoc(); i++) {
      values[i] = Integer.valueOf(reader.document(i).get(ID_FIELD));
    }
    final Sorter.DocComparator comparator = new Sorter.DocComparator() {
      @Override
      public int compare(int docID1, int docID2) {
        final int v1 = values[docID1];
        final int v2 = values[docID2];
        return v1 < v2 ? -1 : v1 == v2 ? 0 : 1;
      }
    };

    final Sorter.DocMap docMap = Sorter.sort(reader.maxDoc(), comparator);
    // Sorter.compute also sorts the values
    sortedValues = new Integer[reader.maxDoc()];
    for (int i = 0; i < reader.maxDoc(); ++i) {
      sortedValues[docMap.oldToNew(i)] = values[i];
    }
    if (VERBOSE) {
      System.out.println("docMap: " + docMap);
      System.out.println("sortedValues: " + Arrays.toString(sortedValues));
    }
    
    reader = SortingAtomicReader.wrap(reader, new Sorter() {
      @Override
      public Sorter.DocMap sort(AtomicReader reader) throws IOException {
        return docMap;
      }
      @Override
      public String getID() {
        return ID_FIELD;
      }
    });
    
    if (VERBOSE) {
      System.out.print("mapped-deleted-docs: ");
      Bits mappedLiveDocs = reader.getLiveDocs();
      for (int i = 0; i < mappedLiveDocs.length(); i++) {
        if (!mappedLiveDocs.get(i)) {
          System.out.print(i + " ");
        }
      }
      System.out.println();
    }
    
    _TestUtil.checkReader(reader);
  }

}

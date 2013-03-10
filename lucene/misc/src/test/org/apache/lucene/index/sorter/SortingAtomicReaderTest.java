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
import java.util.Collections;

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
    Integer[] values = new Integer[reader.maxDoc()];
    int[] docs = new int[reader.maxDoc()];
    for (int i = 0; i < reader.maxDoc(); i++) {
      docs[i] = i;
      values[i] = Integer.valueOf(reader.document(i).get(ID_FIELD));
    }

    final int[] oldToNew = Sorter.compute(docs, Collections.unmodifiableList(Arrays.asList(values)));
    // Sorter.compute also sorts the values
    sortedValues = new Integer[reader.maxDoc()];
    for (int i = 0; i < reader.maxDoc(); ++i) {
      sortedValues[oldToNew[i]] = values[i];
    }
    if (VERBOSE) {
      System.out.println("oldToNew: " + Arrays.toString(oldToNew));
      System.out.println("sortedValues: " + Arrays.toString(sortedValues));
    }
    
    reader = new SortingAtomicReader(reader, new Sorter() {
      @Override
      public int[] oldToNew(AtomicReader reader) throws IOException {
        return oldToNew;
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

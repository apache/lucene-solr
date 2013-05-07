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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util._TestUtil;
import org.junit.BeforeClass;

public class IndexSortingTest extends SorterTestBase {
  
  private static final Sorter[] SORTERS = new Sorter[] {
    new NumericDocValuesSorter(NUMERIC_DV_FIELD, true),
    Sorter.REVERSE_DOCS,
  };
  
  @BeforeClass
  public static void beforeClassSorterUtilTest() throws Exception {
    // only read the values of the undeleted documents, since after addIndexes,
    // the deleted ones will be dropped from the index.
    Bits liveDocs = reader.getLiveDocs();
    List<Integer> values = new ArrayList<Integer>();
    for (int i = 0; i < reader.maxDoc(); i++) {
      if (liveDocs == null || liveDocs.get(i)) {
        values.add(Integer.valueOf(reader.document(i).get(ID_FIELD)));
      }
    }
    Sorter sorter = SORTERS[random().nextInt(SORTERS.length)];
    if (sorter == Sorter.REVERSE_DOCS) {
      Collections.reverse(values);
    } else {
      Collections.sort(values);
      if (sorter instanceof NumericDocValuesSorter && random().nextBoolean()) {
        sorter = new NumericDocValuesSorter(NUMERIC_DV_FIELD, false); // descending
        Collections.reverse(values);
      }
    }
    sortedValues = values.toArray(new Integer[values.size()]);
    if (VERBOSE) {
      System.out.println("sortedValues: " + sortedValues);
      System.out.println("Sorter: " + sorter);
    }

    Directory target = newDirectory();
    IndexWriter writer = new IndexWriter(target, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    reader = SortingAtomicReader.wrap(reader, sorter);
    writer.addIndexes(reader);
    writer.close();
    reader.close();
    dir.close();
    
    // CheckIndex the target directory
    dir = target;
    _TestUtil.checkIndex(dir);
    
    // set reader for tests
    reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir));
    assertFalse("index should not have deletions", reader.hasDeletions());
  }
  
}

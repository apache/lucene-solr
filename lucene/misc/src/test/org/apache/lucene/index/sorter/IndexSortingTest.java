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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.TestUtil;
import org.junit.BeforeClass;

public class IndexSortingTest extends SorterTestBase {
  
  private static final Sort[] SORT = new Sort[] {
    new Sort(new SortField(NUMERIC_DV_FIELD, SortField.Type.LONG)),
    new Sort(new SortField(null, SortField.Type.DOC, true))
  };
  
  @BeforeClass
  public static void beforeClassSorterUtilTest() throws Exception {
    // only read the values of the undeleted documents, since after addIndexes,
    // the deleted ones will be dropped from the index.
    Bits liveDocs = reader.getLiveDocs();
    List<Integer> values = new ArrayList<>();
    for (int i = 0; i < reader.maxDoc(); i++) {
      if (liveDocs == null || liveDocs.get(i)) {
        values.add(Integer.valueOf(reader.document(i).get(ID_FIELD)));
      }
    }
    int idx = random().nextInt(SORT.length);
    Sort sorter = SORT[idx];
    if (idx == 1) { // reverse doc sort
      Collections.reverse(values);
    } else {
      Collections.sort(values);
      if (random().nextBoolean()) {
        sorter = new Sort(new SortField(NUMERIC_DV_FIELD, SortField.Type.LONG, true)); // descending
        Collections.reverse(values);
      }
    }
    sortedValues = values.toArray(new Integer[values.size()]);
    if (VERBOSE) {
      System.out.println("sortedValues: " + sortedValues);
      System.out.println("Sorter: " + sorter);
    }

    Directory target = newDirectory();
    IndexWriter writer = new IndexWriter(target, newIndexWriterConfig(null));
    reader = SortingAtomicReader.wrap(reader, sorter);
    writer.addIndexes(reader);
    writer.close();
    reader.close();
    dir.close();
    
    // CheckIndex the target directory
    dir = target;
    TestUtil.checkIndex(dir);
    
    // set reader for tests
    reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir));
    assertFalse("index should not have deletions", reader.hasDeletions());
  }
  
}

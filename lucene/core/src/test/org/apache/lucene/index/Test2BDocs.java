package org.apache.lucene.index;

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

import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class Test2BDocs extends LuceneTestCase {
  static Directory dir;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newFSDirectory(_TestUtil.getTempDir("2Bdocs"));
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
    Document doc = new Document();
    for (int i = 0; i < 262144; i++) {
      iw.addDocument(doc);
    }
    iw.forceMerge(1);
    iw.close();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    dir.close();
    dir = null;
  }

  public void testOverflow() throws Exception {
    DirectoryReader ir = DirectoryReader.open(dir);
    IndexReader subReaders[] = new IndexReader[8192];
    Arrays.fill(subReaders, ir);
    try {
      new MultiReader(subReaders);
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
    ir.close();
  }
  
  public void testExactlyAtLimit() throws Exception {
    Directory dir2 = newFSDirectory(_TestUtil.getTempDir("2BDocs2"));
    IndexWriter iw = new IndexWriter(dir2, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
    Document doc = new Document();
    for (int i = 0; i < 262143; i++) {
      iw.addDocument(doc);
    }
    iw.close();
    DirectoryReader ir = DirectoryReader.open(dir);
    DirectoryReader ir2 = DirectoryReader.open(dir2);
    IndexReader subReaders[] = new IndexReader[8192];
    Arrays.fill(subReaders, ir);
    subReaders[subReaders.length-1] = ir2;
    MultiReader mr = new MultiReader(subReaders);
    assertEquals(Integer.MAX_VALUE, mr.maxDoc());
    assertEquals(Integer.MAX_VALUE, mr.numDocs());
    ir.close();
    ir2.close();
    dir2.close();
  }
}

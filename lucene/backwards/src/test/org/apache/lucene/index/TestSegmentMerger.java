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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.document.Document;

public class TestSegmentMerger extends LuceneTestCase {
  //The variables for the new merged segment
  private Directory mergedDir = new RAMDirectory();
  //First segment to be merged
  private Directory merge1Dir = new RAMDirectory();
  private Document doc1 = new Document();
  private SegmentReader reader1 = null;
  //Second Segment to be merged
  private Directory merge2Dir = new RAMDirectory();
  private Document doc2 = new Document();
  private SegmentReader reader2 = null;
  

  public TestSegmentMerger(String s) {
    super(s);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    DocHelper.setupDoc(doc1);
    SegmentInfo info1 = DocHelper.writeDoc(merge1Dir, doc1);
    DocHelper.setupDoc(doc2);
    SegmentInfo info2 = DocHelper.writeDoc(merge2Dir, doc2);
    reader1 = SegmentReader.get(true, info1, IndexReader.DEFAULT_TERMS_INDEX_DIVISOR);
    reader2 = SegmentReader.get(true, info2, IndexReader.DEFAULT_TERMS_INDEX_DIVISOR);
  }

  public void test() {
    assertTrue(mergedDir != null);
    assertTrue(merge1Dir != null);
    assertTrue(merge2Dir != null);
    assertTrue(reader1 != null);
    assertTrue(reader2 != null);
  }
}

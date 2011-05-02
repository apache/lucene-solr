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
package org.apache.lucene.index;

import java.io.File;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestIndexSplitter extends LuceneTestCase {
  public void test() throws Exception {
    File dir = new File(TEMP_DIR, "testfilesplitter");
    _TestUtil.rmDir(dir);
    dir.mkdirs();
    File destDir = new File(TEMP_DIR, "testfilesplitterdest");
    _TestUtil.rmDir(destDir);
    destDir.mkdirs();
    FSDirectory fsDir = FSDirectory.open(dir);

    LogMergePolicy mergePolicy = new LogByteSizeMergePolicy();
    mergePolicy.setNoCFSRatio(1);
    IndexWriter iw = new IndexWriter(
        fsDir,
        new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setOpenMode(OpenMode.CREATE).
            setMergePolicy(mergePolicy)
    );
    for (int x=0; x < 100; x++) {
      Document doc = TestIndexWriterReader.createDocument(x, "index", 5);
      iw.addDocument(doc);
    }
    iw.commit();
    for (int x=100; x < 150; x++) {
      Document doc = TestIndexWriterReader.createDocument(x, "index2", 5);
      iw.addDocument(doc);
    }
    iw.commit();
    for (int x=150; x < 200; x++) {
      Document doc = TestIndexWriterReader.createDocument(x, "index3", 5);
      iw.addDocument(doc);
    }
    iw.commit();
    assertEquals(3, iw.getReader().getSequentialSubReaders().length);
    iw.close();
    // we should have 2 segments now
    IndexSplitter is = new IndexSplitter(dir);
    String splitSegName = is.infos.info(1).name;
    is.split(destDir, new String[] {splitSegName});
    IndexReader r = IndexReader.open(FSDirectory.open(destDir), true);
    assertEquals(50, r.maxDoc());
    
    // now test cmdline
    File destDir2 = new File(TEMP_DIR, "testfilesplitterdest2");
    _TestUtil.rmDir(destDir2);
    destDir2.mkdirs();
    IndexSplitter.main(new String[] {dir.getAbsolutePath(), destDir2.getAbsolutePath(), splitSegName});
    assertEquals(3, destDir2.listFiles().length);
    r = IndexReader.open(FSDirectory.open(destDir2), true);
    assertEquals(50, r.maxDoc());
    
    // now remove the copied segment from src
    IndexSplitter.main(new String[] {dir.getAbsolutePath(), "-d", splitSegName});
    r = IndexReader.open(FSDirectory.open(dir), true);
    assertEquals(2, r.getSequentialSubReaders().length);
  }
}

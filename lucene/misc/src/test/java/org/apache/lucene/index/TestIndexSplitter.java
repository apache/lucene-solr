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
package org.apache.lucene.index;

import java.nio.file.Path;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;

public class TestIndexSplitter extends LuceneTestCase {
  public void test() throws Exception {
    Path dir = createTempDir(LuceneTestCase.getTestClass().getSimpleName());
    Path destDir = createTempDir(LuceneTestCase.getTestClass().getSimpleName());
    Directory fsDir = newFSDirectory(dir);
    // IndexSplitter.split makes its own commit directly with SIPC/SegmentInfos,
    // so the unreferenced files are expected.
    if (fsDir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)fsDir).setAssertNoUnrefencedFilesOnClose(false);
    }

    MergePolicy mergePolicy = new LogByteSizeMergePolicy();
    mergePolicy.setNoCFSRatio(1.0);
    mergePolicy.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
    IndexWriter iw = new IndexWriter(
        fsDir,
        new IndexWriterConfig(new MockAnalyzer(random())).
            setOpenMode(OpenMode.CREATE).
            setMergePolicy(mergePolicy)
    );
    for (int x=0; x < 100; x++) {
      Document doc = DocHelper.createDocument(x, "index", 5);
      iw.addDocument(doc);
    }
    iw.commit();
    for (int x=100; x < 150; x++) {
      Document doc = DocHelper.createDocument(x, "index2", 5);
      iw.addDocument(doc);
    }
    iw.commit();
    for (int x=150; x < 200; x++) {
      Document doc = DocHelper.createDocument(x, "index3", 5);
      iw.addDocument(doc);
    }
    iw.commit();
    DirectoryReader iwReader = iw.getReader();
    assertEquals(3, iwReader.leaves().size());
    iwReader.close();
    iw.close();
    // we should have 2 segments now
    IndexSplitter is = new IndexSplitter(dir);
    String splitSegName = is.infos.info(1).info.name;
    is.split(destDir, new String[] {splitSegName});
    Directory fsDirDest = newFSDirectory(destDir);
    DirectoryReader r = DirectoryReader.open(fsDirDest);
    assertEquals(50, r.maxDoc());
    r.close();
    fsDirDest.close();
    
    // now test cmdline
    Path destDir2 = createTempDir(LuceneTestCase.getTestClass().getSimpleName());
    IndexSplitter.main(new String[] {dir.toAbsolutePath().toString(), destDir2.toAbsolutePath().toString(), splitSegName});
    Directory fsDirDest2 = newFSDirectory(destDir2);
    SegmentInfos sis = SegmentInfos.readLatestCommit(fsDirDest2);
    assertEquals(1, sis.size());
    r = DirectoryReader.open(fsDirDest2);
    assertEquals(50, r.maxDoc());
    r.close();
    fsDirDest2.close();
    
    // now remove the copied segment from src
    IndexSplitter.main(new String[] {dir.toAbsolutePath().toString(), "-d", splitSegName});
    r = DirectoryReader.open(fsDir);
    assertEquals(2, r.leaves().size());
    r.close();
    fsDir.close();
  }

}

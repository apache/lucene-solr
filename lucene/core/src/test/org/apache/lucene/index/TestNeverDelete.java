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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

// Make sure if you use NoDeletionPolicy that no file
// referenced by a commit point is ever deleted

public class TestNeverDelete extends LuceneTestCase {

  public void testIndexing() throws Exception {
    final Path tmpDir = createTempDir("TestNeverDelete");
    final BaseDirectoryWrapper d = newFSDirectory(tmpDir);

    final RandomIndexWriter w = new RandomIndexWriter(random(),
                                                      d,
                                                      newIndexWriterConfig(new MockAnalyzer(random()))
                                                        .setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE));
    w.w.getConfig().setMaxBufferedDocs(TestUtil.nextInt(random(), 5, 30));

    w.commit();
    Thread[] indexThreads = new Thread[random().nextInt(4)];
    final long stopTime = System.currentTimeMillis() + atLeast(1000);
    for (int x=0; x < indexThreads.length; x++) {
      indexThreads[x] = new Thread() {
          @Override
          public void run() {
            try {
              int docCount = 0;
              while (System.currentTimeMillis() < stopTime) {
                final Document doc = new Document();
                doc.add(newStringField("dc", ""+docCount, Field.Store.YES));
                doc.add(newTextField("field", "here is some text", Field.Store.YES));
                w.addDocument(doc);

                if (docCount % 13 == 0) {
                  w.commit();
                }
                docCount++;
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
      indexThreads[x].setName("Thread " + x);
      indexThreads[x].start();
    }

    final Set<String> allFiles = new HashSet<>();

    DirectoryReader r = DirectoryReader.open(d);
    while(System.currentTimeMillis() < stopTime) {
      final IndexCommit ic = r.getIndexCommit();
      if (VERBOSE) {
        System.out.println("TEST: check files: " + ic.getFileNames());
      }
      allFiles.addAll(ic.getFileNames());
      // Make sure no old files were removed
      for(String fileName : allFiles) {
        assertTrue("file " + fileName + " does not exist", slowFileExists(d, fileName));
      }
      DirectoryReader r2 = DirectoryReader.openIfChanged(r);
      if (r2 != null) {
        r.close();
        r = r2;
      }
      Thread.sleep(1);
    }
    r.close();

    for(Thread t : indexThreads) {
      t.join();
    }
    w.close();
    d.close();
  }
}

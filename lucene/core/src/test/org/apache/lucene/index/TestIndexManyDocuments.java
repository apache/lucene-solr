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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestIndexManyDocuments extends LuceneTestCase {

  public void test() throws Exception {
    Directory dir = newFSDirectory(createTempDir());
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 2000));

    int numDocs = atLeast(10000);

    final IndexWriter w = new IndexWriter(dir, iwc);
    final AtomicInteger count = new AtomicInteger();
    Thread[] threads = new Thread[2];
    for(int i=0;i<threads.length;i++) {
      threads[i] = new Thread() {
          @Override
          public void run() {
           while (count.getAndIncrement() < numDocs) {
              Document doc = new Document();
              doc.add(newTextField("field", "text", Field.Store.NO));
              try {
                w.addDocument(doc);
              } catch (IOException ioe) {
                throw new RuntimeException(ioe);
              }
            }
          }
        };
      threads[i].start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    assertEquals("lost " + (numDocs - w.getDocStats().maxDoc) + " documents; maxBufferedDocs=" + iwc.getMaxBufferedDocs(), numDocs, w.getDocStats().maxDoc);
    w.close();
             
    IndexReader r = DirectoryReader.open(dir);
    assertEquals(numDocs, r.maxDoc());
    IOUtils.close(r, dir);
  }
}

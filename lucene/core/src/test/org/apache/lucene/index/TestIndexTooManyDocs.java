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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestIndexTooManyDocs extends LuceneTestCase {

  /*
   * This test produces a boat load of very small segments with lot of deletes which are likely deleting
   * the entire segment. see https://issues.apache.org/jira/browse/LUCENE-8043
   */
  public void testIndexTooManyDocs() throws IOException, InterruptedException {
    Directory dir = newDirectory();
    int numMaxDoc = 25;
    IndexWriterConfig config = new IndexWriterConfig();
    config.setRAMBufferSizeMB(0.000001); // force lots of small segments and logs of concurrent deletes
    IndexWriter writer = new IndexWriter(dir, config);
    try {
      IndexWriter.setMaxDocs(numMaxDoc);
      int numThreads = 5 + random().nextInt(5);
      Thread[] threads = new Thread[numThreads];
      CountDownLatch latch = new CountDownLatch(numThreads);
      CountDownLatch indexingDone = new CountDownLatch(numThreads - 2);
      AtomicBoolean done = new AtomicBoolean(false);
      for (int i = 0; i < numThreads; i++) {
        if (i >= 2) {
          threads[i] = new Thread(() -> {
            latch.countDown();
            try {
              try {
                latch.await();
              } catch (InterruptedException e) {
                throw new AssertionError(e);
              }
              for (int d = 0; d < 100; d++) {
                Document doc = new Document();
                String id = Integer.toString(random().nextInt(numMaxDoc * 2));
                doc.add(new StringField("id", id, Field.Store.NO));
                try {
                  Term t = new Term("id", id);
                  if (random().nextInt(5) == 0) {
                    writer.deleteDocuments(new TermQuery(t));
                  }
                  writer.updateDocument(t, doc);
                } catch (IOException e) {
                  throw new AssertionError(e);
                } catch (IllegalArgumentException e) {
                  assertEquals("number of documents in the index cannot exceed " + IndexWriter.getActualMaxDocs(), e.getMessage());
                }
              }
            } finally {
              indexingDone.countDown();
            }
          });
        } else {
          threads[i] = new Thread(() -> {
            try {
              latch.countDown();
              latch.await();
              DirectoryReader open = DirectoryReader.open(writer, true, true);
              while (done.get() == false) {
                DirectoryReader directoryReader = DirectoryReader.openIfChanged(open);
                if (directoryReader != null) {
                  open.close();
                  open = directoryReader;
                }
              }
              IOUtils.closeWhileHandlingException(open);
            } catch (Exception e) {
              throw new AssertionError(e);
            }
          });
        }
        threads[i].start();
      }

      indexingDone.await();
      done.set(true);


      for (int i = 0; i < numThreads; i++) {
        threads[i].join();
      }
      writer.close();
      dir.close();
    } finally {
      IndexWriter.setMaxDocs(IndexWriter.MAX_DOCS);
    }
  }
}
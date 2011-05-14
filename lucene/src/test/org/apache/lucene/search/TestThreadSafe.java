package org.apache.lucene.search;
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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.*;

import java.util.Random;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.IOException;

public class TestThreadSafe extends LuceneTestCase {
  Directory dir1;

  IndexReader ir1;

  class Thr extends Thread {
    final int iter;
    final Random rand;
    final AtomicBoolean failed;

    // pass in random in case we want to make things reproducable
    public Thr(int iter, Random rand, AtomicBoolean failed) {
      this.iter = iter;
      this.rand = rand;
      this.failed = failed;
    }

    @Override
    public void run() {
      try {
        for (int i=0; i<iter; i++) {
          /*** future
           // pick a random index reader... a shared one, or create your own
           IndexReader ir;
           ***/

          switch(rand.nextInt(1)) {
            case 0: loadDoc(ir1); break;
          }

        }
      } catch (Throwable th) {
        failed.set(true);
        throw new RuntimeException(th);
      }
    }


    void loadDoc(IndexReader ir) throws IOException {
      // beware of deleted docs in the future
      Document doc = ir.document(rand.nextInt(ir.maxDoc()),
                new FieldSelector() {
                  public FieldSelectorResult accept(String fieldName) {
                    switch(rand.nextInt(2)) {
                      case 0: return FieldSelectorResult.LAZY_LOAD;
                      case 1: return FieldSelectorResult.LOAD;
                      // TODO: add other options
                      default: return FieldSelectorResult.LOAD;
                    }
                  }
                }
              );

      List<Fieldable> fields = doc.getFields();
      for (final Fieldable f : fields ) {
        validateField(f);
      }

    }

  }


  void validateField(Fieldable f) {
    String val = f.stringValue();
    if (!val.startsWith("^") || !val.endsWith("$")) {
      throw new RuntimeException("Invalid field:" + f.toString() + " val=" +val);
    }
  }

  String[] words = "now is the time for all good men to come to the aid of their country".split(" ");

  void buildDir(Directory dir, int nDocs, int maxFields, int maxFieldLen) throws IOException {
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.CREATE).setMaxBufferedDocs(10));
    for (int j=0; j<nDocs; j++) {
      Document d = new Document();
      int nFields = random.nextInt(maxFields);
      for (int i=0; i<nFields; i++) {
        int flen = random.nextInt(maxFieldLen);
        StringBuilder sb = new StringBuilder("^ ");
        while (sb.length() < flen) sb.append(' ').append(words[random.nextInt(words.length)]);
        sb.append(" $");
        Field.Store store = Field.Store.YES;  // make random later
        Field.Index index = Field.Index.ANALYZED;  // make random later
        d.add(newField("f"+i, sb.toString(), store, index));
      }
      iw.addDocument(d);
    }
    iw.close();
  }


  void doTest(int iter, int nThreads) throws Exception {
    Thr[] tarr = new Thr[nThreads];
    AtomicBoolean failed = new AtomicBoolean();
    for (int i=0; i<nThreads; i++) {
      tarr[i] = new Thr(iter, new Random(random.nextLong()), failed);
      tarr[i].start();
    }
    for (int i=0; i<nThreads; i++) {
      tarr[i].join();
    }
    assertFalse(failed.get());
  }

  public void testLazyLoadThreadSafety() throws Exception{
    dir1 = newDirectory();
    // test w/ field sizes bigger than the buffer of an index input
    buildDir(dir1, 15, 5, 2000);

    // do many small tests so the thread locals go away inbetween
    int num = 10 * RANDOM_MULTIPLIER;
    for (int i = 0; i < num; i++) {
      ir1 = IndexReader.open(dir1, false);
      doTest(10,10);
      ir1.close();
    }
    dir1.close();
  }

}

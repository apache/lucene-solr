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

import junit.framework.TestCase;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.*;

import java.util.Random;
import java.util.List;
import java.io.IOException;

/**
 * 
 * @version $Id$
 */
public class TestThreadSafe extends LuceneTestCase {
  Random r;
  Directory dir1;
  Directory dir2;

  IndexReader ir1;
  IndexReader ir2;

  String failure=null;


  class Thr extends Thread {
    final int iter;
    final Random rand;
    // pass in random in case we want to make things reproducable
    public Thr(int iter, Random rand) {
      this.iter = iter;
      this.rand = rand;
    }

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
        failure=th.toString();
        TestCase.fail(failure);
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

      List fields = doc.getFields();
      for (int i=0; i<fields.size(); i++) {
        Fieldable f = (Fieldable)fields.get(i);
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
    IndexWriter iw = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    iw.setMaxBufferedDocs(10);
    for (int j=0; j<nDocs; j++) {
      Document d = new Document();
      int nFields = r.nextInt(maxFields);
      for (int i=0; i<nFields; i++) {
        int flen = r.nextInt(maxFieldLen);
        StringBuffer sb = new StringBuffer("^ ");
        while (sb.length() < flen) sb.append(' ').append(words[r.nextInt(words.length)]);
        sb.append(" $");
        Field.Store store = Field.Store.YES;  // make random later
        Field.Index index = Field.Index.ANALYZED;  // make random later
        d.add(new Field("f"+i, sb.toString(), store, index));
      }
      iw.addDocument(d);
    }
    iw.close();
  }


  void doTest(int iter, int nThreads) throws Exception {
    Thr[] tarr = new Thr[nThreads];
    for (int i=0; i<nThreads; i++) {
      tarr[i] = new Thr(iter, new Random(r.nextLong()));
      tarr[i].start();
    }
    for (int i=0; i<nThreads; i++) {
      tarr[i].join();
    }
    if (failure!=null) {
      TestCase.fail(failure);
    }
  }

  public void testLazyLoadThreadSafety() throws Exception{
    r = newRandom();
    dir1 = new RAMDirectory();
    // test w/ field sizes bigger than the buffer of an index input
    buildDir(dir1, 15, 5, 2000);

    // do many small tests so the thread locals go away inbetween
    for (int i=0; i<100; i++) {
      ir1 = IndexReader.open(dir1);
      doTest(10,100);
    }
  }

}

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


import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Monster;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.TimeUnits;

@SuppressCodecs({"SimpleText", "Direct"})
@TimeoutSuite(millis = 80 * TimeUnits.HOUR) // effectively no limit
@Monster("Takes ~30min")
@SuppressSysoutChecks(bugUrl = "Stuff gets printed")
public class Test2BDocs extends LuceneTestCase {
  
  // indexes Integer.MAX_VALUE docs with indexed field(s)
  public void test2BDocs() throws Exception {
    BaseDirectoryWrapper dir = newFSDirectory(createTempDir("2BDocs"));
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    
    IndexWriter w = new IndexWriter(dir,
        new IndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setRAMBufferSizeMB(256.0)
        .setMergeScheduler(new ConcurrentMergeScheduler())
        .setMergePolicy(newLogMergePolicy(false, 10))
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        .setCodec(TestUtil.getDefaultCodec()));

    Document doc = new Document();
    Field field = new Field("f1", "a", StringField.TYPE_NOT_STORED);
    doc.add(field);
    
    for (int i = 0; i < IndexWriter.MAX_DOCS; i++) {
      w.addDocument(doc);
      if (i % (10*1000*1000) == 0) {
        System.out.println("indexed: " + i);
        System.out.flush();
      }
    }
    
    w.forceMerge(1);
    w.close();
    
    System.out.println("verifying...");
    System.out.flush();
    
    DirectoryReader r = DirectoryReader.open(dir);

    BytesRef term = new BytesRef(1);
    term.bytes[0] = (byte)'a';
    term.length = 1;

    long skips = 0;

    Random rnd = random();

    long start = System.nanoTime();

    for (LeafReaderContext context : r.leaves()) {
      LeafReader reader = context.reader();
      int lim = context.reader().maxDoc();

      Terms terms = reader.terms("f1");
      for (int i=0; i<10000; i++) {
        TermsEnum te = terms.iterator();
        assertTrue( te.seekExact(term) );
        PostingsEnum docs = te.postings(null);

        // skip randomly through the term
        for (int target = -1;;)
        {
          int maxSkipSize = lim - target + 1;
          // do a smaller skip half of the time
          if (rnd.nextBoolean()) {
            maxSkipSize = Math.min(256, maxSkipSize);
          }
          int newTarget = target + rnd.nextInt(maxSkipSize) + 1;
          if (newTarget >= lim) {
            if (target+1 >= lim) break; // we already skipped to end, so break.
            newTarget = lim-1;  // skip to end
          }
          target = newTarget;

          int res = docs.advance(target);
          if (res == PostingsEnum.NO_MORE_DOCS) break;

          assertTrue( res >= target );

          skips++;
          target = res;
        }
      }
    }
    
    r.close();
    dir.close();

    long end = System.nanoTime();

    System.out.println("Skip count=" + skips + " seconds=" + TimeUnit.NANOSECONDS.toSeconds(end-start));
    assert skips > 0;
  }
  
}

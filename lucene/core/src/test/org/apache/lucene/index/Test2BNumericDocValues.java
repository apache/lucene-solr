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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TimeUnits;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.junit.Ignore;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

@TimeoutSuite(millis = 80 * TimeUnits.HOUR)
@Ignore("takes ~ 30 minutes")
@SuppressCodecs("Lucene3x")
public class Test2BNumericDocValues extends LuceneTestCase {
  
  // indexes Integer.MAX_VALUE docs with an increasing dv field
  public void testNumerics() throws Exception {
    BaseDirectoryWrapper dir = newFSDirectory(_TestUtil.getTempDir("2BNumerics"));
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    
    IndexWriter w = new IndexWriter(dir,
        new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setRAMBufferSizeMB(256.0)
        .setMergeScheduler(new ConcurrentMergeScheduler())
        .setMergePolicy(newLogMergePolicy(false, 10))
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE));

    Document doc = new Document();
    NumericDocValuesField dvField = new NumericDocValuesField("dv", 0);
    doc.add(dvField);
    
    for (int i = 0; i < Integer.MAX_VALUE; i++) {
      dvField.setLongValue(i);
      w.addDocument(doc);
      if (i % 100000 == 0) {
        System.out.println("indexed: " + i);
        System.out.flush();
      }
    }
    
    w.forceMerge(1);
    w.close();
    
    System.out.println("verifying...");
    System.out.flush();
    
    DirectoryReader r = DirectoryReader.open(dir);
    long expectedValue = 0;
    for (AtomicReaderContext context : r.leaves()) {
      AtomicReader reader = context.reader();
      NumericDocValues dv = reader.getNumericDocValues("dv");
      for (int i = 0; i < reader.maxDoc(); i++) {
        assertEquals(expectedValue, dv.get(i));
        expectedValue++;
      }
    }
    
    r.close();
    dir.close();
  }
}

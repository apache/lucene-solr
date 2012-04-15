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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase.UseNoMemoryExpensiveCodec;

/**
 * Test indexes ~82M docs with 26 terms each, so you get > Integer.MAX_VALUE terms/docs pairs
 * @lucene.experimental
 */
@UseNoMemoryExpensiveCodec
public class Test2BPostings extends LuceneTestCase {

  @Nightly
  public void test() throws Exception {
    MockDirectoryWrapper dir = newFSDirectory(_TestUtil.getTempDir("2BPostings"));
    dir.setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    dir.setCheckIndexOnClose(false); // don't double-checkindex
    
    IndexWriter w = new IndexWriter(dir,
        new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setRAMBufferSizeMB(256.0)
        .setMergeScheduler(new ConcurrentMergeScheduler())
        .setMergePolicy(newLogMergePolicy(false, 10))
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE));

    MergePolicy mp = w.getConfig().getMergePolicy();
    if (mp instanceof LogByteSizeMergePolicy) {
     // 1 petabyte:
     ((LogByteSizeMergePolicy) mp).setMaxMergeMB(1024*1024*1024);
    }

    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_UNSTORED);
    ft.setOmitNorms(true);
    ft.setIndexOptions(IndexOptions.DOCS_ONLY);
    Field field = new Field("field", new MyTokenStream(), ft);
    doc.add(field);
    
    final int numDocs = (Integer.MAX_VALUE / 26) + 1;
    for (int i = 0; i < numDocs; i++) {
      w.addDocument(doc);
      if (VERBOSE && i % 100000 == 0) {
        System.out.println(i + " of " + numDocs + "...");
      }
    }
    w.forceMerge(1);
    w.close();
    CheckIndex ci = new CheckIndex(dir);
    if (VERBOSE) {
      ci.setInfoStream(System.out);
    }
    ci.checkIndex();
    dir.close();
  }
  
  public static final class MyTokenStream extends TokenStream {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final char buffer[];
    int index;

    public MyTokenStream() {
      termAtt.setLength(1);
      buffer = termAtt.buffer();
    }
    
    @Override
    public boolean incrementToken() throws IOException {
      if (index <= 'z') {
        buffer[0] = (char) index++;
        return true;
      }
      return false;
    }
    
    @Override
    public void reset() throws IOException {
      index = 'a';
    }
  }
}

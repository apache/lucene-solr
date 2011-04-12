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

import org.apache.lucene.util.*;
import org.apache.lucene.store.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.document.*;
import java.io.IOException;
import org.junit.Ignore;

// Best to run this test w/ plenty of RAM (because of the
// terms index):
//
//   ant compile-test
//
//   java -server -Xmx8g -d64 -cp .:lib/junit-4.7.jar:./build/classes/test:./build/classes/java -Dlucene.version=4.0-dev -Dtests.directory=SimpleFSDirectory -DtempDir=build -ea org.junit.runner.JUnitCore org.apache.lucene.index.Test2BTerms
//

public class Test2BTerms extends LuceneTestCase {

  private static final class MyTokenStream extends TokenStream {

    private final int tokensPerDoc;
    private int tokenCount;
    private final CharTermAttribute charTerm;
    private final static int TOKEN_LEN = 5;
    private final char[] chars;
    private final byte[] bytes;

    public MyTokenStream(int tokensPerDoc) {
      super();
      this.tokensPerDoc = tokensPerDoc;
      charTerm = addAttribute(CharTermAttribute.class);
      chars = charTerm.resizeBuffer(TOKEN_LEN);
      charTerm.setLength(TOKEN_LEN);
      bytes = new byte[2*TOKEN_LEN];
    }
    
    @Override
    public boolean incrementToken() {
      if (tokenCount >= tokensPerDoc) {
        return false;
      }
      random.nextBytes(bytes);
      int byteUpto = 0;
      for(int i=0;i<TOKEN_LEN;i++) {
        chars[i] = (char) (((bytes[byteUpto]&0xff) + (bytes[byteUpto+1]&0xff)) % UnicodeUtil.UNI_SUR_HIGH_START);
        byteUpto += 2;
      }
      tokenCount++;
      return true;
    }

    @Override
    public void reset() {
      tokenCount = 0;
    }
  }

  @Ignore("Takes ~4 hours to run on a fast machine!!")
  public void test2BTerms() throws IOException {

    long TERM_COUNT = ((long) Integer.MAX_VALUE) + 100000000;

    int TERMS_PER_DOC = 1000000;

    Directory dir = newFSDirectory(_TestUtil.getTempDir("2BTerms"));
    IndexWriter w = new IndexWriter(
        dir,
        new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH).
            setRAMBufferSizeMB(256.0).
            setMergeScheduler(new ConcurrentMergeScheduler()).
            setMergePolicy(newLogMergePolicy(false, 10))
    );

    MergePolicy mp = w.getConfig().getMergePolicy();
    if (mp instanceof LogByteSizeMergePolicy) {
      // 1 petabyte:
      ((LogByteSizeMergePolicy) mp).setMaxMergeMB(1024*1024*1024);
    }

    Document doc = new Document();
    Field field = new Field("field", new MyTokenStream(TERMS_PER_DOC));
    field.setOmitTermFreqAndPositions(true);
    field.setOmitNorms(true);
    doc.add(field);
    w.setInfoStream(System.out);
    final int numDocs = (int) (TERM_COUNT/TERMS_PER_DOC);
    for(int i=0;i<numDocs;i++) {
      final long t0 = System.currentTimeMillis();
      w.addDocument(doc);
      System.out.println(i + " of " + numDocs + " " + (System.currentTimeMillis()-t0) + " msec");
    }
    System.out.println("now optimize...");
    w.optimize();
    w.close();

    System.out.println("now CheckIndex...");
    CheckIndex.Status status = _TestUtil.checkIndex(dir);
    final long tc = status.segmentInfos.get(0).termIndexStatus.termCount;
    assertTrue("count " + tc + " is not > " + Integer.MAX_VALUE, tc > Integer.MAX_VALUE);
    dir.close();
  }
}

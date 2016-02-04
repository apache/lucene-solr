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


import java.io.InputStream;
import java.nio.file.Path;

import org.apache.lucene.document.Document;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

// LUCENE-6382
public class TestMaxPositionInOldIndex extends LuceneTestCase {


  // Save this to BuildMaxPositionIndex.java and follow the compile/run instructions to regenerate the .zip:
  /*
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

// Compile:
//   javac -cp lucene/build/core/lucene-core-5.1.0-SNAPSHOT.jar:lucene/build/test-framework/lucene-test-framework-5.1.0-SNAPSHOT.jar:lucene/build/analysis/common/lucene-analyzers-common-5.1.0-SNAPSHOT.jar BuildMaxPositionIndex.java

// Run:
//   java -cp .:lucene/build/core/lucene-core-5.1.0-SNAPSHOT.jar:lucene/build/test-framework/lucene-test-framework-5.1.0-SNAPSHOT.jar:lucene/build/analysis/common/lucene-analyzers-common-5.1.0-SNAPSHOT.jar:lucene/build/codecs/lucene-codecs-5.1.0-SNAPSHOT.jar BuildMaxPositionIndex

//  cd maxposindex
//  zip maxposindex.zip *

public class BuildMaxPositionIndex {
  public static void main(String[] args) throws IOException {
    Directory dir = FSDirectory.open(Paths.get("maxposindex"));
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(new WhitespaceAnalyzer()));
    Document doc = new Document();
    // This is at position 1:
    Token t1 = new Token("foo", 0, 3);
    t1.setPositionIncrement(2);
    Token t2 = new Token("foo", 4, 7);
    // This overflows max position:
    t2.setPositionIncrement(Integer.MAX_VALUE-1);
    t2.setPayload(new BytesRef(new byte[] { 0x1 } ));
    doc.add(new TextField("foo", new CannedTokenStream(new Token[] {t1, t2})));
    iw.addDocument(doc);
    iw.close();
    dir.close();
  }
}
  */

  public void testCorruptIndex() throws Exception {
    Path path = createTempDir("maxposindex");
    InputStream resource = getClass().getResourceAsStream("maxposindex.zip");
    assertNotNull("maxposindex not found", resource);
    TestUtil.unzip(resource, path);
    BaseDirectoryWrapper dir = newFSDirectory(path);
    dir.setCheckIndexOnClose(false);
    try {
      TestUtil.checkIndex(dir, false, true);
      fail("corruption was not detected");
    } catch (RuntimeException re) {
      // expected
      assertTrue(re.getMessage().contains("pos 2147483647 > IndexWriter.MAX_POSITION=2147483519"));
    }

    // Also confirm merging detects this:
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setMergeScheduler(new SerialMergeScheduler());
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter w = new IndexWriter(dir, iwc);
    w.addDocument(new Document());
    try {
      w.forceMerge(1);
    } catch (CorruptIndexException cie) {
      assertEquals(cie.getMessage(), new CorruptIndexException(cie.getOriginalMessage(), cie.getResourceDescription()).getMessage());
      // SerialMergeScheduler
      assertTrue("got message " + cie.getMessage(),
                 cie.getMessage().contains("position=2147483647 is too large (> IndexWriter.MAX_POSITION=2147483519), field=\"foo\" doc=0 (resource=PerFieldPostings(segment=_0 formats=1)"));
    }

    w.close();
    dir.close();
  }
}


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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.PayloadProcessorProvider.DirPayloadProcessor;
import org.apache.lucene.index.PayloadProcessorProvider.PayloadProcessor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestPayloadProcessorProvider extends LuceneTestCase {

  private static final class PerDirPayloadProcessor extends PayloadProcessorProvider {

    private Map<Directory, DirPayloadProcessor> processors;

    public PerDirPayloadProcessor(Map<Directory, DirPayloadProcessor> processors) {
      this.processors = processors;
    }

    @Override
    public DirPayloadProcessor getDirProcessor(Directory dir) throws IOException {
      return processors.get(dir);
    }

  }

  private static final class PerTermPayloadProcessor extends DirPayloadProcessor {

    @Override
    public PayloadProcessor getProcessor(Term term) throws IOException {
      // don't process payloads of terms other than "p:p1"
      if (!term.field().equals("p") || !term.text().equals("p1")) {
        return null;
      }
      
      // All other terms are processed the same way
      return new DeletePayloadProcessor();
    }
    
  }
  
  /** deletes the incoming payload */
  private static final class DeletePayloadProcessor extends PayloadProcessor {

    @Override
    public int payloadLength() throws IOException {
      return 0;
    }

    @Override
    public byte[] processPayload(byte[] payload, int start, int length) throws IOException {
      return payload;
    }

  }

  private static final class PayloadTokenStream extends TokenStream {

    private final PayloadAttribute payload = addAttribute(PayloadAttribute.class);
    private final CharTermAttribute term = addAttribute(CharTermAttribute.class);

    private boolean called = false;
    private String t;

    public PayloadTokenStream(String t) {
      this.t = t;
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (called) {
        return false;
      }

      called = true;
      byte[] p = new byte[] { 1 };
      payload.setPayload(new Payload(p));
      term.append(t);
      return true;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      called = false;
      term.setEmpty();
    }
  }

  private static final int NUM_DOCS = 10;

  private IndexWriterConfig getConfig(Random random) {
    return newIndexWriterConfig(random, TEST_VERSION_CURRENT, new WhitespaceAnalyzer(
        TEST_VERSION_CURRENT));
  }

  private void populateDirs(Random random, Directory[] dirs, boolean multipleCommits)
      throws IOException {
    for (int i = 0; i < dirs.length; i++) {
      dirs[i] = newDirectory();
      populateDocs(random, dirs[i], multipleCommits);
      verifyPayloadExists(dirs[i], new Term("p", "p1"), NUM_DOCS);
      verifyPayloadExists(dirs[i], new Term("p", "p2"), NUM_DOCS);
    }
  }

  private void populateDocs(Random random, Directory dir, boolean multipleCommits)
      throws IOException {
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)).
            setMergePolicy(newLogMergePolicy(10))
    );
    TokenStream payloadTS1 = new PayloadTokenStream("p1");
    TokenStream payloadTS2 = new PayloadTokenStream("p2");
    for (int i = 0; i < NUM_DOCS; i++) {
      Document doc = new Document();
      doc.add(newField("id", "doc" + i, Store.NO, Index.NOT_ANALYZED_NO_NORMS));
      doc.add(newField("content", "doc content " + i, Store.NO, Index.ANALYZED));
      doc.add(new Field("p", payloadTS1));
      doc.add(new Field("p", payloadTS2));
      writer.addDocument(doc);
      if (multipleCommits && (i % 4 == 0)) {
        writer.commit();
      }
    }
    writer.close();
  }

  private void verifyPayloadExists(Directory dir, Term term, int numExpected)
      throws IOException {
    IndexReader reader = IndexReader.open(dir);
    try {
      int numPayloads = 0;
      TermPositions tp = reader.termPositions(term);
      while (tp.next()) {
        tp.nextPosition();
        if (tp.isPayloadAvailable()) {
          assertEquals(1, tp.getPayloadLength());
          byte[] p = new byte[tp.getPayloadLength()];
          tp.getPayload(p, 0);
          assertEquals(1, p[0]);
          ++numPayloads;
        }
      }
      assertEquals(numExpected, numPayloads);
    } finally {
      reader.close();
    }
  }

  private void doTest(Random random, boolean addToEmptyIndex,
      int numExpectedPayloads, boolean multipleCommits) throws IOException {
    Directory[] dirs = new Directory[2];
    populateDirs(random, dirs, multipleCommits);

    Directory dir = newDirectory();
    if (!addToEmptyIndex) {
      populateDocs(random, dir, multipleCommits);
      verifyPayloadExists(dir, new Term("p", "p1"), NUM_DOCS);
      verifyPayloadExists(dir, new Term("p", "p2"), NUM_DOCS);
    }

    // Add two source dirs. By not adding the dest dir, we ensure its payloads
    // won't get processed.
    Map<Directory, DirPayloadProcessor> processors = new HashMap<Directory, DirPayloadProcessor>();
    for (Directory d : dirs) {
      processors.put(d, new PerTermPayloadProcessor());
    }
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)));
    writer.setPayloadProcessorProvider(new PerDirPayloadProcessor(processors));

    IndexReader[] readers = new IndexReader[dirs.length];
    for (int i = 0; i < readers.length; i++) {
      readers[i] = IndexReader.open(dirs[i]);
    }
    try {
      writer.addIndexes(readers);
    } finally {
      for (IndexReader r : readers) {
        r.close();
      }
    }
    writer.close();
    verifyPayloadExists(dir, new Term("p", "p1"), numExpectedPayloads);
    // the second term should always have all payloads
    numExpectedPayloads = NUM_DOCS * dirs.length
        + (addToEmptyIndex ? 0 : NUM_DOCS);
    verifyPayloadExists(dir, new Term("p", "p2"), numExpectedPayloads);
    for (Directory d : dirs)
      d.close();
    dir.close();
  }

  @Test
  public void testAddIndexes() throws Exception {
    // addIndexes - single commit in each
    doTest(random, true, 0, false);

    // addIndexes - multiple commits in each
    doTest(random, true, 0, true);
  }

  @Test
  public void testAddIndexesIntoExisting() throws Exception {
    // addIndexes - single commit in each
    doTest(random, false, NUM_DOCS, false);

    // addIndexes - multiple commits in each
    doTest(random, false, NUM_DOCS, true);
  }

  @Test
  public void testRegularMerges() throws Exception {
    Directory dir = newDirectory();
    populateDocs(random, dir, true);
    verifyPayloadExists(dir, new Term("p", "p1"), NUM_DOCS);
    verifyPayloadExists(dir, new Term("p", "p2"), NUM_DOCS);

    // Add two source dirs. By not adding the dest dir, we ensure its payloads
    // won't get processed.
    Map<Directory, DirPayloadProcessor> processors = new HashMap<Directory, DirPayloadProcessor>();
    processors.put(dir, new PerTermPayloadProcessor());
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)));
    writer.setPayloadProcessorProvider(new PerDirPayloadProcessor(processors));
    writer.forceMerge(1);
    writer.close();

    verifyPayloadExists(dir, new Term("p", "p1"), 0);
    verifyPayloadExists(dir, new Term("p", "p2"), NUM_DOCS);
    dir.close();
  }

}

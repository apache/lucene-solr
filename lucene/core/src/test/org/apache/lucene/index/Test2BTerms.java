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

import org.apache.lucene.util.*;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.store.*;
import org.apache.lucene.search.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.junit.Ignore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.*;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

// NOTE: this test will fail w/ PreFlexRW codec!  (Because
// this test uses full binary term space, but PreFlex cannot
// handle this since it requires the terms are UTF8 bytes).
//
// Also, SimpleText codec will consume very large amounts of
// disk (but, should run successfully).  Best to run w/
// -Dtests.codec=Standard, and w/ plenty of RAM, eg:
//
//   ant test -Dtest.slow=true -Dtests.heapsize=8g
//
//   java -server -Xmx8g -d64 -cp .:lib/junit-4.10.jar:./build/classes/test:./build/classes/test-framework:./build/classes/java -Dlucene.version=4.0-dev -Dtests.directory=MMapDirectory -DtempDir=build -ea org.junit.runner.JUnitCore org.apache.lucene.index.Test2BTerms
//
@SuppressCodecs({ "SimpleText", "Memory", "Direct" })
public class Test2BTerms extends LuceneTestCase {

  private final static int TOKEN_LEN = 10;

  private final static BytesRef bytes = new BytesRef(TOKEN_LEN);

  private final static class MyTokenStream extends TokenStream {

    private final int tokensPerDoc;
    private int tokenCount;
    public final List<BytesRef> savedTerms = new ArrayList<BytesRef>();
    private int nextSave;
    private final Random random;

    public MyTokenStream(Random random, int tokensPerDoc) {
      super(new MyAttributeFactory(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY));
      this.tokensPerDoc = tokensPerDoc;
      addAttribute(TermToBytesRefAttribute.class);
      bytes.length = TOKEN_LEN;
      this.random = random;
      nextSave = _TestUtil.nextInt(random, 500000, 1000000);
    }
    
    @Override
    public boolean incrementToken() {
      if (tokenCount >= tokensPerDoc) {
        return false;
      }
      random.nextBytes(bytes.bytes);
      tokenCount++;
      if (--nextSave == 0) {
        savedTerms.add(BytesRef.deepCopyOf(bytes));
        System.out.println("TEST: save term=" + bytes);
        nextSave = _TestUtil.nextInt(random, 500000, 1000000);
      }
      return true;
    }

    @Override
    public void reset() {
      tokenCount = 0;
    }

    private final static class MyTermAttributeImpl extends AttributeImpl implements TermToBytesRefAttribute {
      @Override
      public int fillBytesRef() {
        return bytes.hashCode();
      }
      
      @Override
      public BytesRef getBytesRef() {
        return bytes;
      }

      @Override
      public void clear() {
      }

      @Override
      public boolean equals(Object other) {
        return other == this;
      }

      @Override
      public int hashCode() {
        return System.identityHashCode(this);
      }
    
      @Override
      public void copyTo(AttributeImpl target) {
      }
    
      @Override
      public MyTermAttributeImpl clone() {
        throw new UnsupportedOperationException();
      }
    }

    private static final class MyAttributeFactory extends AttributeFactory {
      private final AttributeFactory delegate;

      public MyAttributeFactory(AttributeFactory delegate) {
        this.delegate = delegate;
      }
  
      @Override
      public AttributeImpl createAttributeInstance(Class<? extends Attribute> attClass) {
        if (attClass == TermToBytesRefAttribute.class)
          return new MyTermAttributeImpl();
        if (CharTermAttribute.class.isAssignableFrom(attClass))
          throw new IllegalArgumentException("no");
        return delegate.createAttributeInstance(attClass);
      }
    }
  }

  @Ignore("Very slow. Enable manually by removing @Ignore.")
  public void test2BTerms() throws IOException {

    if ("Lucene3x".equals(Codec.getDefault().getName())) {
      throw new RuntimeException("this test cannot run with PreFlex codec");
    }
    System.out.println("Starting Test2B");
    final long TERM_COUNT = ((long) Integer.MAX_VALUE) + 100000000;

    final int TERMS_PER_DOC = _TestUtil.nextInt(random(), 100000, 1000000);

    List<BytesRef> savedTerms = null;

    BaseDirectoryWrapper dir = newFSDirectory(_TestUtil.getTempDir("2BTerms"));
    //MockDirectoryWrapper dir = newFSDirectory(new File("/p/lucene/indices/2bindex"));
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    dir.setCheckIndexOnClose(false); // don't double-checkindex

    if (true) {

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
      final MyTokenStream ts = new MyTokenStream(random(), TERMS_PER_DOC);

      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setIndexOptions(IndexOptions.DOCS_ONLY);
      customType.setOmitNorms(true);
      Field field = new Field("field", ts, customType);
      doc.add(field);
      //w.setInfoStream(System.out);
      final int numDocs = (int) (TERM_COUNT/TERMS_PER_DOC);

      System.out.println("TERMS_PER_DOC=" + TERMS_PER_DOC);
      System.out.println("numDocs=" + numDocs);

      for(int i=0;i<numDocs;i++) {
        final long t0 = System.currentTimeMillis();
        w.addDocument(doc);
        System.out.println(i + " of " + numDocs + " " + (System.currentTimeMillis()-t0) + " msec");
      }
      savedTerms = ts.savedTerms;

      System.out.println("TEST: full merge");
      w.forceMerge(1);
      System.out.println("TEST: close writer");
      w.close();
    }

    System.out.println("TEST: open reader");
    final IndexReader r = DirectoryReader.open(dir);
    if (savedTerms == null) {
      savedTerms = findTerms(r);
    }
    final int numSavedTerms = savedTerms.size();
    final List<BytesRef> bigOrdTerms = new ArrayList<BytesRef>(savedTerms.subList(numSavedTerms-10, numSavedTerms));
    System.out.println("TEST: test big ord terms...");
    testSavedTerms(r, bigOrdTerms);
    System.out.println("TEST: test all saved terms...");
    testSavedTerms(r, savedTerms);
    r.close();

    System.out.println("TEST: now CheckIndex...");
    CheckIndex.Status status = _TestUtil.checkIndex(dir);
    final long tc = status.segmentInfos.get(0).termIndexStatus.termCount;
    assertTrue("count " + tc + " is not > " + Integer.MAX_VALUE, tc > Integer.MAX_VALUE);

    dir.close();
    System.out.println("TEST: done!");
  }

  private List<BytesRef> findTerms(IndexReader r) throws IOException {
    System.out.println("TEST: findTerms");
    final TermsEnum termsEnum = MultiFields.getTerms(r, "field").iterator(null);
    final List<BytesRef> savedTerms = new ArrayList<BytesRef>();
    int nextSave = _TestUtil.nextInt(random(), 500000, 1000000);
    BytesRef term;
    while((term = termsEnum.next()) != null) {
      if (--nextSave == 0) {
        savedTerms.add(BytesRef.deepCopyOf(term));
        System.out.println("TEST: add " + term);
        nextSave = _TestUtil.nextInt(random(), 500000, 1000000);
      }
    }
    return savedTerms;
  }

  private void testSavedTerms(IndexReader r, List<BytesRef> terms) throws IOException {
    System.out.println("TEST: run " + terms.size() + " terms on reader=" + r);
    IndexSearcher s = newSearcher(r);
    Collections.shuffle(terms);
    TermsEnum termsEnum = MultiFields.getTerms(r, "field").iterator(null);
    boolean failed = false;
    for(int iter=0;iter<10*terms.size();iter++) {
      final BytesRef term = terms.get(random().nextInt(terms.size()));
      System.out.println("TEST: search " + term);
      final long t0 = System.currentTimeMillis();
      final int count = s.search(new TermQuery(new Term("field", term)), 1).totalHits;
      if (count <= 0) {
        System.out.println("  FAILED: count=" + count);
        failed = true;
      }
      final long t1 = System.currentTimeMillis();
      System.out.println("  took " + (t1-t0) + " millis");

      TermsEnum.SeekStatus result = termsEnum.seekCeil(term);
      if (result != TermsEnum.SeekStatus.FOUND) {
        if (result == TermsEnum.SeekStatus.END) {
          System.out.println("  FAILED: got END");
        } else {
          System.out.println("  FAILED: wrong term: got " + termsEnum.term());
        }
        failed = true;
      }
    }
    assertFalse(failed);
  }
}

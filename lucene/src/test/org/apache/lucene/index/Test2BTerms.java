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
import org.apache.lucene.index.codecs.CodecProvider;
import java.io.IOException;
import org.junit.Ignore;

// NOTE: this test will fail w/ PreFlexRW codec!  (Because
// this test uses full binary term space, but PreFlex cannot
// handle this since it requires the terms are UTF8 bytes).
//
// Also, SimpleText codec will consume very large amounts of
// disk (but, should run successfully).  Best to run w/
// -Dtests.codec=Standard, and w/ plenty of RAM, eg:
//
//   ant compile-test
//
//   java -server -Xmx2g -Xms2g -d64 -cp .:lib/junit-4.7.jar:./build/classes/test:./build/classes/java -Dlucene.version=4.0-dev -Dtests.directory=SimpleFSDirectory -Dtests.codec=Standard -DtempDir=build -ea org.junit.runner.JUnitCore org.apache.lucene.index.Test2BTerms
//

public class Test2BTerms extends LuceneTestCase {

  private final static int TOKEN_LEN = 10;

  private final static BytesRef bytes = new BytesRef(TOKEN_LEN);

  private static final class MyTokenStream extends TokenStream {

    private final int tokensPerDoc;
    private int tokenCount;
    private int byteUpto;

    public MyTokenStream(int tokensPerDoc) {
      super(new MyAttributeFactory(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY));
      this.tokensPerDoc = tokensPerDoc;
      addAttribute(TermToBytesRefAttribute.class);
      bytes.length = TOKEN_LEN;
    }
    
    @Override
    public boolean incrementToken() {
      if (tokenCount >= tokensPerDoc) {
        return false;
      }
      random.nextBytes(bytes.bytes);
      tokenCount++;
      return true;
    }

    @Override
    public void reset() {
      tokenCount = 0;
    }

    private final static class MyTermAttributeImpl extends AttributeImpl implements TermToBytesRefAttribute {
      public int toBytesRef(BytesRef bs) {
        bs.bytes = bytes.bytes;
        bs.offset = bytes.offset;
        bs.length = bytes.length;
        return bytes.hashCode();
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
      public Object clone() {
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

  @Ignore("Takes ~4 hours to run on a fast machine!!  And requires that you don't use PreFlex codec.")
  public void test2BTerms() throws IOException {

    if ("PreFlex".equals(CodecProvider.getDefault().getDefaultFieldCodec())) {
      throw new RuntimeException("thist test cannot run with PreFlex codec");
    }

    long TERM_COUNT = ((long) Integer.MAX_VALUE) + 100000000;

    int TERMS_PER_DOC = 1000000;

    Directory dir = newFSDirectory(_TestUtil.getTempDir("2BTerms"));
    IndexWriter w = new IndexWriter(
        dir,
        new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer()).
            setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH).
            setRAMBufferSizeMB(256.0).
            setMergeScheduler(new ConcurrentMergeScheduler()).
            setMergePolicy(newLogMergePolicy(false, 10))
    );

    Document doc = new Document();
    Field field = new Field("field", new MyTokenStream(TERMS_PER_DOC));
    field.setOmitTermFreqAndPositions(true);
    field.setOmitNorms(true);
    doc.add(field);
    //w.setInfoStream(System.out);
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

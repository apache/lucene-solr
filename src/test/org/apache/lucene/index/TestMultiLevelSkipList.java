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
import java.io.Reader;

import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RAMDirectory;

/**
 * This testcase tests whether multi-level skipping is being used
 * to reduce I/O while skipping through posting lists.
 * 
 * Skipping in general is already covered by several other
 * testcases.
 * 
 */
public class TestMultiLevelSkipList extends LuceneTestCase {
  public void testSimpleSkip() throws IOException {
    RAMDirectory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new PayloadAnalyzer(), true,
                                         IndexWriter.MaxFieldLength.LIMITED);
    Term term = new Term("test", "a");
    for (int i = 0; i < 5000; i++) {
      Document d1 = new Document();
      d1.add(new Field(term.field(), term.text(), Store.NO, Index.ANALYZED));
      writer.addDocument(d1);
    }
    writer.flush();
    writer.optimize();
    writer.close();

    IndexReader reader = IndexReader.open(dir);
    SegmentTermPositions tp = (SegmentTermPositions) reader.termPositions();
    tp.freqStream = new CountingStream(tp.freqStream);

    for (int i = 0; i < 2; i++) {
      counter = 0;
      tp.seek(term);

      checkSkipTo(tp, 14, 185); // no skips
      checkSkipTo(tp, 17, 190); // one skip on level 0
      checkSkipTo(tp, 287, 200); // one skip on level 1, two on level 0
    
      // this test would fail if we had only one skip level,
      // because than more bytes would be read from the freqStream
      checkSkipTo(tp, 4800, 250);// one skip on level 2
    }
  }

  public void checkSkipTo(TermPositions tp, int target, int maxCounter) throws IOException {
    tp.skipTo(target);
    if (maxCounter < counter) {
      fail("Too many bytes read: " + counter);
    }

    assertEquals("Wrong document " + tp.doc() + " after skipTo target " + target, target, tp.doc());
    assertEquals("Frequency is not 1: " + tp.freq(), 1,tp.freq());
    tp.nextPosition();
    byte[] b = new byte[1];
    tp.getPayload(b, 0);
    assertEquals("Wrong payload for the target " + target + ": " + b[0], (byte) target, b[0]);
  }

  private static class PayloadAnalyzer extends Analyzer {
    public TokenStream tokenStream(String fieldName, Reader reader) {
      return new PayloadFilter(new LowerCaseTokenizer(reader));
    }

  }

  private static class PayloadFilter extends TokenFilter {
    static int count = 0;
    
    protected PayloadFilter(TokenStream input) {
      super(input);
    }

    public Token next(final Token reusableToken) throws IOException {
      assert reusableToken != null;
      Token nextToken = input.next(reusableToken);
      if (nextToken != null) {
        nextToken.setPayload(new Payload(new byte[] { (byte) count++ }));
      }
      return nextToken;
    }

  }

  private int counter = 0;

  // Simply extends IndexInput in a way that we are able to count the number
  // of bytes read
  class CountingStream extends IndexInput {
    private IndexInput input;

    CountingStream(IndexInput input) {
      this.input = input;
    }

    public byte readByte() throws IOException {
      TestMultiLevelSkipList.this.counter++;
      return this.input.readByte();
    }

    public void readBytes(byte[] b, int offset, int len) throws IOException {
      TestMultiLevelSkipList.this.counter += len;
      this.input.readBytes(b, offset, len);
    }

    public void close() throws IOException {
      this.input.close();
    }

    public long getFilePointer() {
      return this.input.getFilePointer();
    }

    public void seek(long pos) throws IOException {
      this.input.seek(pos);
    }

    public long length() {
      return this.input.length();
    }

    public Object clone() {
      return new CountingStream((IndexInput) this.input.clone());
    }

  }
}

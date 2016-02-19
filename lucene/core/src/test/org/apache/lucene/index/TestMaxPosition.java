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


import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

// LUCENE-6382
public class TestMaxPosition extends LuceneTestCase {

  public void testTooBigPosition() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    // This is at position 1:
    Token t1 = new Token("foo", 0, 3);
    t1.setPositionIncrement(2);
    if (random().nextBoolean()) {
      t1.setPayload(new BytesRef(new byte[] { 0x1 } ));
    }
    Token t2 = new Token("foo", 4, 7);
    // This should overflow max:
    t2.setPositionIncrement(IndexWriter.MAX_POSITION);
    if (random().nextBoolean()) {
      t2.setPayload(new BytesRef(new byte[] { 0x1 } ));
    }
    doc.add(new TextField("foo", new CannedTokenStream(new Token[] {t1, t2})));
    expectThrows(IllegalArgumentException.class, () -> {
      iw.addDocument(doc);
    });

    // Document should not be visible:
    IndexReader r = DirectoryReader.open(iw);
    assertEquals(0, r.numDocs());
    r.close();

    iw.close();
    dir.close();
  }
  
  public void testMaxPosition() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    // This is at position 0:
    Token t1 = new Token("foo", 0, 3);
    if (random().nextBoolean()) {
      t1.setPayload(new BytesRef(new byte[] { 0x1 } ));
    }
    Token t2 = new Token("foo", 4, 7);
    t2.setPositionIncrement(IndexWriter.MAX_POSITION);
    if (random().nextBoolean()) {
      t2.setPayload(new BytesRef(new byte[] { 0x1 } ));
    }
    doc.add(new TextField("foo", new CannedTokenStream(new Token[] {t1, t2})));
    iw.addDocument(doc);

    // Document should be visible:
    IndexReader r = DirectoryReader.open(iw);
    assertEquals(1, r.numDocs());
    PostingsEnum postings = MultiFields.getTermPositionsEnum(r, "foo", new BytesRef("foo"));

    // "foo" appears in docID=0
    assertEquals(0, postings.nextDoc());

    // "foo" appears 2 times in the doc
    assertEquals(2, postings.freq());

    // first at pos=0
    assertEquals(0, postings.nextPosition());

    // next at pos=MAX
    assertEquals(IndexWriter.MAX_POSITION, postings.nextPosition());

    r.close();

    iw.close();
    dir.close();
  }
}

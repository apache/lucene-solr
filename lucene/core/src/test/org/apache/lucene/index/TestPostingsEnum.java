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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

/** 
 * Test basic postingsenum behavior, flags, reuse, etc.
 */
@SuppressCodecs("Direct") // Direct does not support reuse, but we test that it works...
public class TestPostingsEnum extends LuceneTestCase {
  
  public void testDocsOnly() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw, false);
    
    // sugar method (FREQS)
    PostingsEnum postings = getOnlySegmentReader(reader).postings(new Term("foo", "bar"));
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    TermsEnum termsEnum = getOnlySegmentReader(reader).terms("foo").iterator(null);
    termsEnum.seekExact(new BytesRef("bar"));
    PostingsEnum postings2 = termsEnum.postings(null, postings);
    assertNotNull(postings2);
    assertSame(postings, postings2);
    // and it had better work
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // asking for docs only: ok
    PostingsEnum docsOnly = termsEnum.postings(null, null, PostingsEnum.NONE);
    assertEquals(-1, docsOnly.docID());
    assertEquals(0, docsOnly.nextDoc());
    assertEquals(1, docsOnly.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly.nextDoc());
    // reuse that too
    PostingsEnum docsOnly2 = termsEnum.postings(null, docsOnly, PostingsEnum.NONE);
    assertNotNull(docsOnly2);
    assertSame(docsOnly, docsOnly2);
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    assertEquals(1, docsOnly2.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // we did not index positions
    PostingsEnum docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.POSITIONS);
    assertNotNull(docsAndPositionsEnum);
    
    // we did not index positions
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.PAYLOADS);
    assertNotNull(docsAndPositionsEnum);
    
    // we did not index positions
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.OFFSETS);
    assertNotNull(docsAndPositionsEnum);
    
    // we did not index positions
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.ALL);
    assertNotNull(docsAndPositionsEnum);
    
    iw.close();
    reader.close();
    dir.close();
  }
  
  public void testFreqs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer());
      }
    });
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    doc.add(new Field("foo", "bar bar", ft));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw, false);
    
    // sugar method (FREQS)
    PostingsEnum postings = getOnlySegmentReader(reader).postings(new Term("foo", "bar"));
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    TermsEnum termsEnum = getOnlySegmentReader(reader).terms("foo").iterator(null);
    termsEnum.seekExact(new BytesRef("bar"));
    PostingsEnum postings2 = termsEnum.postings(null, postings);
    assertNotNull(postings2);
    assertSame(postings, postings2);
    // and it had better work
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // asking for docs only: ok
    PostingsEnum docsOnly = termsEnum.postings(null, null, PostingsEnum.NONE);
    assertEquals(-1, docsOnly.docID());
    assertEquals(0, docsOnly.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly.nextDoc());
    // reuse that too
    PostingsEnum docsOnly2 = termsEnum.postings(null, docsOnly, PostingsEnum.NONE);
    assertNotNull(docsOnly2);
    assertSame(docsOnly, docsOnly2);
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // we did not index positions
    PostingsEnum docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.POSITIONS);
    assertNotNull(docsAndPositionsEnum);
    
    // we did not index positions
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.PAYLOADS);
    assertNotNull(docsAndPositionsEnum);
    
    // we did not index positions
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.OFFSETS);
    assertNotNull(docsAndPositionsEnum);
    
    // we did not index positions
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.ALL);
    assertNotNull(docsAndPositionsEnum);
    
    iw.close();
    reader.close();
    dir.close();
  }
  
  public void testPositions() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer());
      }
    });
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("foo", "bar bar", Field.Store.NO));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw, false);
    
    // sugar method (FREQS)
    PostingsEnum postings = getOnlySegmentReader(reader).postings(new Term("foo", "bar"));
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    TermsEnum termsEnum = getOnlySegmentReader(reader).terms("foo").iterator(null);
    termsEnum.seekExact(new BytesRef("bar"));
    PostingsEnum postings2 = termsEnum.postings(null, postings);
    assertNotNull(postings2);
    assertSame(postings, postings2);
    // and it had better work
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // asking for docs only: ok
    PostingsEnum docsOnly = termsEnum.postings(null, null, PostingsEnum.NONE);
    assertEquals(-1, docsOnly.docID());
    assertEquals(0, docsOnly.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly.nextDoc());
    // reuse that too
    PostingsEnum docsOnly2 = termsEnum.postings(null, docsOnly, PostingsEnum.NONE);
    assertNotNull(docsOnly2);
    assertSame(docsOnly, docsOnly2);
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for positions, ok
    PostingsEnum docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.POSITIONS);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    // now reuse the positions
    PostingsEnum docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.POSITIONS);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    // payloads, offsets, etc don't cause an error if they aren't there
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.PAYLOADS);
    assertNotNull(docsAndPositionsEnum);
    // but make sure they work
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.PAYLOADS);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.OFFSETS);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.OFFSETS);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.ALL);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.ALL);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    iw.close();
    reader.close();
    dir.close();
  }
  
  public void testOffsets() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer());
      }
    });
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    doc.add(new Field("foo", "bar bar", ft));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw, false);
    
    // sugar method (FREQS)
    PostingsEnum postings = getOnlySegmentReader(reader).postings(new Term("foo", "bar"));
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    TermsEnum termsEnum = getOnlySegmentReader(reader).terms("foo").iterator(null);
    termsEnum.seekExact(new BytesRef("bar"));
    PostingsEnum postings2 = termsEnum.postings(null, postings);
    assertNotNull(postings2);
    assertSame(postings, postings2);
    // and it had better work
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // asking for docs only: ok
    PostingsEnum docsOnly = termsEnum.postings(null, null, PostingsEnum.NONE);
    assertEquals(-1, docsOnly.docID());
    assertEquals(0, docsOnly.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly.nextDoc());
    // reuse that too
    PostingsEnum docsOnly2 = termsEnum.postings(null, docsOnly, PostingsEnum.NONE);
    assertNotNull(docsOnly2);
    assertSame(docsOnly, docsOnly2);
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for positions, ok
    PostingsEnum docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.POSITIONS);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    // now reuse the positions
    PostingsEnum docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.POSITIONS);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    // payloads don't cause an error if they aren't there
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.PAYLOADS);
    assertNotNull(docsAndPositionsEnum);
    // but make sure they work
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.PAYLOADS);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.OFFSETS);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.OFFSETS);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.ALL);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.ALL);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    iw.close();
    reader.close();
    dir.close();
  }
  
  public void testPayloads() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    Token token1 = new Token("bar", 0, 3);
    token1.setPayload(new BytesRef("pay1"));
    Token token2 = new Token("bar", 4, 7);
    token2.setPayload(new BytesRef("pay2"));
    doc.add(new TextField("foo", new CannedTokenStream(token1, token2)));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw, false);
    
    // sugar method (FREQS)
    PostingsEnum postings = getOnlySegmentReader(reader).postings(new Term("foo", "bar"));
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    TermsEnum termsEnum = getOnlySegmentReader(reader).terms("foo").iterator(null);
    termsEnum.seekExact(new BytesRef("bar"));
    PostingsEnum postings2 = termsEnum.postings(null, postings);
    assertNotNull(postings2);
    assertSame(postings, postings2);
    // and it had better work
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // asking for docs only: ok
    PostingsEnum docsOnly = termsEnum.postings(null, null, PostingsEnum.NONE);
    assertEquals(-1, docsOnly.docID());
    assertEquals(0, docsOnly.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly.nextDoc());
    // reuse that too
    PostingsEnum docsOnly2 = termsEnum.postings(null, docsOnly, PostingsEnum.NONE);
    assertNotNull(docsOnly2);
    assertSame(docsOnly, docsOnly2);
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for positions, ok
    PostingsEnum docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.POSITIONS);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    // now reuse the positions
    PostingsEnum docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.POSITIONS);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    // payloads
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.PAYLOADS);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.PAYLOADS);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.OFFSETS);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.OFFSETS);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.ALL);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.ALL);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    iw.close();
    reader.close();
    dir.close();
  }
  
  public void testAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    Token token1 = new Token("bar", 0, 3);
    token1.setPayload(new BytesRef("pay1"));
    Token token2 = new Token("bar", 4, 7);
    token2.setPayload(new BytesRef("pay2"));
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    doc.add(new Field("foo", new CannedTokenStream(token1, token2), ft));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw, false);
    
    // sugar method (FREQS)
    PostingsEnum postings = getOnlySegmentReader(reader).postings(new Term("foo", "bar"));
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    TermsEnum termsEnum = getOnlySegmentReader(reader).terms("foo").iterator(null);
    termsEnum.seekExact(new BytesRef("bar"));
    PostingsEnum postings2 = termsEnum.postings(null, postings);
    assertNotNull(postings2);
    assertSame(postings, postings2);
    // and it had better work
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // asking for docs only: ok
    PostingsEnum docsOnly = termsEnum.postings(null, null, PostingsEnum.NONE);
    assertEquals(-1, docsOnly.docID());
    assertEquals(0, docsOnly.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly.nextDoc());
    // reuse that too
    PostingsEnum docsOnly2 = termsEnum.postings(null, docsOnly, PostingsEnum.NONE);
    assertNotNull(docsOnly2);
    assertSame(docsOnly, docsOnly2);
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for positions, ok
    PostingsEnum docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.POSITIONS);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    // now reuse the positions
    PostingsEnum docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.POSITIONS);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    // payloads
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.PAYLOADS);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.PAYLOADS);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.OFFSETS);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.OFFSETS);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.ALL);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    docsAndPositionsEnum2 = termsEnum.postings(null, docsAndPositionsEnum, PostingsEnum.ALL);
    assertSame(docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    iw.close();
    reader.close();
    dir.close();
  }
}

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/** 
 * Test basic postingsenum behavior, flags, reuse, etc.
 */
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
    assertNull(docsAndPositionsEnum);
    
    // we did not index positions
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.PAYLOADS);
    assertNull(docsAndPositionsEnum);
    
    // we did not index positions
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.OFFSETS);
    assertNull(docsAndPositionsEnum);
    
    // we did not index positions
    docsAndPositionsEnum = getOnlySegmentReader(reader).postings(new Term("foo", "bar"), PostingsEnum.ALL);
    assertNull(docsAndPositionsEnum);
    
    iw.close();
    reader.close();
    dir.close();
  }
}

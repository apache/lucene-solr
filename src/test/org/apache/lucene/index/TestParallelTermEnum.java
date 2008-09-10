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

import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.store.RAMDirectory;

public class TestParallelTermEnum extends LuceneTestCase {
    private IndexReader ir1;
    private IndexReader ir2;

    protected void setUp() throws Exception {
        super.setUp();
        super.setUp();
        Document doc;

        RAMDirectory rd1 = new RAMDirectory();
        IndexWriter iw1 = new IndexWriter(rd1, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

        doc = new Document();
        doc.add(new Field("field1", "the quick brown fox jumps", Store.YES,
            Index.ANALYZED));
        doc.add(new Field("field2", "the quick brown fox jumps", Store.YES,
            Index.ANALYZED));
        doc.add(new Field("field4", "", Store.NO, Index.ANALYZED));
        iw1.addDocument(doc);

        iw1.close();
        RAMDirectory rd2 = new RAMDirectory();
        IndexWriter iw2 = new IndexWriter(rd2, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

        doc = new Document();
        doc.add(new Field("field0", "", Store.NO, Index.ANALYZED));
        doc.add(new Field("field1", "the fox jumps over the lazy dog",
            Store.YES, Index.ANALYZED));
        doc.add(new Field("field3", "the fox jumps over the lazy dog",
            Store.YES, Index.ANALYZED));
        iw2.addDocument(doc);

        iw2.close();

        this.ir1 = IndexReader.open(rd1);
        this.ir2 = IndexReader.open(rd2);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        super.tearDown();

        ir1.close();
        ir2.close();
    }

    public void test1() throws IOException {
        ParallelReader pr = new ParallelReader();
        pr.add(ir1);
        pr.add(ir2);

        TermDocs td = pr.termDocs();

        TermEnum te = pr.terms();
        assertTrue(te.next());
        assertEquals("field1:brown", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field1:fox", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field1:jumps", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field1:quick", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field1:the", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field2:brown", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field2:fox", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field2:jumps", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field2:quick", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field2:the", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field3:dog", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field3:fox", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field3:jumps", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field3:lazy", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field3:over", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertTrue(te.next());
        assertEquals("field3:the", te.term().toString());
        td.seek(te.term());
        assertTrue(td.next());
        assertEquals(0, td.doc());
        assertFalse(td.next());
        assertFalse(te.next());
    }
}

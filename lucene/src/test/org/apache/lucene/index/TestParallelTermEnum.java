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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestParallelTermEnum extends LuceneTestCase {
    private IndexReader ir1;
    private IndexReader ir2;
    private Directory rd1;
    private Directory rd2;
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
        Document doc;
        rd1 = newDirectory();
        IndexWriter iw1 = new IndexWriter(rd1, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));

        doc = new Document();
        doc.add(newField("field1", "the quick brown fox jumps", TextField.TYPE_STORED));
        doc.add(newField("field2", "the quick brown fox jumps", TextField.TYPE_STORED));
        doc.add(newField("field4", "", TextField.TYPE_UNSTORED));
        iw1.addDocument(doc);

        iw1.close();
        rd2 = newDirectory();
        IndexWriter iw2 = new IndexWriter(rd2, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));

        doc = new Document();
        doc.add(newField("field0", "", TextField.TYPE_UNSTORED));
        doc.add(newField("field1", "the fox jumps over the lazy dog", TextField.TYPE_STORED));
        doc.add(newField("field3", "the fox jumps over the lazy dog", TextField.TYPE_STORED));
        iw2.addDocument(doc);

        iw2.close();

        this.ir1 = IndexReader.open(rd1, true);
        this.ir2 = IndexReader.open(rd2, true);
    }

    @Override
    public void tearDown() throws Exception {
        ir1.close();
        ir2.close();
        rd1.close();
        rd2.close();
        super.tearDown();
    }

    public void test1() throws IOException {
        ParallelReader pr = new ParallelReader();
        pr.add(ir1);
        pr.add(ir2);

        Bits liveDocs = pr.getLiveDocs();

        FieldsEnum fe = pr.fields().iterator();

        String f = fe.next();
        assertEquals("field0", f);
        f = fe.next();
        assertEquals("field1", f);

        Terms terms = fe.terms();
        TermsEnum te = terms.iterator(null);

        assertEquals("brown", te.next().utf8ToString());
        DocsEnum td = _TestUtil.docs(random, te, liveDocs, null, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertEquals("fox", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertEquals("jumps", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertEquals("quick", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertEquals("the", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertNull(te.next());
        f = fe.next();
        assertEquals("field2", f);
        terms = fe.terms();
        assertNotNull(terms);
        te = terms.iterator(null);

        assertEquals("brown", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertEquals("fox", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertEquals("jumps", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertEquals("quick", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertEquals("the", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertNull(te.next());
        f = fe.next();
        assertEquals("field3", f);
        terms = fe.terms();
        assertNotNull(terms);
        te = terms.iterator(null);

        assertEquals("dog", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertEquals("fox", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertEquals("jumps", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertEquals("lazy", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertEquals("over", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertEquals("the", te.next().utf8ToString());
        td = _TestUtil.docs(random, te, liveDocs, td, false);
        assertTrue(td.nextDoc() != DocsEnum.NO_MORE_DOCS);
        assertEquals(0, td.docID());
        assertEquals(td.nextDoc(), DocsEnum.NO_MORE_DOCS);

        assertNull(te.next());
    }
}

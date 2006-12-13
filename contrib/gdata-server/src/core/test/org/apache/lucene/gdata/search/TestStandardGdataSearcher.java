/** 
 * Copyright 2004 The Apache Software Foundation 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 */
package org.apache.lucene.gdata.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.gdata.search.StandardGdataSearcher;
import org.apache.lucene.gdata.search.index.IndexDocument;
import org.apache.lucene.gdata.utils.ReferenceCounter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/**
 * @author Simon Willnauer
 *
 */
public class TestStandardGdataSearcher extends TestCase {
    private Directory dir;

    private int amountDocuments = 30;

    private static final String FIELDNAME = "fname";

    private static final String FIELDVALUE = "foobar";

    private ReferenceCounter<IndexSearcher> searcher;

    private StandardGdataSearcher gdataSearcher;

    private List<String> idlist;
    
    private String feedId = "myFeed";

    protected void setUp() throws Exception {
        this.dir = new RAMDirectory();
        IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), true);
        this.idlist = new ArrayList<String>();
        for (int i = 0; i < this.amountDocuments; i++) {
            Document doc = new Document();
            doc.add(new Field(IndexDocument.FIELD_FEED_ID, this.feedId,
                    Field.Store.YES, Field.Index.UN_TOKENIZED));
            doc.add(new Field(IndexDocument.FIELD_ENTRY_ID, "" + i,
                    Field.Store.YES, Field.Index.UN_TOKENIZED));
            doc.add(new Field(FIELDNAME, FIELDVALUE, Field.Store.YES,
                    Field.Index.UN_TOKENIZED));
            writer.addDocument(doc);
            this.idlist.add("" + i);
        }
        writer.close();
        this.searcher = new TestRefcounter(new IndexSearcher(this.dir));
        this.searcher.increamentReference();
        this.gdataSearcher = new StandardGdataSearcher(this.searcher);
    }

    protected void tearDown() throws Exception {
        this.searcher.decrementRef();
        StandardGdataSearcher.flushFilterCache();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.StandardGdataSearcher.StandardGdataSearcher(ReferenceCounter<IndexSearcher>)'
     */
    public void testStandardGdataSearcher() {
        try {
            new StandardGdataSearcher(null);
            fail("searcher ref is null");
        } catch (IllegalArgumentException e) {

        }
        new StandardGdataSearcher(this.searcher);
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.StandardGdataSearcher.search(Query, int,
     * int)'
     */
    public void testSearch() throws IOException {
        Query q = new TermQuery(new Term(FIELDNAME, FIELDVALUE));
        Hits hits = this.searcher.get().search(q);
        assertEquals(amountDocuments, hits.length());
        List<String> returnValue = this.gdataSearcher.search(q,
                this.amountDocuments, 0,this.feedId);
        assertEquals(amountDocuments, returnValue.size());
        assertTrue(returnValue.containsAll(this.idlist));
        try {
            this.gdataSearcher.search(null, 1, 0,this.feedId);
            fail("searcher is null");
        } catch (RuntimeException e) {
        }

        try {
            this.gdataSearcher.search(q, -1, 5,this.feedId);
            fail("hitcount is less than 0");
        } catch (IllegalArgumentException e) {}
        try {
            this.gdataSearcher.search(q, 4, -1,this.feedId);
            fail("offset is less than 0");
        } catch (IllegalArgumentException e) {}
        try {
            this.gdataSearcher.search(q, 4, 5,null);
            fail("feed id is null");
        } catch (IllegalArgumentException e) {}
      
        returnValue = this.gdataSearcher.search(q,this.amountDocuments, 0,"SomeOtherFeed");
        assertEquals(0,returnValue.size());
        

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.StandardGdataSearcher.collectHits(Hits,
     * int, int)'
     */
    public void testCollectHits() throws IOException {
        Query q = new TermQuery(new Term(FIELDNAME, FIELDVALUE));
        Hits hits = this.searcher.get().search(q);
        assertEquals(amountDocuments, hits.length());
        List<String> returnValue = this.gdataSearcher.collectHits(hits, 1, 0);
        assertEquals(hits.doc(0).getField(IndexDocument.FIELD_ENTRY_ID)
                .stringValue(), returnValue.get(0));

        returnValue = this.gdataSearcher.collectHits(hits, 1, 1);
        assertEquals(hits.doc(0).getField(IndexDocument.FIELD_ENTRY_ID)
                .stringValue(), returnValue.get(0));

        returnValue = this.gdataSearcher.collectHits(hits, 1,
                this.amountDocuments);
        assertEquals(1, returnValue.size());
        assertEquals(hits.doc(this.amountDocuments - 1).getField(
                IndexDocument.FIELD_ENTRY_ID).stringValue(), returnValue.get(0));

        returnValue = this.gdataSearcher.collectHits(hits, 10,
                this.amountDocuments);
        assertEquals(1, returnValue.size());
        assertEquals(hits.doc(this.amountDocuments - 1).getField(
                IndexDocument.FIELD_ENTRY_ID).stringValue(), returnValue.get(0));

        returnValue = this.gdataSearcher.collectHits(hits, 50, 0);
        assertEquals(this.amountDocuments, returnValue.size());
        assertTrue(returnValue.containsAll(this.idlist));

        returnValue = this.gdataSearcher.collectHits(hits, 1, 5);
        assertEquals(1, returnValue.size());
        assertEquals(hits.doc(4).getField(IndexDocument.FIELD_ENTRY_ID)
                .stringValue(), returnValue.get(0));

        returnValue = this.gdataSearcher.collectHits(hits, 50,
                this.amountDocuments);
        assertEquals(1, returnValue.size());
        assertEquals(hits.doc(this.amountDocuments - 1).getField(
                IndexDocument.FIELD_ENTRY_ID).stringValue(), returnValue.get(0));

        returnValue = this.gdataSearcher.collectHits(hits, 1,
                this.amountDocuments + 1);
        assertEquals(0, returnValue.size());

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.StandardGdataSearcher.close()'
     */
    public void testClose() throws IOException {
        StandardGdataSearcher s = new StandardGdataSearcher(new TestRefcounter(
                new IndexSearcher(this.dir)));
        s.close();
        try {
            s.search(null, 0, 0,this.feedId);
            fail("searcher is closed");
        } catch (IllegalStateException e) {
        }

    }

    private static class TestRefcounter extends ReferenceCounter<IndexSearcher> {

        public TestRefcounter(IndexSearcher resource) {
            super(resource);
            // TODO Auto-generated constructor stub
        }

        @Override
        protected void close() {
            try {
                this.resource.close();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }

    }

}

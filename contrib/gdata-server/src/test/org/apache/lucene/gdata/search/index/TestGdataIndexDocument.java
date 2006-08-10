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
package org.apache.lucene.gdata.search.index;

import junit.framework.TestCase;

import org.apache.lucene.document.Document;
import org.apache.lucene.gdata.search.analysis.ContentStrategy;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.lucene.gdata.search.config.IndexSchemaField.ContentType;
import org.apache.lucene.index.Term;

public class TestGdataIndexDocument extends TestCase {
    static String ENTRYID = "someEId";
    static String FEEDID = "someFId";
    GDataIndexDocument delDocument;
    GDataIndexDocument updDocument;
    GDataIndexDocument insDocument;
    protected void setUp() throws Exception {
        this.delDocument = new GDataIndexDocument(IndexAction.DELETE,ENTRYID,FEEDID,false,true);
        this.insDocument = new GDataIndexDocument(IndexAction.INSERT,ENTRYID,FEEDID,true,false);
        this.updDocument = new GDataIndexDocument(IndexAction.UPDATE,ENTRYID,FEEDID,false,true);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.index.GDataIndexDocument.addField(ContentStrategy)'
     */
    public void testAddField() {
        assertEquals(0,this.delDocument.fields.size());    
        this.delDocument.addField(null);
        assertEquals(0,this.delDocument.fields.size());
        IndexSchemaField ifield = new IndexSchemaField();
        ifield.setContentType(ContentType.TEXT);
        this.delDocument.addField(ContentStrategy.getFieldStrategy(ifield));
        assertEquals(1,this.delDocument.fields.size());
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.index.GDataIndexDocument.getWriteable()'
     */
    public void testGetWriteable() {
        assertNotNull(this.insDocument.getWriteable());
        Document doc = this.insDocument.getWriteable();
        assertEquals(2,doc.getFields().size());
        assertEquals(ENTRYID,doc.getField(GDataIndexDocument.FIELD_ENTRY_ID).stringValue());
        assertEquals(FEEDID,doc.getField(GDataIndexDocument.FIELD_FEED_ID).stringValue());
        
        
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.index.GDataIndexDocument.getDeletealbe()'
     */
    public void testGetDeletealbe() {
        assertNotNull(this.insDocument.getDeletealbe());
        Term t = this.insDocument.getDeletealbe();
        assertEquals(IndexDocument.FIELD_ENTRY_ID,t.field());
        assertEquals(ENTRYID,t.text());
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.index.GDataIndexDocument.isUpdate()'
     */
    public void testIsUpdate() {
        assertFalse(this.insDocument.isUpdate());
        assertTrue(this.updDocument.isUpdate());
        assertFalse(this.delDocument.isUpdate());
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.index.GDataIndexDocument.isDelete()'
     */
    public void testIsDelete() {
        assertFalse(this.insDocument.isDelete());
        assertFalse(this.updDocument.isDelete());
        assertTrue(this.delDocument.isDelete());
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.index.GDataIndexDocument.isInsert()'
     */
    public void testIsInsert() {
        assertTrue(this.insDocument.isInsert());
        assertFalse(this.updDocument.isInsert());
        assertFalse(this.delDocument.isInsert());
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.index.GDataIndexDocument.commitAfter()'
     */
    public void testCommitAfter() {
        assertTrue(this.insDocument.commitAfter());
        assertFalse(this.updDocument.commitAfter());
        assertFalse(this.delDocument.commitAfter());
    }
    
    /*
     * Test method for 'org.apache.lucene.gdata.search.index.GDataIndexDocument.optimizeAfter()'
     */
    public void testOptimizeAfter() {
        assertFalse(this.insDocument.optimizeAfter());
        assertTrue(this.updDocument.optimizeAfter());
        assertTrue(this.delDocument.optimizeAfter());
    }

}

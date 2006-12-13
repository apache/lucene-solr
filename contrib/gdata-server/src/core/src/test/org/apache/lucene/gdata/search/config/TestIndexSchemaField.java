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
package org.apache.lucene.gdata.search.config;

import junit.framework.TestCase;

import org.apache.lucene.document.Field;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.search.analysis.ContentStrategy;
import org.apache.lucene.gdata.search.analysis.Indexable;
import org.apache.lucene.gdata.search.analysis.NotIndexableException;
import org.apache.lucene.gdata.search.analysis.PlainTextStrategy;
import org.apache.lucene.gdata.search.config.IndexSchemaField.ContentType;
import org.w3c.dom.Node;

/**
 * @author Simon Willnauer
 *
 */
public class TestIndexSchemaField extends TestCase {

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.config.IndexSchemaField.checkRequieredValues()'
     */
    public void testCheckRequieredValues() {
        IndexSchemaField f = new IndexSchemaField();
        assertFalse(f.checkRequieredValues());
        f.setName("someName");
        assertFalse(f.checkRequieredValues());
        f.setPath("somePath");
        assertFalse(f.checkRequieredValues());
        f.setType("text");
        assertTrue(f.checkRequieredValues());
        f.setType("mixed");
        assertFalse(f.checkRequieredValues());
        f.setTypePath("sometypepath");
        assertTrue(f.checkRequieredValues());
        
        f.setType("custom");
        assertFalse(f.checkRequieredValues());
        f.setFieldClass(TestContentStragtegy.class);
        assertTrue(f.checkRequieredValues());
    }
    public void testSetFieldType(){
        IndexSchemaField f = new IndexSchemaField();
        f.setFieldClass(TestContentStragtegy.class);
        try{
            f.setFieldClass(PlainTextStrategy.class);
            fail("no pub const.");
        }catch (RuntimeException e) {
            
        }
        try{
            f.setFieldClass(null);
            fail("is null");
        }catch (RuntimeException e) {
            
        }
        
    }
    /*
     * Test method for 'org.apache.lucene.gdata.search.config.IndexSchemaField.setAnalyzerClass(Class<? extends Analyzer>)'
     */
    public void testSetType() {
        IndexSchemaField f = new IndexSchemaField();
        f.setType("notatype");
        assertNull(f.getContentType());
        f.setType("custom");
        assertEquals(ContentType.CUSTOM,f.getContentType());
        f.setType("text");
        assertEquals(ContentType.TEXT,f.getContentType());
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.config.IndexSchemaField.setStoreByName(String)'
     */
    public void testSetStoreByName() {
        IndexSchemaField f = new IndexSchemaField();
        f.setStoreByName("someother");
        assertEquals(Field.Store.NO,f.getStore());
        f.setStoreByName("COMPRESS");
        assertEquals(Field.Store.COMPRESS,f.getStore());
        f.setStoreByName("YeS");
        assertEquals(Field.Store.YES,f.getStore());
        f.setStoreByName("No");
        assertEquals(Field.Store.NO,f.getStore());
        
        
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.config.IndexSchemaField.setIndexByName(String)'
     */
    public void testSetIndexByName() {
        IndexSchemaField f = new IndexSchemaField();
        f.setIndexByName("UN_done");
        assertEquals(Field.Index.TOKENIZED,f.getIndex());
        f.setIndexByName("UN_tokenized");
        assertEquals(Field.Index.UN_TOKENIZED,f.getIndex());
        f.setIndexByName("tokenized");
        assertEquals(Field.Index.TOKENIZED,f.getIndex());
        f.setIndexByName("no");
        assertEquals(Field.Index.NO,f.getIndex());
        f.setIndexByName("no_norms");
        assertEquals(Field.Index.NO_NORMS,f.getIndex());
        
        
    }
    
    public void testSetboost(){
        IndexSchemaField f = new IndexSchemaField();
        f.setBoost(-0.1f);
        assertEquals(1.0f,f.getBoost());
        f.setBoost(2.50f);
        assertEquals(2.50f,f.getBoost());
    }
    
    public void testToSTringNoNullPEx(){
        assertNotNull(new IndexSchemaField().toString());
    }
    static class TestContentStragtegy extends ContentStrategy{
        
        public TestContentStragtegy(IndexSchemaField fieldConfiguration) {
            super(fieldConfiguration);
            // TODO Auto-generated constructor stub
        }

        @Override
        public void processIndexable(Indexable<? extends Node, ? extends ServerBaseEntry> indexable) throws NotIndexableException {
        }
        
    }
}

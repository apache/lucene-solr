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
package org.apache.lucene.gdata.search.analysis;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.lucene.gdata.search.index.GdataIndexerException;
import org.w3c.dom.Node;

import junit.framework.TestCase;

public class TestContentStrategy extends TestCase {
    private static final String FIELD = "foo";
    private static final float BOOST = 2.0f;
    ContentStrategy strategy;
    
    protected void setUp() throws Exception {
        IndexSchemaField field = new IndexSchemaField();
        field.setName(FIELD);
        field.setStore(Field.Store.YES);
        field.setIndex(Field.Index.UN_TOKENIZED);
        field.setBoost(BOOST);
        this.strategy = new TestStrategy(field);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }
    public void testContentStrategyIndexStoreField() throws NotIndexableException{
        IndexSchemaField field = new IndexSchemaField();
        field.setName(FIELD);
        
        
        this.strategy = new TestStrategy(Field.Index.UN_TOKENIZED,Field.Store.YES,field);
        this.strategy.processIndexable(null);
        Field f = this.strategy.createLuceneField()[0];
        assertEquals(FIELD,f.name());
        assertEquals(TestStrategy.CONTENT,f.stringValue());
        assertEquals(1.0f,f.getBoost());
        assertTrue(f.isIndexed());
        assertTrue(f.isStored());
        assertFalse(f.isTokenized());
        assertFalse(f.isCompressed());
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.analysis.ContentStrategy.ContentStrategy(Index, Store, IndexSchemaField)'
     */
    public void testContentStrategyIndexSchemaField() throws NotIndexableException {
        IndexSchemaField field = new IndexSchemaField();
        field.setName(FIELD);
        
        
        this.strategy = new TestStrategy(field);
        this.strategy.processIndexable(null);
        Field f = this.strategy.createLuceneField()[0];
        
        assertEquals(FIELD,f.name());
        assertEquals(TestStrategy.CONTENT,f.stringValue());
        assertEquals(1.0f,f.getBoost());
        assertTrue(f.isIndexed());
        assertFalse(f.isStored());
        assertTrue(f.isTokenized());
        assertFalse(f.isCompressed());
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.analysis.ContentStrategy.createLuceneField()'
     */
    public void testCreateLuceneField() throws NotIndexableException {
        try{
        this.strategy.createLuceneField();
        fail("processIndexable is not called");
        }catch (GdataIndexerException e) {
          //
        }
        this.strategy.processIndexable(null);
        Field f = this.strategy.createLuceneField()[0];
        
        assertEquals(FIELD,f.name());
        assertEquals(TestStrategy.CONTENT,f.stringValue());
        assertEquals(BOOST,f.getBoost());
        assertTrue(f.isIndexed());
        assertTrue(f.isStored());
        assertFalse(f.isTokenized());
        assertFalse(f.isCompressed());
       
        
        
    }
    
    private static class TestStrategy extends ContentStrategy{

        private static final String CONTENT = "someString";

      
        protected TestStrategy(Index index, Store store, IndexSchemaField fieldConfig) {
            super(index, store, fieldConfig);
      
        }

        protected TestStrategy(IndexSchemaField fieldConfiguration) {
            super(fieldConfiguration);
            
        }

        @Override
        public void processIndexable(Indexable<? extends Node, ? extends ServerBaseEntry> indexable) throws NotIndexableException {
            this.content = CONTENT;
        }
        
    }

}

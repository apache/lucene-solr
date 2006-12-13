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

import junit.framework.TestCase;

import org.apache.lucene.document.Field;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.lucene.gdata.search.index.GdataIndexerException;

/**
 * @author Simon Willnauer
 * 
 */
public class TestKeywordStrategy extends TestCase {
    private static final String FIELD = "foo";

    private static final float BOOST = 2.0f;

    ContentStrategy strategy;

    private IndexSchemaField field;

    protected void setUp() throws Exception {
        this.field = new IndexSchemaField();
        field.setName(FIELD);
        field.setStore(Field.Store.NO);
        field.setIndex(Field.Index.TOKENIZED);
        field.setBoost(BOOST);
        field.setPath("/path");
        
        this.strategy = new KeywordStrategy(field);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.analysis.PlainTextStrategy.processIndexable(Indexable<?
     * extends Node, ? extends ServerBaseEntry>)'
     */
    public void testProcessIndexable() throws NotIndexableException {
        IndexableStub stub = new IndexableStub();
        stub.setReturnNull(true);
        try {
            this.strategy.processIndexable(stub);
            fail("retun value is null must fail");
        } catch (NotIndexableException e) {
        }
        assertNull(this.strategy.content);
        try {
            this.strategy.createLuceneField();
            fail("content is not set");
        } catch (GdataIndexerException e) {
        }

        String content = "fooBar";
        stub.setReturnNull(false);
        stub.setReturnValueTextContent(content);
        this.strategy.processIndexable(stub);
        assertNotNull(this.strategy.content);
        assertEquals(content, this.strategy.content);

        Field[] fields = this.strategy.createLuceneField();
        assertEquals(1, fields.length);
        assertEquals(this.field.getName(), fields[0].name());
        assertEquals(content, fields[0].stringValue());
        // this is also stored an untok.
        assertTrue(fields[0].isStored());
        assertFalse(fields[0].isTokenized());
        
        
        // test for xpath exc.
        this.field.setPath(null);
        this.strategy = new KeywordStrategy(field);
        try {
            this.strategy.processIndexable(stub);
            fail("path is null must fail");
        } catch (NotIndexableException e) {
        }
        try {
            this.strategy.createLuceneField();
            fail("content is not set");
        } catch (GdataIndexerException e) {
        }

    }

}

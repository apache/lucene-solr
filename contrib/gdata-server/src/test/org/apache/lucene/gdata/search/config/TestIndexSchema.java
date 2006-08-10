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

import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.gdata.search.config.IndexSchemaField.ContentType;
import org.apache.lucene.gdata.search.index.IndexDocument;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 *
 */
public class TestIndexSchema extends TestCase {
    IndexSchema schema;

    protected void setUp() throws Exception {
        schema = new IndexSchema();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.config.IndexSchema.initialize()'
     */
    public void testInitialize() {
        try {
            schema.initialize();
            fail("def search field is null");
        } catch (RuntimeException e) {
            // TODO: handle exception
        }
        schema.setDefaultSearchField("someField");
        try {
            schema.initialize();
            fail("name is null");
        } catch (RuntimeException e) {
            // TODO: handle exception
        }
        schema.setName("someName");
        try {
            schema.initialize();
            fail("indexLocation  is null");
        } catch (RuntimeException e) {
            // TODO: handle exception
        }
        schema.setIndexLocation("/loc/loc");
        try {
            schema.initialize();
            fail("default search field is not set as a field");
        } catch (RuntimeException e) {
            // TODO: handle exception
        }
        IndexSchemaField f = new IndexSchemaField();
        f.setName(schema.getDefaultSearchField());
        f.setContentType(ContentType.TEXT);
        schema.addSchemaField(f);
        try {
            schema.initialize();
            fail("field check failed");
        } catch (RuntimeException e) {
            // TODO: handle exception
        }
        f.setPath("path");
        schema.initialize();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.config.IndexSchema.addSchemaField(IndexSchemaField)'
     */
    public void testAddSchemaField() {
        schema.addSchemaField(null);
        assertEquals(0, schema.getFields().size());

        IndexSchemaField f = new IndexSchemaField();
        f.setName(IndexDocument.FIELD_ENTRY_ID);
        schema.addSchemaField(f);
        assertEquals(0, schema.getFields().size());

        f.setName(IndexDocument.FIELD_FEED_ID);
        schema.addSchemaField(f);
        assertEquals(0, schema.getFields().size());

        f.setName("some");
        schema.addSchemaField(f);
        assertEquals(1, schema.getFields().size());
        assertEquals(StandardAnalyzer.class, schema.getServiceAnalyzer()
                .getClass());
        assertEquals(StandardAnalyzer.class, schema.getSchemaAnalyzer()
                .getClass());
        f.setName("someOther");
        f.setAnalyzerClass(StopAnalyzer.class);
        schema.addSchemaField(f);
        assertEquals(2, schema.getFields().size());
        assertEquals(PerFieldAnalyzerWrapper.class, schema.getSchemaAnalyzer()
                .getClass());
        schema.addSchemaField(f);
        assertEquals(3, schema.getFields().size());

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.config.IndexSchema.getSearchableFieldNames()'
     */
    public void testGetSearchableFieldNames() {
        IndexSchemaField f = new IndexSchemaField();
        f.setName("some");
        schema.addSchemaField(f);
        assertEquals(1, schema.getSearchableFieldNames().size());
        assertTrue(schema.getSearchableFieldNames().contains("some"));
    }

    public void testEquals() {
        assertFalse(schema.equals(null));
        assertFalse(schema.equals(new String()));
        assertTrue(schema.equals(schema));
        assertFalse(schema.equals(new IndexSchema()));
        IndexSchema s1 = new IndexSchema();
        s1.setName("someName");
        assertFalse(schema.equals(s1));
        schema.setName(s1.getName());
        assertTrue(schema.equals(s1));
    }

    public void testHashCode() {
        assertEquals(schema.hashCode(), schema.hashCode());
        assertNotNull(schema.hashCode());
        IndexSchema s1 = new IndexSchema();
        s1.setName("someName");
        assertTrue(schema.hashCode() != s1.hashCode());
        schema.setName(s1.getName());
        assertTrue(schema.hashCode() == s1.hashCode());
    }

    public void testToSTringNoNullPEx() {
        assertNotNull(schema.toString());
    }
}

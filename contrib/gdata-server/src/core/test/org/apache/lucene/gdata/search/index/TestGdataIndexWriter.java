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
package org.apache.lucene.gdata.search.index;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.lucene.store.RAMDirectory;

/**
 * @author Simon Willnauer
 *
 */
public class TestGdataIndexWriter extends TestCase {
    IndexSchema schemaNoPerFielAnalyzer;
    IndexSchema schemaPerFielAnalyzer;
    long VALUE_GT_DEFAULT_LONG = 15000;
    int VALUE_GT_DEFAULT_INT = 10000;
    
    protected void setUp() throws Exception {
        this.schemaNoPerFielAnalyzer = new IndexSchema();
        this.schemaPerFielAnalyzer = new IndexSchema();
        IndexSchemaField field = new IndexSchemaField();
        field.setName("someField");
        field.setAnalyzerClass(StopAnalyzer.class);
        this.schemaPerFielAnalyzer.addSchemaField(field);
        this.schemaPerFielAnalyzer.setCommitLockTimeout(VALUE_GT_DEFAULT_LONG);
        this.schemaPerFielAnalyzer.setMaxBufferedDocs(VALUE_GT_DEFAULT_INT);
        this.schemaPerFielAnalyzer.setMaxFieldLength(VALUE_GT_DEFAULT_INT);
        this.schemaPerFielAnalyzer.setMaxMergeDocs(VALUE_GT_DEFAULT_INT);
        this.schemaPerFielAnalyzer.setMergeFactor(VALUE_GT_DEFAULT_INT);
        this.schemaPerFielAnalyzer.setWriteLockTimeout(VALUE_GT_DEFAULT_LONG);
        this.schemaPerFielAnalyzer.setUseCompoundFile(true);
    }


    /**
     * Test method for 'org.apache.lucene.gdata.search.index.GDataIndexWriter.GDataIndexWriter(Directory, boolean, IndexSchema)'
     * @throws IOException 
     */
    public void testGDataIndexWriter() throws IOException {
        try{
        new GDataIndexWriter(new RAMDirectory(),true,null);
        fail("no index schema");
        }catch (IllegalArgumentException e) {}
        GDataIndexWriter writer = new GDataIndexWriter(new RAMDirectory(),true,this.schemaNoPerFielAnalyzer);
        assertTrue(writer.getAnalyzer().getClass() == StandardAnalyzer.class);
        
        writer = new GDataIndexWriter(new RAMDirectory(),true,this.schemaPerFielAnalyzer);
        assertTrue(writer.getAnalyzer().getClass() == PerFieldAnalyzerWrapper.class);
        assertEquals(VALUE_GT_DEFAULT_LONG,writer.getCommitLockTimeout());
        assertEquals(VALUE_GT_DEFAULT_LONG,writer.getWriteLockTimeout());
        assertEquals(VALUE_GT_DEFAULT_INT,writer.getMaxBufferedDocs());
        assertEquals(VALUE_GT_DEFAULT_INT,writer.getMaxMergeDocs());
        assertEquals(VALUE_GT_DEFAULT_INT,writer.getMaxFieldLength());
        assertEquals(VALUE_GT_DEFAULT_INT,writer.getMergeFactor());
        assertTrue(writer.getUseCompoundFile());
    }

}

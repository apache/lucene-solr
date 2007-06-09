package org.apache.lucene.gdata.storage.lucenestorage; 

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
import java.util.ArrayList; 
import java.util.List; 
 
import junit.framework.TestCase; 
 
import org.apache.lucene.analysis.standard.StandardAnalyzer; 
import org.apache.lucene.document.Document; 
import org.apache.lucene.document.Field; 
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper; 
import org.apache.lucene.gdata.utils.ModifiedEntryFilter;
import org.apache.lucene.index.IndexReader; 
import org.apache.lucene.index.IndexWriter; 
import org.apache.lucene.index.Term; 
import org.apache.lucene.search.Hits; 
import org.apache.lucene.search.IndexSearcher; 
import org.apache.lucene.search.Query; 
import org.apache.lucene.search.Searcher; 
import org.apache.lucene.search.TermQuery; 
import org.apache.lucene.store.RAMDirectory; 
 
public class TestModifiedEntryFilter extends TestCase { 
    IndexWriter writer; 
    IndexReader reader; 
    List<String> excludeList; 
    String feedID = "feed"; 
    String fieldFeedId = "feedID"; 
    protected void setUp() throws Exception { 
        RAMDirectory dir = new RAMDirectory(); 
        this.writer = new IndexWriter(dir,new StandardAnalyzer(),true); 
        Document doc = new Document(); 
        doc.add(new Field(StorageEntryWrapper.FIELD_ENTRY_ID,"1",Field.Store.YES,Field.Index.UN_TOKENIZED)); 
        doc.add(new Field(fieldFeedId,feedID,Field.Store.YES,Field.Index.UN_TOKENIZED)); 
        Document doc1 = new Document(); 
        doc1.add(new Field(StorageEntryWrapper.FIELD_ENTRY_ID,"2",Field.Store.YES,Field.Index.UN_TOKENIZED)); 
        doc1.add(new Field(fieldFeedId,feedID,Field.Store.YES,Field.Index.UN_TOKENIZED)); 
        this.writer.addDocument(doc); 
        this.writer.addDocument(doc1); 
        this.writer.close(); 
        this.reader = IndexReader.open(dir); 
        this.excludeList = new ArrayList(); 
        this.excludeList.add("1"); 
         
         
    } 
 
    protected void tearDown() throws Exception { 
        super.tearDown(); 
    } 
    public void testFilter() throws IOException{ 
        Searcher s = new IndexSearcher(this.reader); 
        Query q = new TermQuery(new Term(fieldFeedId,feedID)); 
        Hits hits = s.search(q); 
        assertEquals(2,hits.length()); 
         
        hits = s.search(q,new ModifiedEntryFilter(this.excludeList.toArray(new String[0]),StorageEntryWrapper.FIELD_ENTRY_ID)); 
        assertEquals(1,hits.length()); 
        this.excludeList.add("2"); 
 
        hits = s.search(q,new ModifiedEntryFilter(this.excludeList.toArray(new String[0]),StorageEntryWrapper.FIELD_ENTRY_ID)); 
        assertEquals(0,hits.length());
        this.excludeList.add(null);
        this.excludeList.add("5"); 
        hits = s.search(q,new ModifiedEntryFilter(this.excludeList.toArray(new String[0]),StorageEntryWrapper.FIELD_ENTRY_ID)); 
        assertEquals(0,hits.length()); 
         
    } 
} 

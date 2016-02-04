/*
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
package org.apache.lucene.search;

import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;

import java.io.IOException;

/**
 * DateFilter JUnit tests.
 * 
 * 
 */
public class TestDateFilter extends LuceneTestCase {
 
  /**
   *
   */
  public void testBefore() throws IOException {
    // create an index
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
    
    long now = System.currentTimeMillis();
    
    Document doc = new Document();
    // add time that is in the past
    doc.add(newStringField("datefield", DateTools.timeToString(now - 1000, DateTools.Resolution.MILLISECOND), Field.Store.YES));
    doc.add(newTextField("body", "Today is a very sunny day in New York City", Field.Store.YES));
    writer.addDocument(doc);
    
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    
    // filter that should preserve matches
    // DateFilter df1 = DateFilter.Before("datefield", now);
    Filter df1 = new QueryWrapperFilter(TermRangeQuery.newStringRange("datefield", DateTools
        .timeToString(now - 2000, DateTools.Resolution.MILLISECOND), DateTools
        .timeToString(now, DateTools.Resolution.MILLISECOND), false, true));
    // filter that should discard matches
    // DateFilter df2 = DateFilter.Before("datefield", now - 999999);
    Filter df2 = new QueryWrapperFilter(TermRangeQuery.newStringRange("datefield", DateTools
        .timeToString(0, DateTools.Resolution.MILLISECOND), DateTools
        .timeToString(now - 2000, DateTools.Resolution.MILLISECOND), true,
        false));
    
    // search something that doesn't exist with DateFilter
    Query query1 = new TermQuery(new Term("body", "NoMatchForThis"));
    
    // search for something that does exists
    Query query2 = new TermQuery(new Term("body", "sunny"));
    
    ScoreDoc[] result;
    
    // ensure that queries return expected results without DateFilter first
    result = searcher.search(query1, 1000).scoreDocs;
    assertEquals(0, result.length);
    
    result = searcher.search(query2, 1000).scoreDocs;
    assertEquals(1, result.length);
    
    // run queries with DateFilter
    result = searcher.search(new FilteredQuery(query1, df1), 1000).scoreDocs;
    assertEquals(0, result.length);
    
    result = searcher.search(new FilteredQuery(query1, df2), 1000).scoreDocs;
    assertEquals(0, result.length);
    
    result = searcher.search(new FilteredQuery(query2, df1), 1000).scoreDocs;
    assertEquals(1, result.length);
    
    result = searcher.search(new FilteredQuery(query2, df2), 1000).scoreDocs;
    assertEquals(0, result.length);
    reader.close();
    indexStore.close();
  }
  
  /**
   *
   */
  public void testAfter() throws IOException {
    // create an index
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
    
    long now = System.currentTimeMillis();
    
    Document doc = new Document();
    // add time that is in the future
    doc.add(newStringField("datefield", DateTools.timeToString(now + 888888, DateTools.Resolution.MILLISECOND), Field.Store.YES));
    doc.add(newTextField("body", "Today is a very sunny day in New York City", Field.Store.YES));
    writer.addDocument(doc);
    
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    
    // filter that should preserve matches
    // DateFilter df1 = DateFilter.After("datefield", now);
    Filter df1 = new QueryWrapperFilter(TermRangeQuery.newStringRange("datefield", DateTools
        .timeToString(now, DateTools.Resolution.MILLISECOND), DateTools
        .timeToString(now + 999999, DateTools.Resolution.MILLISECOND), true,
        false));
    // filter that should discard matches
    // DateFilter df2 = DateFilter.After("datefield", now + 999999);
    Filter df2 = new QueryWrapperFilter(TermRangeQuery.newStringRange("datefield", DateTools
        .timeToString(now + 999999, DateTools.Resolution.MILLISECOND),
        DateTools.timeToString(now + 999999999,
            DateTools.Resolution.MILLISECOND), false, true));
    
    // search something that doesn't exist with DateFilter
    Query query1 = new TermQuery(new Term("body", "NoMatchForThis"));
    
    // search for something that does exists
    Query query2 = new TermQuery(new Term("body", "sunny"));
    
    ScoreDoc[] result;
    
    // ensure that queries return expected results without DateFilter first
    result = searcher.search(query1, 1000).scoreDocs;
    assertEquals(0, result.length);
    
    result = searcher.search(query2, 1000).scoreDocs;
    assertEquals(1, result.length);
    
    // run queries with DateFilter
    result = searcher.search(new FilteredQuery(query1, df1), 1000).scoreDocs;
    assertEquals(0, result.length);
    
    result = searcher.search(new FilteredQuery(query1, df2), 1000).scoreDocs;
    assertEquals(0, result.length);
    
    result = searcher.search(new FilteredQuery(query2, df1), 1000).scoreDocs;
    assertEquals(1, result.length);
    
    result = searcher.search(new FilteredQuery(query2, df2), 1000).scoreDocs;
    assertEquals(0, result.length);
    reader.close();
    indexStore.close();
  }
}

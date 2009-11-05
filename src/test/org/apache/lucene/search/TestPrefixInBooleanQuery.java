package org.apache.lucene.search;

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

import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

/**
 * https://issues.apache.org/jira/browse/LUCENE-1974
 *
 * represent the bug of 
 * 
 *    BooleanScorer.score(Collector collector, int max, int firstDocID)
 * 
 * Line 273, end=8192, subScorerDocID=11378, then more got false?
 * 
 */
public class TestPrefixInBooleanQuery extends LuceneTestCase {

  private static final String FIELD = "name";
  private RAMDirectory directory = new RAMDirectory();

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    
    IndexWriter writer = new IndexWriter(directory,
                                         new WhitespaceAnalyzer(), true,
                                         IndexWriter.MaxFieldLength.LIMITED);

    for (int i = 0; i < 5137; ++i) {
      Document doc = new Document();
      doc.add(new Field(FIELD, "meaninglessnames", Field.Store.YES,
                        Field.Index.NOT_ANALYZED));
      writer.addDocument(doc);
    }
    { 
      Document doc = new Document();
      doc.add(new Field(FIELD, "tangfulin", Field.Store.YES,
                        Field.Index.NOT_ANALYZED));
      writer.addDocument(doc);
    }

    for (int i = 5138; i < 11377; ++i) {
      Document doc = new Document();
      doc.add(new Field(FIELD, "meaninglessnames", Field.Store.YES,
                        Field.Index.NOT_ANALYZED));
      writer.addDocument(doc);
    }
    {
      Document doc = new Document();
      doc.add(new Field(FIELD, "tangfulin", Field.Store.YES,
                        Field.Index.NOT_ANALYZED));
      writer.addDocument(doc);
    }
    
    writer.close();
  }
  
  public void testPrefixQuery() throws Exception {
    IndexSearcher indexSearcher = new IndexSearcher(directory, true);
    Query query = new PrefixQuery(new Term(FIELD, "tang"));
    assertEquals("Number of matched documents", 2,
                 indexSearcher.search(query, null, 1000).totalHits);
  }
  public void testTermQuery() throws Exception {
    IndexSearcher indexSearcher = new IndexSearcher(directory, true);
    Query query = new TermQuery(new Term(FIELD, "tangfulin"));
    assertEquals("Number of matched documents", 2,
                 indexSearcher.search(query, null, 1000).totalHits);
  }
  public void testTermBooleanQuery() throws Exception {
    IndexSearcher indexSearcher = new IndexSearcher(directory, true);
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(FIELD, "tangfulin")),
              BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term(FIELD, "notexistnames")),
              BooleanClause.Occur.SHOULD);
    assertEquals("Number of matched documents", 2,
                 indexSearcher.search(query, null, 1000).totalHits);

  }
  public void testPrefixBooleanQuery() throws Exception {
    IndexSearcher indexSearcher = new IndexSearcher(directory, true);
    BooleanQuery query = new BooleanQuery();
    query.add(new PrefixQuery(new Term(FIELD, "tang")),
              BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term(FIELD, "notexistnames")),
              BooleanClause.Occur.SHOULD);
    assertEquals("Number of matched documents", 2,
                 indexSearcher.search(query, null, 1000).totalHits);
  }
}

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

import java.io.IOException;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

import junit.framework.TestCase;

/**
 *
 * @version $rcs = ' $Id$ ' ;
 */
public class TestBooleanScorer extends TestCase
{

  public TestBooleanScorer(String name) {
    super(name);
  }

  private static final String FIELD = "category";
  
  public void testMethod() {
    RAMDirectory directory = new RAMDirectory();

    String[] values = new String[] { "1", "2", "3", "4" };

    try {
      IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true);
      for (int i = 0; i < values.length; i++) {
        Document doc = new Document();
        doc.add(new Field(FIELD, values[i], Field.Store.YES, Field.Index.UN_TOKENIZED));
        writer.addDocument(doc);
      }
      writer.close();

      BooleanQuery booleanQuery1 = new BooleanQuery();
      booleanQuery1.add(new TermQuery(new Term(FIELD, "1")), BooleanClause.Occur.SHOULD);
      booleanQuery1.add(new TermQuery(new Term(FIELD, "2")), BooleanClause.Occur.SHOULD);

      BooleanQuery query = new BooleanQuery();
      query.add(booleanQuery1, BooleanClause.Occur.MUST);
      query.add(new TermQuery(new Term(FIELD, "9")), BooleanClause.Occur.MUST_NOT);

      IndexSearcher indexSearcher = new IndexSearcher(directory);
      Hits hits = indexSearcher.search(query);
      assertEquals("Number of matched documents", 2, hits.length());

    }
    catch (IOException e) {
      fail(e.getMessage());
    }

  }
  

}

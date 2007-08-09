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

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;

import java.io.IOException;

/**
 *
 * @version $Id$
 **/

public class TestBooleanPrefixQuery extends TestCase {

  public static void main(String[] args) {
    TestRunner.run(suite());
  }

  public static Test suite() {
    return new TestSuite(TestBooleanPrefixQuery.class);
  }

  public TestBooleanPrefixQuery(String name) {
    super(name);
  }

  public void testMethod() {
    RAMDirectory directory = new RAMDirectory();

    String[] categories = new String[]{"food",
                                       "foodanddrink",
                                       "foodanddrinkandgoodtimes",
                                       "food and drink"};

    Query rw1 = null;
    Query rw2 = null;
    try {
      IndexWriter writer = new IndexWriter(directory, new
                                           WhitespaceAnalyzer(), true);
      for (int i = 0; i < categories.length; i++) {
        Document doc = new Document();
        doc.add(new Field("category", categories[i], Field.Store.YES, Field.Index.UN_TOKENIZED));
        writer.addDocument(doc);
      }
      writer.close();
      
      IndexReader reader = IndexReader.open(directory);
      PrefixQuery query = new PrefixQuery(new Term("category", "foo"));
      
      rw1 = query.rewrite(reader);
      
      BooleanQuery bq = new BooleanQuery();
      bq.add(query, BooleanClause.Occur.MUST);
      
      rw2 = bq.rewrite(reader);
    } catch (IOException e) {
      fail(e.getMessage());
    }

    BooleanQuery bq1 = null;
    if (rw1 instanceof BooleanQuery) {
      bq1 = (BooleanQuery) rw1;
    }

    BooleanQuery bq2 = null;
    if (rw2 instanceof BooleanQuery) {
        bq2 = (BooleanQuery) rw2;
    } else {
      fail("Rewrite");
    }

    assertEquals("Number of Clauses Mismatch", bq1.getClauses().length,
                 bq2.getClauses().length);
  }
}


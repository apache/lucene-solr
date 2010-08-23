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

import java.util.Random;

import org.apache.lucene.util.LuceneTestCase;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.store.Directory;

/**
 *
 **/

public class TestBooleanPrefixQuery extends LuceneTestCase {

  public static void main(String[] args) {
    TestRunner.run(suite());
  }

  public static Test suite() {
    return new TestSuite(TestBooleanPrefixQuery.class);
  }

  public TestBooleanPrefixQuery(String name) {
    super(name);
  }

  private int getCount(IndexReader r, Query q) throws Exception {
    if (q instanceof BooleanQuery) {
      return ((BooleanQuery) q).getClauses().length;
    } else if (q instanceof ConstantScoreQuery) {
      DocIdSetIterator iter = ((ConstantScoreQuery) q).getFilter().getDocIdSet(r).iterator();
      int count = 0;
      while(iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        count++;
      }
      return count;
    } else {
      throw new RuntimeException("unepxected query " + q);
    }
  }

  public void testMethod() throws Exception {
    Random random = newRandom();
    Directory directory = newDirectory(random);

    String[] categories = new String[]{"food",
                                       "foodanddrink",
                                       "foodanddrinkandgoodtimes",
                                       "food and drink"};

    Query rw1 = null;
    Query rw2 = null;
    IndexReader reader = null;
    RandomIndexWriter writer = new RandomIndexWriter(random, directory);
    for (int i = 0; i < categories.length; i++) {
      Document doc = new Document();
      doc.add(new Field("category", categories[i], Field.Store.YES, Field.Index.NOT_ANALYZED));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    writer.close();
      
    PrefixQuery query = new PrefixQuery(new Term("category", "foo"));
    rw1 = query.rewrite(reader);
      
    BooleanQuery bq = new BooleanQuery();
    bq.add(query, BooleanClause.Occur.MUST);
      
    rw2 = bq.rewrite(reader);

    assertEquals("Number of Clauses Mismatch", getCount(reader, rw1), getCount(reader, rw2));
    reader.close();
    directory.close();
  }
}


package org.apache.lucene.search.intervals;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;

public class TestDisjunctionIntervalIterator extends IntervalTestBase {

  protected void addDocs(RandomIndexWriter writer) throws IOException {
    {
      Document doc = new Document();
      doc.add(newField(
          "field",
          "Pease porridge hot! Pease porridge cold! Pease porridge in the pot nine days old! Some like it hot, some"
              + " like it cold, Some like it in the pot nine days old! Pease porridge hot! Pease porridge cold!",
              TextField.TYPE_STORED));
      writer.addDocument(doc);
    }

    {
      Document doc = new Document();
      doc.add(newField(
          "field",
          "Pease porridge cold! Pease porridge hot! Pease porridge in the pot nine days old! Some like it cold, some"
              + " like it hot, Some like it in the pot nine days old! Pease porridge cold! Pease porridge hot!",
              TextField.TYPE_STORED));
      writer.addDocument(doc);
    }

    {
      Document doc = new Document();
      doc.add(newField("field", "The quick brown fox jumps over the lazy porridge", TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }
  }


  public void testDisjunctionRangePositionsBooleanQuery() throws IOException {

    BooleanQuery query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term("field", "porridge")), Occur.SHOULD));
    query.add(new BooleanClause(new TermQuery(new Term("field", "pease")), Occur.SHOULD));
    query.add(new BooleanClause(new TermQuery(new Term("field", "hot!")), Occur.SHOULD));

    IntervalFilterQuery rangeFilter = new IntervalFilterQuery(query, new RangeIntervalFilter(0, 2));
    checkIntervals(rangeFilter, searcher, new int[][]{
        { 0, 0, 0, 1, 1, 2, 2 },
        { 1, 0, 0, 1, 1 }
    });

  }

  public void testDisjunctionPartialMatchQuery() throws IOException {

    BooleanQuery query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term("field", "porridge")), Occur.SHOULD));
    query.add(new BooleanClause(new TermQuery(new Term("field", "fox")), Occur.SHOULD));

    checkIntervals(query, searcher, new int[][]{
        { 0, 1, 1, 4, 4, 7, 7, 32, 32, 35, 35 },
        { 1, 1, 1, 4, 4, 7, 7, 32, 32, 35, 35},
        { 2, 3, 3, 8, 8}
    });
  }

  public void testDisjunctionFullMatchQuery() throws IOException {

    BooleanQuery query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term("field", "porridge")), Occur.SHOULD));
    query.add(new BooleanClause(new TermQuery(new Term("field", "pease")), Occur.SHOULD));
    query.add(new BooleanClause(new TermQuery(new Term("field", "hot!")), Occur.SHOULD));

    checkIntervals(query, searcher, new int[][]{
        { 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 6, 6, 7, 7,
            17, 17, 31, 31, 32, 32, 33, 33, 34, 34, 35, 35 },
        { 1, 0, 0, 1, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7,
            20, 20, 31, 31, 32, 32, 34, 34, 35, 35, 36, 36},
        { 2, 8, 8 }
    });

  }


}


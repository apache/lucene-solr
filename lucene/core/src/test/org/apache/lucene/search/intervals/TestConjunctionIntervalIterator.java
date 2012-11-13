package org.apache.lucene.search.intervals;
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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import java.io.IOException;

public class TestConjunctionIntervalIterator extends IntervalTestBase {
  
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
  }

  public void testConjunctionRangeIntervalQuery() throws IOException {
    BooleanQuery q = new BooleanQuery();
    q.add(makeTermQuery("porridge"), Occur.MUST);
    q.add(makeTermQuery("pease"), Occur.MUST);
    q.add(makeTermQuery("hot!"), Occur.MUST);
    Query rangeQuery = new IntervalFilterQuery(q, new RangeIntervalFilter(0, 2));
    checkIntervals(rangeQuery, searcher, new int[][]{
        { 0, 0, 2, 0, 0, 1, 1, 2, 2 }
    });
  }

  public void testConjunctionOrderedQuery() throws IOException {
    Query q = new OrderedNearQuery(0, false, makeTermQuery("pease"),
                                    makeTermQuery("porridge"), makeTermQuery("hot!"));
    checkIntervals(q, searcher, new int[][]{
        { 0, 0, 2, 31, 33 },
        { 1, 3, 5, 34, 36 }
    });
  }

  public void testConjunctionUnorderedQuery() throws IOException {
    Query q = new UnorderedNearQuery(0, false, makeTermQuery("pease"),
                                      makeTermQuery("porridge"), makeTermQuery("hot!"));
    checkIntervals(q, searcher, new int[][]{
        { 0, 0, 2, 1, 3, 2, 4, 31, 33, 32, 34, 33, 35 },
        { 1, 3, 5, 4, 6, 5, 7, 34, 36 }
    });
  }

}

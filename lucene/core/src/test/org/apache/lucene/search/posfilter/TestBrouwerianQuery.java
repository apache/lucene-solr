package org.apache.lucene.search.posfilter;
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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.posfilter.NonOverlappingQuery;
import org.apache.lucene.search.posfilter.OrderedNearQuery;
import org.apache.lucene.search.posfilter.UnorderedNearQuery;

import java.io.IOException;

public class TestBrouwerianQuery extends IntervalTestBase {
  
  protected void addDocs(RandomIndexWriter writer) throws IOException {
    {
      Document doc = new Document();
      doc.add(newField(
          "field",
          "The quick brown fox jumps over the lazy dog",
              TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
    
    {
      Document doc = new Document();
      doc.add(newField(
          "field",
          "The quick brown duck jumps over the lazy dog with the quick brown fox jumps",
              TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
  }
  
  public void testBrouwerianBooleanQuery() throws IOException {

    Query query = new OrderedNearQuery(2, makeTermQuery("the"),
                                        makeTermQuery("quick"), makeTermQuery("jumps"));
    Query sub = makeTermQuery("fox");
    NonOverlappingQuery q = new NonOverlappingQuery(query, sub);

    checkIntervals(q, searcher, new int[][]{
        { 1, 0, 4 }
    });
  }

  public void testBrouwerianBooleanQueryExcludedDoesNotExist() throws IOException {

    Query query = new OrderedNearQuery(2, makeTermQuery("the"),
        makeTermQuery("quick"), makeTermQuery("jumps"));
    Query sub = makeTermQuery("blox");
    NonOverlappingQuery q = new NonOverlappingQuery(query, sub);

    checkIntervals(q, searcher, new int[][]{
        { 0, 0, 4 },
        { 1, 0, 4, 10, 14 }
    });
  }

  public void testBrouwerianOverlapQuery() throws IOException {
    // We want to find 'jumps NOT WITHIN 2 positions of fox'
    Query sub = new UnorderedNearQuery(2, makeTermQuery("jumps"), makeTermQuery("fox"));
    Query query = makeTermQuery("jumps");
    NonOverlappingQuery q = new NonOverlappingQuery(query, sub);

    checkIntervals(q, searcher, new int[][]{
        { 1, 4, 4 }
    });
  }

  public void testBrouwerianNonExistentOverlapQuery() throws IOException {
    Query sub = new UnorderedNearQuery(2, makeTermQuery("dog"), makeTermQuery("over"));
    Query query = makeTermQuery("dog");
    NonOverlappingQuery q = new NonOverlappingQuery(query, sub);

    checkIntervals(q, searcher, new int[][]{});
  }

  public void testBrouwerianExistentOverlapQuery() throws IOException {
    Query sub = new UnorderedNearQuery(1, makeTermQuery("dog"), makeTermQuery("over"));
    Query query = makeTermQuery("dog");
    NonOverlappingQuery q = new NonOverlappingQuery(query, sub);

    checkIntervals(q, searcher, new int[][]{
        { 0, 8, 8 },
        { 1, 8, 8 }
    });
  }

}

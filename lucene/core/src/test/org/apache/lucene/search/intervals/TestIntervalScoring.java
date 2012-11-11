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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Assert;

import java.io.IOException;

public class TestIntervalScoring extends LuceneTestCase {

  private IndexSearcher searcher;
  private IndexReader reader;
  private Directory directory;

  public static final String field = "field";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
            .setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newField(field, docFields[i], TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
    searcher.setSimilarity(new DefaultSimilarity());
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  private String[] docFields = {
      "Should we, could we, would we?",
      "It should -  would it?",
      "It shouldn't",
      "Should we, should we, should we"
  };

  public void testOrderedNearQueryScoring() throws IOException {
    OrderedNearQuery q = new OrderedNearQuery(10, new TermQuery(new Term(field, "should")),
                                                  new TermQuery(new Term(field, "would")));
    TopDocs docs = searcher.search(q, 10);
    Assert.assertEquals(docs.scoreDocs[0].doc, 1);
    Assert.assertEquals(docs.scoreDocs[1].doc, 0);
  }

  public void testEmptyMultiTermQueryScoring() throws IOException {
    OrderedNearQuery q = new OrderedNearQuery(10, new RegexpQuery(new Term(field, "bar.*")),
                                                  new RegexpQuery(new Term(field, "foo.*")));
    TopDocs docs = searcher.search(q, 10);
    Assert.assertEquals(docs.totalHits, 0);
  }

}

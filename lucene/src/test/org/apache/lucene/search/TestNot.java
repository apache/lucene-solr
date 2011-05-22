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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/** Similarity unit test.
 *
 *
 * @version $Revision$
 */
public class TestNot extends LuceneTestCase {

  public void testNot() throws Exception {
    Directory store = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, store);

    Document d1 = new Document();
    d1.add(newField("field", "a b", Field.Store.YES, Field.Index.ANALYZED));

    writer.addDocument(d1);
    IndexReader reader = writer.getReader();

    IndexSearcher searcher = newSearcher(reader);
      QueryParser parser = new QueryParser(TEST_VERSION_CURRENT, "field", new MockAnalyzer(random));
    Query query = parser.parse("a NOT b");
    //System.out.println(query);
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    writer.close();
    searcher.close();
    reader.close();
    store.close();
  }
}

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
import org.apache.lucene.store.Directory;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;

/**
 * Tests {@link PrefixFilter} class.
 *
 */
public class TestPrefixFilter extends LuceneTestCase {
  public void testPrefixFilter() throws Exception {
    Directory directory = newDirectory();

    String[] categories = new String[] {"/Computers/Linux",
                                        "/Computers/Mac/One",
                                        "/Computers/Mac/Two",
                                        "/Computers/Windows"};
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    for (int i = 0; i < categories.length; i++) {
      Document doc = new Document();
      doc.add(newField("category", categories[i], StringField.TYPE_STORED));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();

    // PrefixFilter combined with ConstantScoreQuery
    PrefixFilter filter = new PrefixFilter(new Term("category", "/Computers"));
    Query query = new ConstantScoreQuery(filter);
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(4, hits.length);

    // test middle of values
    filter = new PrefixFilter(new Term("category", "/Computers/Mac"));
    query = new ConstantScoreQuery(filter);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(2, hits.length);

    // test start of values
    filter = new PrefixFilter(new Term("category", "/Computers/Linux"));
    query = new ConstantScoreQuery(filter);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    // test end of values
    filter = new PrefixFilter(new Term("category", "/Computers/Windows"));
    query = new ConstantScoreQuery(filter);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    // test non-existant
    filter = new PrefixFilter(new Term("category", "/Computers/ObsoleteOS"));
    query = new ConstantScoreQuery(filter);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // test non-existant, before values
    filter = new PrefixFilter(new Term("category", "/Computers/AAA"));
    query = new ConstantScoreQuery(filter);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // test non-existant, after values
    filter = new PrefixFilter(new Term("category", "/Computers/ZZZ"));
    query = new ConstantScoreQuery(filter);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // test zero length prefix
    filter = new PrefixFilter(new Term("category", ""));
    query = new ConstantScoreQuery(filter);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(4, hits.length);

    // test non existent field
    filter = new PrefixFilter(new Term("nonexistantfield", "/Computers"));
    query = new ConstantScoreQuery(filter);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    
    writer.close();
    reader.close();
    directory.close();
  }
}

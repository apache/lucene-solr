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

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;

/**
 * Tests {@link PrefixQuery} class.
 *
 */
public class TestPrefixQuery extends LuceneTestCase {
  public void testPrefixQuery() throws Exception {
    Directory directory = newDirectory();

    String[] categories = new String[] {"/Computers",
                                        "/Computers/Mac",
                                        "/Computers/Windows"};
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    for (int i = 0; i < categories.length; i++) {
      Document doc = new Document();
      doc.add(newField("category", categories[i], StringField.TYPE_STORED));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();

    PrefixQuery query = new PrefixQuery(new Term("category", "/Computers"));
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("All documents in /Computers category and below", 3, hits.length);

    query = new PrefixQuery(new Term("category", "/Computers/Mac"));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("One in /Computers/Mac", 1, hits.length);

    query = new PrefixQuery(new Term("category", ""));
    Terms terms = MultiFields.getTerms(searcher.getIndexReader(), "category");
    assertFalse(query.getTermsEnum(terms) instanceof PrefixTermsEnum);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("everything", 3, hits.length);
    writer.close();
    reader.close();
    directory.close();
  }
}

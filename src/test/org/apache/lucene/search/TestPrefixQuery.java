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
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/**
 * Tests {@link PrefixQuery} class.
 *
 */
public class TestPrefixQuery extends LuceneTestCase {
  public void testPrefixQuery() throws Exception {
    RAMDirectory directory = new RAMDirectory();

    String[] categories = new String[] {"/Computers",
                                        "/Computers/Mac",
                                        "/Computers/Windows"};
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    for (int i = 0; i < categories.length; i++) {
      Document doc = new Document();
      doc.add(new Field("category", categories[i], Field.Store.YES, Field.Index.NOT_ANALYZED));
      writer.addDocument(doc);
    }
    writer.close();

    PrefixQuery query = new PrefixQuery(new Term("category", "/Computers"));
    IndexSearcher searcher = new IndexSearcher(directory);
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("All documents in /Computers category and below", 3, hits.length);

    query = new PrefixQuery(new Term("category", "/Computers/Mac"));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("One in /Computers/Mac", 1, hits.length);
  }
}

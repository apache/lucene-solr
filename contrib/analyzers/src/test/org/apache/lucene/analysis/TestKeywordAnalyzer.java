package org.apache.lucene.analysis;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Hits;
import org.apache.lucene.queryParser.QueryParser;

public class TestKeywordAnalyzer extends TestCase {
  RAMDirectory directory;
  private IndexSearcher searcher;

  public void setUp() throws Exception {
    directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory,
                                         new SimpleAnalyzer(),
                                         true);

    Document doc = new Document();
    doc.add(Field.Keyword("partnum", "Q36"));
    doc.add(Field.Text("description", "Illidium Space Modulator"));
    writer.addDocument(doc);

    writer.close();

    searcher = new IndexSearcher(directory);
  }

  public void testPerFieldAnalyzer() throws Exception {
    PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(
                                              new SimpleAnalyzer());
    analyzer.addAnalyzer("partnum", new KeywordAnalyzer());

    Query query = QueryParser.parse("partnum:Q36 AND SPACE",
                                    "description",
                                    analyzer);

    Hits hits = searcher.search(query);
    assertEquals("Q36 kept as-is",
              "+partnum:Q36 +space", query.toString("description"));
    assertEquals("doc found!", 1, hits.length());
  }
}

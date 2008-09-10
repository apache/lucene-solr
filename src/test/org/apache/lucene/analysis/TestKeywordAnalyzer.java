package org.apache.lucene.analysis;

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
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

public class TestKeywordAnalyzer extends LuceneTestCase {
  
  private RAMDirectory directory;
  private IndexSearcher searcher;

  public void setUp() throws Exception {
    super.setUp();
    directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory,
                                         new SimpleAnalyzer(),
                                         true, IndexWriter.MaxFieldLength.LIMITED);

    Document doc = new Document();
    doc.add(new Field("partnum", "Q36", Field.Store.YES, Field.Index.NOT_ANALYZED));
    doc.add(new Field("description", "Illidium Space Modulator", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    writer.close();

    searcher = new IndexSearcher(directory);
  }

  public void testPerFieldAnalyzer() throws Exception {
    PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new SimpleAnalyzer());
    analyzer.addAnalyzer("partnum", new KeywordAnalyzer());

    QueryParser queryParser = new QueryParser("description", analyzer);
    Query query = queryParser.parse("partnum:Q36 AND SPACE");

    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("Q36 kept as-is",
              "+partnum:Q36 +space", query.toString("description"));
    assertEquals("doc found!", 1, hits.length);
  }

  public void testMutipleDocument() throws Exception {
    RAMDirectory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,new KeywordAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("partnum", "Q36", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new Field("partnum", "Q37", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();

    IndexReader reader = IndexReader.open(dir);
    TermDocs td = reader.termDocs(new Term("partnum", "Q36"));
    assertTrue(td.next());
    td = reader.termDocs(new Term("partnum", "Q37"));
    assertTrue(td.next());
  }
}

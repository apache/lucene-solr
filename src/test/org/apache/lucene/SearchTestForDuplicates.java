package org.apache.lucene;

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

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

class SearchTestForDuplicates {

  static final String PRIORITY_FIELD ="priority";
  static final String ID_FIELD ="id";
  static final String HIGH_PRIORITY ="high";
  static final String MED_PRIORITY ="medium";
  static final String LOW_PRIORITY ="low";

  public static void main(String[] args) {
    try {
      Directory directory = new RAMDirectory();
      Analyzer analyzer = new SimpleAnalyzer();
      IndexWriter writer = new IndexWriter(directory, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);

      final int MAX_DOCS = 225;

      for (int j = 0; j < MAX_DOCS; j++) {
        Document d = new Document();
        d.add(new Field(PRIORITY_FIELD, HIGH_PRIORITY, Field.Store.YES, Field.Index.ANALYZED));
        d.add(new Field(ID_FIELD, Integer.toString(j), Field.Store.YES, Field.Index.ANALYZED));
        writer.addDocument(d);
      }
      writer.close();

      // try a search without OR
      Searcher searcher = new IndexSearcher(directory);
      ScoreDoc[] hits = null;

      QueryParser parser = new QueryParser(PRIORITY_FIELD, analyzer);

      Query query = parser.parse(HIGH_PRIORITY);
      System.out.println("Query: " + query.toString(PRIORITY_FIELD));

      hits = searcher.search(query, null, 1000).scoreDocs;
      printHits(hits, searcher);

      searcher.close();

      // try a new search with OR
      searcher = new IndexSearcher(directory);
      hits = null;

      parser = new QueryParser(PRIORITY_FIELD, analyzer);

      query = parser.parse(HIGH_PRIORITY + " OR " + MED_PRIORITY);
      System.out.println("Query: " + query.toString(PRIORITY_FIELD));

      hits = searcher.search(query, null, 1000).scoreDocs;
      printHits(hits, searcher);

      searcher.close();

    } catch (Exception e) {
      System.out.println(" caught a " + e.getClass() +
                         "\n with message: " + e.getMessage());
    }
  }

  private static void printHits( ScoreDoc[] hits, Searcher searcher) throws IOException {
    System.out.println(hits.length + " total results\n");
    for (int i = 0 ; i < hits.length; i++) {
      if ( i < 10 || (i > 94 && i < 105) ) {
        Document d = searcher.doc(hits[i].doc);
        System.out.println(i + " " + d.get(ID_FIELD));
      }
    }
  }

}

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
package org.apache.lucene.facet;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestFacetQuery extends FacetTestCase {

  private static Directory indexDirectory;
  private static RandomIndexWriter indexWriter;
  private static IndexReader indexReader;
  private static IndexSearcher searcher;
  private static FacetsConfig config;

  private static final IndexableField[] DOC_SINGLEVALUED =
          new IndexableField[] { new SortedSetDocValuesFacetField("Author", "Mark Twain") };

  private static final IndexableField[] DOC_MULTIVALUED =
          new SortedSetDocValuesFacetField[] { new SortedSetDocValuesFacetField("Author", "Kurt Vonnegut") };

  private static final IndexableField[] DOC_NOFACET =
          new IndexableField[] { new TextField("Hello", "World", Field.Store.YES) };

  @BeforeClass
  public static void createTestIndex() throws IOException {
    indexDirectory = newDirectory();
    // create and open an index writer
    indexWriter = new RandomIndexWriter(random(), indexDirectory,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));

    config = new FacetsConfig();

    indexDocuments(DOC_SINGLEVALUED, DOC_MULTIVALUED, DOC_NOFACET);

    indexReader = indexWriter.getReader();
    // prepare searcher to search against
    searcher = newSearcher(indexReader);
  }

  private static void indexDocuments(IndexableField[]... docs) throws IOException {
    for (IndexableField[] fields : docs) {
      for (IndexableField field : fields) {
        Document doc = new Document();
        doc.add(field);
        indexWriter.addDocument(config.build(doc));
      }
    }
  }

  @AfterClass
  public static void closeTestIndex() throws IOException {
    IOUtils.close(indexReader, indexWriter, indexDirectory);
    indexReader = null;
    indexWriter = null;
    indexDirectory = null;
    searcher = null;
    config = null;
  }

  @Test
  public void testSingleValued() throws Exception {
    TopDocs topDocs = searcher.search(new FacetQuery("Author", "Mark Twain"), 10);
    assertEquals(1, topDocs.totalHits);
  }

  @Test
  public void testMultiValued() throws Exception {
    TopDocs topDocs = searcher.search(
            new MultiFacetQuery("Author", new String[] { "Mark Twain" }, new String[] { "Kurt Vonnegut" }), 10);
    assertEquals(2, topDocs.totalHits);
  }
}

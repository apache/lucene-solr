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

import java.util.Arrays;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;

public class TestFacetsConfig extends FacetTestCase {
  
  public void testPathToStringAndBack() throws Exception {
    int iters = atLeast(1000);
    for(int i=0;i<iters;i++) {
      int numParts = TestUtil.nextInt(random(), 1, 6);
      String[] parts = new String[numParts];
      for(int j=0;j<numParts;j++) {
        String s;
        while (true) {
          s = TestUtil.randomUnicodeString(random());
          if (s.length() > 0) {
            break;
          }
        }
        parts[j] = s;
      }

      String s = FacetsConfig.pathToString(parts);
      String[] parts2 = FacetsConfig.stringToPath(s);
      assertTrue(Arrays.equals(parts, parts2));
    }
  }
  
  public void testAddSameDocTwice() throws Exception {
    // LUCENE-5367: this was a problem with the previous code, making sure it
    // works with the new code.
    Directory indexDir = newDirectory(), taxoDir = newDirectory();
    IndexWriter indexWriter = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig facetsConfig = new FacetsConfig();
    Document doc = new Document();
    doc.add(new FacetField("a", "b"));
    doc = facetsConfig.build(taxoWriter, doc);
    // these two addDocument() used to fail
    indexWriter.addDocument(doc);
    indexWriter.addDocument(doc);
    indexWriter.close();
    IOUtils.close(taxoWriter);
    
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(indexReader);
    FacetsCollector fc = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), fc);
    
    Facets facets = getTaxonomyFacetCounts(taxoReader, facetsConfig, fc);
    FacetResult res = facets.getTopChildren(10, "a");
    assertEquals(1, res.labelValues.length);
    assertEquals(2, res.labelValues[0].value);
    IOUtils.close(indexReader, taxoReader);
    
    IOUtils.close(indexDir, taxoDir);
  }

  /** LUCENE-5479 */
  public void testCustomDefault() {
    FacetsConfig config = new FacetsConfig() {
        @Override
        protected DimConfig getDefaultDimConfig() {
          DimConfig config = new DimConfig();
          config.hierarchical = true;
          return config;
        }
      };

    assertTrue(config.getDimConfig("foobar").hierarchical);
  }
}

package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.util.AssertingCategoryListIterator;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

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

public class TestStandardFacetsAccumulator extends LuceneTestCase {
  
  private void indexTwoDocs(IndexWriter indexWriter, FacetFields facetFields, boolean withContent) throws Exception {
    for (int i = 0; i < 2; i++) {
      Document doc = new Document();
      if (withContent) {
        doc.add(new StringField("f", "a", Store.NO));
      }
      if (facetFields != null) {
        facetFields.addFields(doc, Collections.singletonList(new CategoryPath("A", Integer.toString(i))));
      }
      indexWriter.addDocument(doc);
    }
    
    indexWriter.commit();
  }
  
  @Test
  public void testSegmentsWithoutCategoriesOrResults() throws Exception {
    // tests the accumulator when there are segments with no results
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setMergePolicy(NoMergePolicy.COMPOUND_FILES); // prevent merges
    IndexWriter indexWriter = new IndexWriter(indexDir, iwc);
    FacetIndexingParams fip = new FacetIndexingParams(new CategoryListParams() {
      @Override
      public CategoryListIterator createCategoryListIterator(int partition) throws IOException {
        return new AssertingCategoryListIterator(super.createCategoryListIterator(partition));
      }
    });
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetFields facetFields = new FacetFields(taxoWriter, fip);
    indexTwoDocs(indexWriter, facetFields, false); // 1st segment, no content, with categories
    indexTwoDocs(indexWriter, null, true);         // 2nd segment, with content, no categories
    indexTwoDocs(indexWriter, facetFields, true);  // 3rd segment ok
    indexTwoDocs(indexWriter, null, false);        // 4th segment, no content, or categories
    indexTwoDocs(indexWriter, null, true);         // 5th segment, with content, no categories
    indexTwoDocs(indexWriter, facetFields, true);  // 6th segment, with content, with categories
    IOUtils.close(indexWriter, taxoWriter);

    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher indexSearcher = new IndexSearcher(indexReader);
    
    // search for "f:a", only segments 1 and 3 should match results
    Query q = new TermQuery(new Term("f", "a"));
    ArrayList<FacetRequest> requests = new ArrayList<FacetRequest>(1);
    CountFacetRequest countNoComplements = new CountFacetRequest(new CategoryPath("A"), 10) {
      @Override
      public boolean supportsComplements() {
        return false; // disable complements
      }
    };
    requests.add(countNoComplements);
    FacetSearchParams fsp = new FacetSearchParams(requests, fip);
    FacetsCollector fc = new FacetsCollector(fsp , indexReader, taxoReader);
    indexSearcher.search(q, fc);
    List<FacetResult> results = fc.getFacetResults();
    assertEquals("received too many facet results", 1, results.size());
    FacetResultNode frn = results.get(0).getFacetResultNode();
    assertEquals("wrong weight for \"A\"", 4, (int) frn.getValue());
    assertEquals("wrong number of children", 2, frn.getNumSubResults());
    for (FacetResultNode node : frn.getSubResults()) {
      assertEquals("wrong weight for child " + node.getLabel(), 2, (int) node.getValue());
    }
    IOUtils.close(indexReader, taxoReader);
    
    IOUtils.close(indexDir, taxoDir);
  }
  
}

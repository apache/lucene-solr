package org.apache.lucene.facet.taxonomy;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.MemoryOrdinalMap;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.Before;
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

public class TestOrdinalMappingAtomicReader extends FacetTestCase {
  
  private static final int NUM_DOCS = 100;
  private final FacetsConfig facetConfig = new FacetsConfig();
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    facetConfig.setMultiValued("tag", true);
  }

  @Test
  public void testTaxonomyMergeUtils() throws Exception {
    Directory srcIndexDir = newDirectory();
    Directory srcTaxoDir = newDirectory();
    buildIndexWithFacets(srcIndexDir, srcTaxoDir, true);
    
    Directory targetIndexDir = newDirectory();
    Directory targetTaxoDir = newDirectory();
    buildIndexWithFacets(targetIndexDir, targetTaxoDir, false);
    
    IndexWriter destIndexWriter = new IndexWriter(targetIndexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    DirectoryTaxonomyWriter destTaxoWriter = new DirectoryTaxonomyWriter(targetTaxoDir);
    try {
      TaxonomyMergeUtils.merge(srcIndexDir, srcTaxoDir, new MemoryOrdinalMap(), destIndexWriter, destTaxoWriter);
    } finally {
      IOUtils.close(destIndexWriter, destTaxoWriter);
    }
    verifyResults(targetIndexDir, targetTaxoDir);
    
    IOUtils.close(targetIndexDir, targetTaxoDir, srcIndexDir, srcTaxoDir);
  }
  
  private void verifyResults(Directory indexDir, Directory taxoDir) throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(indexReader);
    
    FacetsCollector collector = new FacetsCollector();
    FacetsCollector.search(searcher, new MatchAllDocsQuery(), 10, collector);
    Facets facets = new FastTaxonomyFacetCounts(taxoReader, facetConfig, collector);
    FacetResult result = facets.getTopChildren(10, "tag");
    
    for (LabelAndValue lv: result.labelValues) {
      if (VERBOSE) {
        System.out.println(lv);
      }
      assertEquals(NUM_DOCS, lv.value.intValue());
    }
    
    IOUtils.close(indexReader, taxoReader);
  }
  
  private void buildIndexWithFacets(Directory indexDir, Directory taxoDir, boolean asc) throws IOException {
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT, null);
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexDir, config);
    
    DirectoryTaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxoDir);
    for (int i = 1; i <= NUM_DOCS; i++) {
      Document doc = new Document();
      for (int j = i; j <= NUM_DOCS; j++) {
        int facetValue = asc ? j: NUM_DOCS - j;
        doc.add(new FacetField("tag", Integer.toString(facetValue)));
      }
      writer.addDocument(facetConfig.build(taxonomyWriter, doc));
    }
    taxonomyWriter.commit();
    taxonomyWriter.close();
    writer.commit();
    writer.close();
  }

}

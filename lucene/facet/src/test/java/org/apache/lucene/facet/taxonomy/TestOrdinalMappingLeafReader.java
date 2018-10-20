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
package org.apache.lucene.facet.taxonomy;

import java.io.IOException;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.MemoryOrdinalMap;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class TestOrdinalMappingLeafReader extends FacetTestCase {
  
  private static final int NUM_DOCS = 100;
  private final FacetsConfig facetConfig = new FacetsConfig();
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    facetConfig.setMultiValued("tag", true);
    facetConfig.setIndexFieldName("tag", "$tags"); // add custom index field name
  }

  @Test
  public void testTaxonomyMergeUtils() throws Exception {
    Directory srcIndexDir = newDirectory();
    Directory srcTaxoDir = newDirectory();
    buildIndexWithFacets(srcIndexDir, srcTaxoDir, true);
    
    Directory targetIndexDir = newDirectory();
    Directory targetTaxoDir = newDirectory();
    buildIndexWithFacets(targetIndexDir, targetTaxoDir, false);
    
    IndexWriter destIndexWriter = new IndexWriter(targetIndexDir, newIndexWriterConfig(null));
    DirectoryTaxonomyWriter destTaxoWriter = new DirectoryTaxonomyWriter(targetTaxoDir);
    try {
      TaxonomyMergeUtils.merge(srcIndexDir, srcTaxoDir, new MemoryOrdinalMap(), destIndexWriter, destTaxoWriter, facetConfig);
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

    // tag facets
    Facets tagFacets = new FastTaxonomyFacetCounts("$tags", taxoReader, facetConfig, collector);
    FacetResult result = tagFacets.getTopChildren(10, "tag");
    for (LabelAndValue lv: result.labelValues) {
      if (VERBOSE) {
        System.out.println(lv);
      }
      assertEquals(NUM_DOCS, lv.value.intValue());
    }
    
    // id facets
    Facets idFacets = new FastTaxonomyFacetCounts(taxoReader, facetConfig, collector);
    FacetResult idResult = idFacets.getTopChildren(10, "id");
    assertEquals(NUM_DOCS, idResult.childCount);
    assertEquals(NUM_DOCS * 2, idResult.value); // each "id" appears twice
    
    BinaryDocValues bdv = MultiDocValues.getBinaryValues(indexReader, "bdv");
    BinaryDocValues cbdv = MultiDocValues.getBinaryValues(indexReader, "cbdv");
    for (int i = 0; i < indexReader.maxDoc(); i++) {
      assertEquals(i, bdv.nextDoc());
      assertEquals(i, cbdv.nextDoc());
      assertEquals(Integer.parseInt(cbdv.binaryValue().utf8ToString()), Integer.parseInt(bdv.binaryValue().utf8ToString())*2);
    }
    IOUtils.close(indexReader, taxoReader);
  }
  
  private void buildIndexWithFacets(Directory indexDir, Directory taxoDir, boolean asc) throws IOException {
    IndexWriterConfig config = newIndexWriterConfig(null);
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexDir, config);
    
    DirectoryTaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxoDir);
    for (int i = 1; i <= NUM_DOCS; i++) {
      Document doc = new Document();
      for (int j = i; j <= NUM_DOCS; j++) {
        int facetValue = asc ? j: NUM_DOCS - j;
        doc.add(new FacetField("tag", Integer.toString(facetValue)));
      }
      // add a facet under default dim config
      doc.add(new FacetField("id", Integer.toString(i)));
      
      // make sure OrdinalMappingLeafReader ignores non-facet BinaryDocValues fields
      doc.add(new BinaryDocValuesField("bdv", new BytesRef(Integer.toString(i))));
      doc.add(new BinaryDocValuesField("cbdv", new BytesRef(Integer.toString(i*2))));
      writer.addDocument(facetConfig.build(taxonomyWriter, doc));
    }
    taxonomyWriter.commit();
    taxonomyWriter.close();
    writer.commit();
    writer.close();
  }

}

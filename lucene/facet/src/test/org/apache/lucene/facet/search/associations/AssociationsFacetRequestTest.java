package org.apache.lucene.facet.search.associations;

import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.associations.AssociationsFacetFields;
import org.apache.lucene.facet.associations.CategoryAssociationsContainer;
import org.apache.lucene.facet.associations.CategoryFloatAssociation;
import org.apache.lucene.facet.associations.CategoryIntAssociation;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.params.associations.AssociationFloatSumFacetRequest;
import org.apache.lucene.facet.search.params.associations.AssociationIntSumFacetRequest;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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

/** Test for associations */
public class AssociationsFacetRequestTest extends LuceneTestCase {

  private static Directory dir;
  private static IndexReader reader;
  private static Directory taxoDir;
  
  private static final CategoryPath aint = new CategoryPath("int", "a");
  private static final CategoryPath bint = new CategoryPath("int", "b");
  private static final CategoryPath afloat = new CategoryPath("float", "a");
  private static final CategoryPath bfloat = new CategoryPath("float", "b");
  
  @BeforeClass
  public static void beforeClassAssociationsFacetRequestTest() throws Exception {
    dir = newDirectory();
    taxoDir = newDirectory();
    // preparations - index, taxonomy, content
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig(TEST_VERSION_CURRENT, 
        new MockAnalyzer(random(), MockTokenizer.KEYWORD, false)));
    
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    
    AssociationsFacetFields assocFacetFields = new AssociationsFacetFields(taxoWriter);
    
    // index documents, 50% have only 'b' and all have 'a'
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      CategoryAssociationsContainer associations = new CategoryAssociationsContainer();
      associations.setAssociation(aint, new CategoryIntAssociation(2));
      associations.setAssociation(afloat, new CategoryFloatAssociation(0.5f));
      if (i % 2 == 0) { // 50
        associations.setAssociation(bint, new CategoryIntAssociation(3));
        associations.setAssociation(bfloat, new CategoryFloatAssociation(0.2f));
      }
      assocFacetFields.addFields(doc, associations);
      writer.addDocument(doc);
    }
    
    taxoWriter.close();
    reader = writer.getReader();
    writer.close();
  }
  
  @AfterClass
  public static void afterClassAssociationsFacetRequestTest() throws Exception {
    reader.close();
    reader = null;
    dir.close();
    dir = null;
    taxoDir.close();
    taxoDir = null;
  }
  
  @Test
  public void testIntSumAssociation() throws Exception {
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);

    // facet requests for two facets
    FacetSearchParams fsp = new FacetSearchParams(
        new AssociationIntSumFacetRequest(aint, 10),
        new AssociationIntSumFacetRequest(bint, 10));
    
    Query q = new MatchAllDocsQuery();

    FacetsCollector fc = new FacetsCollector(fsp, reader, taxo);
    
    IndexSearcher searcher = newSearcher(reader);
    searcher.search(q, fc);
    List<FacetResult> res = fc.getFacetResults();
    
    assertNotNull("No results!",res);
    assertEquals("Wrong number of results!",2, res.size());
    assertEquals("Wrong count for category 'a'!",200, (int) res.get(0).getFacetResultNode().getValue());
    assertEquals("Wrong count for category 'b'!",150, (int) res.get(1).getFacetResultNode().getValue());
    
    taxo.close();
  }
  
  @Test
  public void testFloatSumAssociation() throws Exception {
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);

    // facet requests for two facets
    FacetSearchParams fsp = new FacetSearchParams(
        new AssociationFloatSumFacetRequest(afloat, 10),
        new AssociationFloatSumFacetRequest(bfloat, 10));
    
    Query q = new MatchAllDocsQuery();

    FacetsCollector fc = new FacetsCollector(fsp, reader, taxo);
    
    IndexSearcher searcher = newSearcher(reader);
    searcher.search(q, fc);
    List<FacetResult> res = fc.getFacetResults();
    
    assertNotNull("No results!",res);
    assertEquals("Wrong number of results!",2, res.size());
    assertEquals("Wrong count for category 'a'!",50f, (float) res.get(0).getFacetResultNode().getValue(), 0.00001);
    assertEquals("Wrong count for category 'b'!",10f, (float) res.get(1).getFacetResultNode().getValue(), 0.00001);
    
    taxo.close();
  }  
    
  @Test
  public void testDifferentAggregatorsSameCategoryList() throws Exception {
    // Same category list cannot be aggregated by two different aggregators. If
    // you want to do that, you need to separate the categories into two
    // category list (you'll still have one association list).
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);

    // facet requests for two facets
    FacetSearchParams fsp = new FacetSearchParams(
        new AssociationIntSumFacetRequest(aint, 10),
        new AssociationIntSumFacetRequest(bint, 10),
        new AssociationFloatSumFacetRequest(afloat, 10),
        new AssociationFloatSumFacetRequest(bfloat, 10));
    
    Query q = new MatchAllDocsQuery();

    FacetsCollector fc = new FacetsCollector(fsp, reader, taxo);
    
    IndexSearcher searcher = newSearcher(reader);
    searcher.search(q, fc);
    try {
      fc.getFacetResults();
      fail("different aggregators for same category list should not be supported");
    } catch (RuntimeException e) {
      // ok - expected
    }
    taxo.close();
  }  

}

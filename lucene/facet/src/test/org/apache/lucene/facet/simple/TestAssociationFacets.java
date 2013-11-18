package org.apache.lucene.facet.simple;

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

import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Test for associations */
public class TestAssociationFacets extends FacetTestCase {
  
  private static Directory dir;
  private static IndexReader reader;
  private static Directory taxoDir;
  
  private static final FacetLabel aint = new FacetLabel("int", "a");
  private static final FacetLabel bint = new FacetLabel("int", "b");
  private static final FacetLabel afloat = new FacetLabel("float", "a");
  private static final FacetLabel bfloat = new FacetLabel("float", "b");
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    taxoDir = newDirectory();
    // preparations - index, taxonomy, content
    
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);

    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new FacetIndexWriter(dir, iwc, taxoWriter, new FacetsConfig());

    // index documents, 50% have only 'b' and all have 'a'
    for (int i = 0; i < 110; i++) {
      Document doc = new Document();
      // every 11th document is added empty, this used to cause the association
      // aggregators to go into an infinite loop
      if (i % 11 != 0) {
        doc.add(new AssociationFacetField(2, "int", "a"));
        doc.add(new AssociationFacetField(0.5f, "float", "a"));
        if (i % 2 == 0) { // 50
          doc.add(new AssociationFacetField(3, "int", "b"));
          doc.add(new AssociationFacetField(0.2f, "float", "b"));
        }
      }
      writer.addDocument(doc);
    }
    
    taxoWriter.close();
    reader = DirectoryReader.open(writer, true);
    writer.close();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    reader = null;
    dir.close();
    dir = null;
    taxoDir.close();
    taxoDir = null;
  }
  
  public void testIntSumAssociation() throws Exception {
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    
    SimpleFacetsCollector fc = new SimpleFacetsCollector();
    
    IndexSearcher searcher = newSearcher(reader);
    searcher.search(new MatchAllDocsQuery(), fc);

    SumIntAssociationFacets facets = new SumIntAssociationFacets(taxoReader, new FacetsConfig(), fc);
    
    assertEquals("Wrong count for category 'a'!", 200, facets.getSpecificValue("int", "a").intValue());
    assertEquals("Wrong count for category 'b'!", 150, facets.getSpecificValue("int", "b").intValue());
    
    taxoReader.close();
  }

  public void testFloatSumAssociation() throws Exception {
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    
    SimpleFacetsCollector fc = new SimpleFacetsCollector();
    
    IndexSearcher searcher = newSearcher(reader);
    searcher.search(new MatchAllDocsQuery(), fc);
    
    SumFloatAssociationFacets facets = new SumFloatAssociationFacets(taxoReader, new FacetsConfig(), fc);
    assertEquals("Wrong count for category 'a'!", 50f, facets.getSpecificValue("float", "a").floatValue(), 0.00001);
    assertEquals("Wrong count for category 'b'!", 10f, facets.getSpecificValue("float", "b").floatValue(), 0.00001);
    
    taxoReader.close();
  }  

  /*  
  public void testDifferentAggregatorsSameCategoryList() throws Exception {
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);
    
    // facet requests for two facets
    FacetSearchParams fsp = new FacetSearchParams(
        new SumIntAssociationFacetRequest(aint, 10),
        new SumIntAssociationFacetRequest(bint, 10),
        new SumFloatAssociationFacetRequest(afloat, 10),
        new SumFloatAssociationFacetRequest(bfloat, 10));
    
    Query q = new MatchAllDocsQuery();
    
    FacetsCollector fc = FacetsCollector.create(fsp, reader, taxo);
    
    IndexSearcher searcher = newSearcher(reader);
    searcher.search(q, fc);
    List<FacetResult> res = fc.getFacetResults();
    
    assertEquals("Wrong number of results!", 4, res.size());
    assertEquals("Wrong count for category 'a'!", 200, (int) res.get(0).getFacetResultNode().value);
    assertEquals("Wrong count for category 'b'!", 150, (int) res.get(1).getFacetResultNode().value);
    assertEquals("Wrong count for category 'a'!",50f, (float) res.get(2).getFacetResultNode().value, 0.00001);
    assertEquals("Wrong count for category 'b'!",10f, (float) res.get(3).getFacetResultNode().value, 0.00001);
    
    taxo.close();
  }
  */  
}

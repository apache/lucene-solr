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
public class TestTaxonomyFacetAssociations extends FacetTestCase {
  
  private static Directory dir;
  private static IndexReader reader;
  private static Directory taxoDir;
  private static TaxonomyReader taxoReader;

  private static final FacetLabel aint = new FacetLabel("int", "a");
  private static final FacetLabel bint = new FacetLabel("int", "b");
  private static final FacetLabel afloat = new FacetLabel("float", "a");
  private static final FacetLabel bfloat = new FacetLabel("float", "b");
  private static final FacetsConfig config = new FacetsConfig();

  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    taxoDir = newDirectory();
    // preparations - index, taxonomy, content
    
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);

    // Cannot mix ints & floats in the same indexed field:
    config.setIndexFieldName("int", "$facets.int");
    config.setIndexFieldName("float", "$facets.float");

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DocumentBuilder builder = new DocumentBuilder(taxoWriter, config);

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
      writer.addDocument(builder.build(doc));
    }
    
    taxoWriter.close();
    reader = writer.getReader();
    writer.close();
    taxoReader = new DirectoryTaxonomyReader(taxoDir);
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    reader = null;
    dir.close();
    dir = null;
    taxoReader.close();
    taxoReader = null;
    taxoDir.close();
    taxoDir = null;
  }
  
  public void testIntSumAssociation() throws Exception {
    
    SimpleFacetsCollector fc = new SimpleFacetsCollector();
    
    IndexSearcher searcher = newSearcher(reader);
    searcher.search(new MatchAllDocsQuery(), fc);

    Facets facets = new TaxonomyFacetSumIntAssociations("$facets.int", taxoReader, config, fc);
    
    assertEquals("Wrong count for category 'a'!", 200, facets.getSpecificValue("int", "a").intValue());
    assertEquals("Wrong count for category 'b'!", 150, facets.getSpecificValue("int", "b").intValue());
  }

  public void testFloatSumAssociation() throws Exception {
    SimpleFacetsCollector fc = new SimpleFacetsCollector();
    
    IndexSearcher searcher = newSearcher(reader);
    searcher.search(new MatchAllDocsQuery(), fc);
    
    Facets facets = new TaxonomyFacetSumFloatAssociations("$facets.float", taxoReader, config, fc);
    assertEquals("Wrong count for category 'a'!", 50f, facets.getSpecificValue("float", "a").floatValue(), 0.00001);
    assertEquals("Wrong count for category 'b'!", 10f, facets.getSpecificValue("float", "b").floatValue(), 0.00001);
  }  

  /** Make sure we can test both int and float assocs in one
   *  index, as long as we send each to a different field. */
  public void testIntAndFloatAssocation() throws Exception {
    SimpleFacetsCollector fc = new SimpleFacetsCollector();
    
    IndexSearcher searcher = newSearcher(reader);
    searcher.search(new MatchAllDocsQuery(), fc);
    
    Facets facets = new TaxonomyFacetSumFloatAssociations("$facets.float", taxoReader, config, fc);
    assertEquals("Wrong count for category 'a'!", 50f, facets.getSpecificValue("float", "a").floatValue(), 0.00001);
    assertEquals("Wrong count for category 'b'!", 10f, facets.getSpecificValue("float", "b").floatValue(), 0.00001);
    
    facets = new TaxonomyFacetSumIntAssociations("$facets.int", taxoReader, config, fc);
    assertEquals("Wrong count for category 'a'!", 200, facets.getSpecificValue("int", "a").intValue());
    assertEquals("Wrong count for category 'b'!", 150, facets.getSpecificValue("int", "b").intValue());
  }

  public void testWrongIndexFieldName() throws Exception {
    SimpleFacetsCollector fc = new SimpleFacetsCollector();
    
    IndexSearcher searcher = newSearcher(reader);
    searcher.search(new MatchAllDocsQuery(), fc);
    Facets facets = new TaxonomyFacetSumFloatAssociations(taxoReader, config, fc);
    try {
      facets.getSpecificValue("float");
      fail("should have hit exc");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    try {
      facets.getTopChildren(10, "float");
      fail("should have hit exc");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }
}

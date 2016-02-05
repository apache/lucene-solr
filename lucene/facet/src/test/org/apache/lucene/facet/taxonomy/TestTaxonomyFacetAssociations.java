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

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Test for associations */
public class TestTaxonomyFacetAssociations extends FacetTestCase {
  
  private static Directory dir;
  private static IndexReader reader;
  private static Directory taxoDir;
  private static TaxonomyReader taxoReader;

  private static FacetsConfig config;

  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    taxoDir = newDirectory();
    // preparations - index, taxonomy, content
    
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);

    // Cannot mix ints & floats in the same indexed field:
    config = new FacetsConfig();
    config.setIndexFieldName("int", "$facets.int");
    config.setMultiValued("int", true);
    config.setIndexFieldName("float", "$facets.float");
    config.setMultiValued("float", true);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // index documents, 50% have only 'b' and all have 'a'
    for (int i = 0; i < 110; i++) {
      Document doc = new Document();
      // every 11th document is added empty, this used to cause the association
      // aggregators to go into an infinite loop
      if (i % 11 != 0) {
        doc.add(new IntAssociationFacetField(2, "int", "a"));
        doc.add(new FloatAssociationFacetField(0.5f, "float", "a"));
        if (i % 2 == 0) { // 50
          doc.add(new IntAssociationFacetField(3, "int", "b"));
          doc.add(new FloatAssociationFacetField(0.2f, "float", "b"));
        }
      }
      writer.addDocument(config.build(taxoWriter, doc));
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
    
    FacetsCollector fc = new FacetsCollector();
    
    IndexSearcher searcher = newSearcher(reader);
    searcher.search(new MatchAllDocsQuery(), fc);

    Facets facets = new TaxonomyFacetSumIntAssociations("$facets.int", taxoReader, config, fc);
    assertEquals("dim=int path=[] value=-1 childCount=2\n  a (200)\n  b (150)\n", facets.getTopChildren(10, "int").toString());
    assertEquals("Wrong count for category 'a'!", 200, facets.getSpecificValue("int", "a").intValue());
    assertEquals("Wrong count for category 'b'!", 150, facets.getSpecificValue("int", "b").intValue());
  }

  public void testFloatSumAssociation() throws Exception {
    FacetsCollector fc = new FacetsCollector();
    
    IndexSearcher searcher = newSearcher(reader);
    searcher.search(new MatchAllDocsQuery(), fc);
    
    Facets facets = new TaxonomyFacetSumFloatAssociations("$facets.float", taxoReader, config, fc);
    assertEquals("dim=float path=[] value=-1.0 childCount=2\n  a (50.0)\n  b (9.999995)\n", facets.getTopChildren(10, "float").toString());
    assertEquals("Wrong count for category 'a'!", 50f, facets.getSpecificValue("float", "a").floatValue(), 0.00001);
    assertEquals("Wrong count for category 'b'!", 10f, facets.getSpecificValue("float", "b").floatValue(), 0.00001);
  }  

  /** Make sure we can test both int and float assocs in one
   *  index, as long as we send each to a different field. */
  public void testIntAndFloatAssocation() throws Exception {
    FacetsCollector fc = new FacetsCollector();
    
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
    FacetsCollector fc = new FacetsCollector();
    
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

  public void testMixedTypesInSameIndexField() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new IntAssociationFacetField(14, "a", "x"));
    doc.add(new FloatAssociationFacetField(55.0f, "b", "y"));
    try {
      writer.addDocument(config.build(taxoWriter, doc));
      fail("did not hit expected exception");
    } catch (IllegalArgumentException exc) {
      // expected
    }
    writer.close();
    IOUtils.close(taxoWriter, dir, taxoDir);
  }

  public void testNoHierarchy() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("a", true);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new IntAssociationFacetField(14, "a", "x"));
    try {
      writer.addDocument(config.build(taxoWriter, doc));
      fail("did not hit expected exception");
    } catch (IllegalArgumentException exc) {
      // expected
    }
    writer.close();
    IOUtils.close(taxoWriter, dir, taxoDir);
  }

  public void testRequireDimCount() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    config.setRequireDimCount("a", true);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new IntAssociationFacetField(14, "a", "x"));
    try {
      writer.addDocument(config.build(taxoWriter, doc));
      fail("did not hit expected exception");
    } catch (IllegalArgumentException exc) {
      // expected
    }
    writer.close();
    IOUtils.close(taxoWriter, dir, taxoDir);
  }
  
  public void testIntSumAssociationDrillDown() throws Exception {
    FacetsCollector fc = new FacetsCollector();
    
    IndexSearcher searcher = newSearcher(reader);
    DrillDownQuery q = new DrillDownQuery(config);
    q.add("int", "b");
    searcher.search(q, fc);

    Facets facets = new TaxonomyFacetSumIntAssociations("$facets.int", taxoReader, config, fc);
    assertEquals("dim=int path=[] value=-1 childCount=2\n  b (150)\n  a (100)\n", facets.getTopChildren(10, "int").toString());
    assertEquals("Wrong count for category 'a'!", 100, facets.getSpecificValue("int", "a").intValue());
    assertEquals("Wrong count for category 'b'!", 150, facets.getSpecificValue("int", "b").intValue());
  }

}

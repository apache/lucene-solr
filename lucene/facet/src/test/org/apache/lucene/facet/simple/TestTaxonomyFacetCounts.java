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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.util.PrintTaxonomyStats;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util._TestUtil;

public class TestTaxonomyFacetCounts extends FacetTestCase {

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig(taxoWriter);
    config.setHierarchical("Publish Date", true);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    writer.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    writer.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    writer.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    writer.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    writer.addDocument(config.build(doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    // Aggregate the facet counts:
    SimpleFacetsCollector c = new SimpleFacetsCollector();

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), c);

    Facets facets = new FastTaxonomyFacetCounts(taxoReader, config, c);

    // Retrieve & verify results:
    assertEquals("Publish Date (5)\n  2010 (2)\n  2012 (2)\n  1999 (1)\n", facets.getTopChildren(10, "Publish Date").toString());
    assertEquals("Author (5)\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n", facets.getTopChildren(10, "Author").toString());

    // Now user drills down on Publish Date/2010:
    SimpleDrillDownQuery q2 = new SimpleDrillDownQuery(config);
    q2.add("Publish Date", "2010");
    c = new SimpleFacetsCollector();
    searcher.search(q2, c);
    facets = new FastTaxonomyFacetCounts(taxoReader, config, c);
    assertEquals("Author (2)\n  Bob (1)\n  Lisa (1)\n", facets.getTopChildren(10, "Author").toString());

    assertEquals(1, facets.getSpecificValue("Author", "Lisa"));

    // Smoke test PrintTaxonomyStats:
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintTaxonomyStats.printStats(taxoReader, new PrintStream(bos, false, "UTF-8"), true);
    String result = bos.toString("UTF-8");
    assertTrue(result.indexOf("/Author: 4 immediate children; 5 total categories") != -1);
    assertTrue(result.indexOf("/Publish Date: 3 immediate children; 12 total categories") != -1);
    // Make sure at least a few nodes of the tree came out:
    assertTrue(result.indexOf("  /1999") != -1);
    assertTrue(result.indexOf("  /2012") != -1);
    assertTrue(result.indexOf("      /20") != -1);

    IOUtils.close(writer, taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  // LUCENE-5333
  public void testSparseFacets() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FacetsConfig config = new FacetsConfig(taxoWriter);

    Document doc = new Document();
    doc.add(new FacetField("a", "foo1"));
    writer.addDocument(config.build(doc));

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new FacetField("a", "foo2"));
    doc.add(new FacetField("b", "bar1"));
    writer.addDocument(config.build(doc));

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new FacetField("a", "foo3"));
    doc.add(new FacetField("b", "bar2"));
    doc.add(new FacetField("c", "baz1"));
    writer.addDocument(config.build(doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    SimpleFacetsCollector c = new SimpleFacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);    

    Facets facets = getTaxonomyFacetCounts(taxoReader, new FacetsConfig(), c);

    // Ask for top 10 labels for any dims that have counts:
    List<SimpleFacetResult> results = facets.getAllDims(10);

    assertEquals(3, results.size());
    assertEquals("a (3)\n  foo1 (1)\n  foo2 (1)\n  foo3 (1)\n", results.get(0).toString());
    assertEquals("b (2)\n  bar1 (1)\n  bar2 (1)\n", results.get(1).toString());
    assertEquals("c (1)\n  baz1 (1)\n", results.get(2).toString());

    IOUtils.close(writer, taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  public void testWrongIndexFieldName() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig(taxoWriter);
    config.setIndexFieldName("a", "$facets2");
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new FacetField("a", "foo1"));
    writer.addDocument(config.build(doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    SimpleFacetsCollector c = new SimpleFacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);    

    // Uses default $facets field:
    Facets facets;
    if (random().nextBoolean()) {
      facets = new FastTaxonomyFacetCounts(taxoReader, config, c);
    } else {
      OrdinalsReader ordsReader = new DocValuesOrdinalsReader();
      if (random().nextBoolean()) {
        ordsReader = new CachedOrdinalsReader(ordsReader);
      }
      facets = new TaxonomyFacetCounts(ordsReader, taxoReader, config, c);
    }

    // Ask for top 10 labels for any dims that have counts:
    List<SimpleFacetResult> results = facets.getAllDims(10);
    assertTrue(results.isEmpty());

    try {
      facets.getSpecificValue("a");
      fail("should have hit exc");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    try {
      facets.getTopChildren(10, "a");
      fail("should have hit exc");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    IOUtils.close(writer, taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }


  // nocommit in the sparse case test that we are really
  // sorting by the correct dim count

  public void testReallyNoNormsForDrillDown() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setSimilarity(new PerFieldSimilarityWrapper() {
        final Similarity sim = new DefaultSimilarity();

        @Override
        public Similarity get(String name) {
          assertEquals("field", name);
          return sim;
        }
      });
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    FacetsConfig config = new FacetsConfig(taxoWriter);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("a", "path"));
    writer.addDocument(config.build(doc));
    IOUtils.close(writer, taxoWriter, dir, taxoDir);
  }

  public void testMultiValuedHierarchy() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    FacetsConfig config = new FacetsConfig(taxoWriter);
    config.setHierarchical("a", true);
    config.setMultiValued("a", true);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("a", "path", "x"));
    doc.add(new FacetField("a", "path", "y"));
    writer.addDocument(config.build(doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    
    // Aggregate the facet counts:
    SimpleFacetsCollector c = new SimpleFacetsCollector();

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), c);
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, c);
    SimpleFacetResult result = facets.getTopChildren(10, "a");
    assertEquals(1, result.labelValues.length);
    assertEquals(1, result.labelValues[0].value.intValue());

    IOUtils.close(writer, taxoWriter, searcher.getIndexReader(), taxoReader, dir, taxoDir);
  }

  public void testLabelWithDelimiter() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig(taxoWriter);
    config.setMultiValued("dim", true);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("dim", "test\u001Fone"));
    doc.add(new FacetField("dim", "test\u001Etwo"));
    writer.addDocument(config.build(doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    SimpleFacetsCollector c = new SimpleFacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);
    
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, c);
    assertEquals(1, facets.getSpecificValue("dim", "test\u001Fone"));
    assertEquals(1, facets.getSpecificValue("dim", "test\u001Etwo"));

    SimpleFacetResult result = facets.getTopChildren(10, "dim");
    assertEquals("dim (-1)\n  test\u001Fone (1)\n  test\u001Etwo (1)\n", result.toString());
    IOUtils.close(writer, taxoWriter, searcher.getIndexReader(), taxoReader, dir, taxoDir);
  }

  public void testRequireDimCount() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig(taxoWriter);
    config.setMultiValued("dim2", true);
    config.setMultiValued("dim3", true);
    config.setHierarchical("dim3", true);
    config.setRequireDimCount("dim", true);
    config.setRequireDimCount("dim2", true);
    config.setRequireDimCount("dim3", true);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("dim", "a"));
    doc.add(new FacetField("dim2", "a"));
    doc.add(new FacetField("dim2", "b"));
    doc.add(new FacetField("dim3", "a", "b"));
    doc.add(new FacetField("dim3", "a", "c"));
    writer.addDocument(config.build(doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    SimpleFacetsCollector c = new SimpleFacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);
    
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, c);
    assertEquals(1, facets.getTopChildren(10, "dim").value);
    assertEquals(1, facets.getTopChildren(10, "dim2").value);
    assertEquals(1, facets.getTopChildren(10, "dim3").value);
    IOUtils.close(writer, taxoWriter, searcher.getIndexReader(), taxoReader, dir, taxoDir);
  }

  // LUCENE-4583: make sure if we require > 32 KB for one
  // document, we don't hit exc when using Facet42DocValuesFormat
  public void testManyFacetsInOneDocument() throws Exception {
    assumeTrue("default Codec doesn't support huge BinaryDocValues", _TestUtil.fieldSupportsHugeBinaryDocValues(FacetsConfig.DEFAULT_INDEX_FIELD_NAME));
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig(taxoWriter);
    config.setMultiValued("dim", true);
    
    int numLabels = _TestUtil.nextInt(random(), 40000, 100000);
    
    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    for (int i = 0; i < numLabels; i++) {
      doc.add(new FacetField("dim", "" + i));
    }
    writer.addDocument(config.build(doc));
    
    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());
    
    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    
    // Aggregate the facet counts:
    SimpleFacetsCollector c = new SimpleFacetsCollector();
    
    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), c);
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, c);

    SimpleFacetResult result = facets.getTopChildren(Integer.MAX_VALUE, "dim");
    assertEquals(numLabels, result.labelValues.length);
    Set<String> allLabels = new HashSet<String>();
    for (LabelAndValue labelValue : result.labelValues) {
      allLabels.add(labelValue.label);
      assertEquals(1, labelValue.value.intValue());
    }
    assertEquals(numLabels, allLabels.size());
    
    IOUtils.close(searcher.getIndexReader(), taxoWriter, writer, taxoReader, dir, taxoDir);
  }

  // Make sure we catch when app didn't declare field as
  // hierarchical but it was:
  public void testDetectHierarchicalField() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FacetsConfig config = new FacetsConfig(taxoWriter);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("a", "path", "other"));
    try {
      config.build(doc);
      fail("did not hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    IOUtils.close(writer, taxoWriter, dir, taxoDir);
  }

  // Make sure we catch when app didn't declare field as
  // multi-valued but it was:
  public void testDetectMultiValuedField() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FacetsConfig config = new FacetsConfig(taxoWriter);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("a", "path"));
    doc.add(new FacetField("a", "path2"));
    try {
      config.build(doc);
      fail("did not hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    IOUtils.close(writer, taxoWriter, dir, taxoDir);
  }

  public void testSeparateIndexedFields() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig(taxoWriter);
    config.setIndexFieldName("b", "$b");
    
    for(int i = atLeast(30); i > 0; --i) {
      Document doc = new Document();
      doc.add(new StringField("f", "v", Field.Store.NO));
      doc.add(new FacetField("a", "1"));
      doc.add(new FacetField("b", "1"));
      iw.addDocument(config.build(doc));
    }
    
    DirectoryReader r = DirectoryReader.open(iw, true);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    
    SimpleFacetsCollector sfc = new SimpleFacetsCollector();
    newSearcher(r).search(new MatchAllDocsQuery(), sfc);
    Facets facets1 = getTaxonomyFacetCounts(taxoReader, config, sfc);
    Facets facets2 = getTaxonomyFacetCounts(taxoReader, config, sfc, "$b");
    assertEquals(r.maxDoc(), facets1.getTopChildren(10, "a").value.intValue());
    assertEquals(r.maxDoc(), facets2.getTopChildren(10, "b").value.intValue());
    IOUtils.close(taxoWriter, iw, taxoReader, taxoDir, r, indexDir);
  }
  
  public void testCountRoot() throws Exception {
    // LUCENE-4882: FacetsAccumulator threw NPE if a FacetRequest was defined on CP.EMPTY
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig(taxoWriter);
    for(int i = atLeast(30); i > 0; --i) {
      Document doc = new Document();
      doc.add(new FacetField("a", "1"));
      doc.add(new FacetField("b", "1"));
      iw.addDocument(config.build(doc));
    }
    
    DirectoryReader r = DirectoryReader.open(iw, true);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    
    SimpleFacetsCollector sfc = new SimpleFacetsCollector();
    newSearcher(r).search(new MatchAllDocsQuery(), sfc);
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, sfc);
    for (SimpleFacetResult result : facets.getAllDims(10)) {
      assertEquals(r.numDocs(), result.value.intValue());
    }
    
    IOUtils.close(taxoWriter, iw, taxoReader, taxoDir, r, indexDir);
  }

  public void testGetFacetResultsTwice() throws Exception {
    // LUCENE-4893: counts were multiplied as many times as getFacetResults was called.
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig(taxoWriter);

    Document doc = new Document();
    doc.add(new FacetField("a", "1"));
    doc.add(new FacetField("b", "1"));
    iw.addDocument(config.build(doc));
    
    DirectoryReader r = DirectoryReader.open(iw, true);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    final SimpleFacetsCollector sfc = new SimpleFacetsCollector();
    newSearcher(r).search(new MatchAllDocsQuery(), sfc);

    Facets facets = getTaxonomyFacetCounts(taxoReader, config, sfc);
    List<SimpleFacetResult> res1 = facets.getAllDims(10);
    List<SimpleFacetResult> res2 = facets.getAllDims(10);
    assertEquals("calling getFacetResults twice should return the .equals()=true result", res1, res2);
    
    IOUtils.close(taxoWriter, iw, taxoReader, taxoDir, r, indexDir);
  }
  
  public void testChildCount() throws Exception {
    // LUCENE-4885: FacetResult.numValidDescendants was not set properly by FacetsAccumulator
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig(taxoWriter);
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(new FacetField("a", Integer.toString(i)));
      iw.addDocument(config.build(doc));
    }
    
    DirectoryReader r = DirectoryReader.open(iw, true);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    
    SimpleFacetsCollector sfc = new SimpleFacetsCollector();
    newSearcher(r).search(new MatchAllDocsQuery(), sfc);
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, sfc);
    
    assertEquals(10, facets.getTopChildren(2, "a").childCount);

    IOUtils.close(taxoWriter, iw, taxoReader, taxoDir, r, indexDir);
  }
}

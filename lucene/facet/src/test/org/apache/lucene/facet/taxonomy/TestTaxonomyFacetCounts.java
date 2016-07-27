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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;

public class TestTaxonomyFacetCounts extends FacetTestCase {

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Publish Date", true);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    // Aggregate the facet counts:
    FacetsCollector c = new FacetsCollector();

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), c);

    Facets facets = new FastTaxonomyFacetCounts(taxoReader, config, c);

    // Retrieve & verify results:
    assertEquals("dim=Publish Date path=[] value=5 childCount=3\n  2010 (2)\n  2012 (2)\n  1999 (1)\n", facets.getTopChildren(10, "Publish Date").toString());
    assertEquals("dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n", facets.getTopChildren(10, "Author").toString());

    // Now user drills down on Publish Date/2010:
    DrillDownQuery q2 = new DrillDownQuery(config);
    q2.add("Publish Date", "2010");
    c = new FacetsCollector();
    searcher.search(q2, c);
    facets = new FastTaxonomyFacetCounts(taxoReader, config, c);
    assertEquals("dim=Author path=[] value=2 childCount=2\n  Bob (1)\n  Lisa (1)\n", facets.getTopChildren(10, "Author").toString());

    assertEquals(1, facets.getSpecificValue("Author", "Lisa"));

    assertNull(facets.getTopChildren(10, "Non exitent dim"));

    // Smoke test PrintTaxonomyStats:
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintTaxonomyStats.printStats(taxoReader, new PrintStream(bos, false, IOUtils.UTF_8), true);
    String result = bos.toString(IOUtils.UTF_8);
    assertTrue(result.indexOf("/Author: 4 immediate children; 5 total categories") != -1);
    assertTrue(result.indexOf("/Publish Date: 3 immediate children; 12 total categories") != -1);
    // Make sure at least a few nodes of the tree came out:
    assertTrue(result.indexOf("  /1999") != -1);
    assertTrue(result.indexOf("  /2012") != -1);
    assertTrue(result.indexOf("      /20") != -1);

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  // LUCENE-5333
  public void testSparseFacets() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FacetsConfig config = new FacetsConfig();

    Document doc = new Document();
    doc.add(new FacetField("a", "foo1"));
    writer.addDocument(config.build(taxoWriter, doc));

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new FacetField("a", "foo2"));
    doc.add(new FacetField("b", "bar1"));
    writer.addDocument(config.build(taxoWriter, doc));

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new FacetField("a", "foo3"));
    doc.add(new FacetField("b", "bar2"));
    doc.add(new FacetField("c", "baz1"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    FacetsCollector c = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);    

    Facets facets = getTaxonomyFacetCounts(taxoReader, new FacetsConfig(), c);

    // Ask for top 10 labels for any dims that have counts:
    List<FacetResult> results = facets.getAllDims(10);

    assertEquals(3, results.size());
    assertEquals("dim=a path=[] value=3 childCount=3\n  foo1 (1)\n  foo2 (1)\n  foo3 (1)\n", results.get(0).toString());
    assertEquals("dim=b path=[] value=2 childCount=2\n  bar1 (1)\n  bar2 (1)\n", results.get(1).toString());
    assertEquals("dim=c path=[] value=1 childCount=1\n  baz1 (1)\n", results.get(2).toString());

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  public void testWrongIndexFieldName() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setIndexFieldName("a", "$facets2");
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new FacetField("a", "foo1"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    FacetsCollector c = new FacetsCollector();
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
    List<FacetResult> results = facets.getAllDims(10);
    assertTrue(results.isEmpty());

    expectThrows(IllegalArgumentException.class, () -> {
      facets.getSpecificValue("a");
    });

    expectThrows(IllegalArgumentException.class, () -> {
      facets.getTopChildren(10, "a");
    });

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  public void testReallyNoNormsForDrillDown() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setSimilarity(new PerFieldSimilarityWrapper(new ClassicSimilarity()) {
        @Override
        public Similarity get(String name) {
          assertEquals("field", name);
          return defaultSim;
        }
      });
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    FacetsConfig config = new FacetsConfig();

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("a", "path"));
    writer.addDocument(config.build(taxoWriter, doc));
    writer.close();
    IOUtils.close(taxoWriter, dir, taxoDir);
  }

  public void testMultiValuedHierarchy() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("a", true);
    config.setMultiValued("a", true);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("a", "path", "x"));
    doc.add(new FacetField("a", "path", "y"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    
    // Aggregate the facet counts:
    FacetsCollector c = new FacetsCollector();

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), c);
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, c);

    expectThrows(IllegalArgumentException.class, () -> {
      facets.getSpecificValue("a");
    });

    FacetResult result = facets.getTopChildren(10, "a");
    assertEquals(1, result.labelValues.length);
    assertEquals(1, result.labelValues[0].value.intValue());

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, dir, taxoDir);
  }

  public void testLabelWithDelimiter() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setMultiValued("dim", true);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("dim", "test\u001Fone"));
    doc.add(new FacetField("dim", "test\u001Etwo"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    FacetsCollector c = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);
    
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, c);
    assertEquals(1, facets.getSpecificValue("dim", "test\u001Fone"));
    assertEquals(1, facets.getSpecificValue("dim", "test\u001Etwo"));

    FacetResult result = facets.getTopChildren(10, "dim");
    assertEquals("dim=dim path=[] value=-1 childCount=2\n  test\u001Fone (1)\n  test\u001Etwo (1)\n", result.toString());
    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, dir, taxoDir);
  }

  public void testRequireDimCount() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setRequireDimCount("dim", true);

    config.setMultiValued("dim2", true);
    config.setRequireDimCount("dim2", true);

    config.setMultiValued("dim3", true);
    config.setHierarchical("dim3", true);
    config.setRequireDimCount("dim3", true);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("dim", "a"));
    doc.add(new FacetField("dim2", "a"));
    doc.add(new FacetField("dim2", "b"));
    doc.add(new FacetField("dim3", "a", "b"));
    doc.add(new FacetField("dim3", "a", "c"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    FacetsCollector c = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);
    
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, c);
    assertEquals(1, facets.getTopChildren(10, "dim").value);
    assertEquals(1, facets.getTopChildren(10, "dim2").value);
    assertEquals(1, facets.getTopChildren(10, "dim3").value);
    expectThrows(IllegalArgumentException.class, () -> {
      facets.getSpecificValue("dim");
    });
    assertEquals(1, facets.getSpecificValue("dim2"));
    assertEquals(1, facets.getSpecificValue("dim3"));
    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, dir, taxoDir);
  }

  // LUCENE-4583: make sure if we require > 32 KB for one
  // document, we don't hit exc when using Facet42DocValuesFormat
  public void testManyFacetsInOneDocument() throws Exception {
    assumeTrue("default Codec doesn't support huge BinaryDocValues", TestUtil.fieldSupportsHugeBinaryDocValues(FacetsConfig.DEFAULT_INDEX_FIELD_NAME));
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setMultiValued("dim", true);
    
    int numLabels = TestUtil.nextInt(random(), 40000, 100000);
    
    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    for (int i = 0; i < numLabels; i++) {
      doc.add(new FacetField("dim", "" + i));
    }
    writer.addDocument(config.build(taxoWriter, doc));
    
    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());
    
    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    
    // Aggregate the facet counts:
    FacetsCollector c = new FacetsCollector();
    
    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), c);
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, c);

    FacetResult result = facets.getTopChildren(Integer.MAX_VALUE, "dim");
    assertEquals(numLabels, result.labelValues.length);
    Set<String> allLabels = new HashSet<>();
    for (LabelAndValue labelValue : result.labelValues) {
      allLabels.add(labelValue.label);
      assertEquals(1, labelValue.value.intValue());
    }
    assertEquals(numLabels, allLabels.size());

    writer.close();
    IOUtils.close(searcher.getIndexReader(), taxoWriter, taxoReader, dir, taxoDir);
  }

  // Make sure we catch when app didn't declare field as
  // hierarchical but it was:
  public void testDetectHierarchicalField() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FacetsConfig config = new FacetsConfig();

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("a", "path", "other"));
    expectThrows(IllegalArgumentException.class, () -> {
      config.build(taxoWriter, doc);
    });

    writer.close();
    IOUtils.close(taxoWriter, dir, taxoDir);
  }

  // Make sure we catch when app didn't declare field as
  // multi-valued but it was:
  public void testDetectMultiValuedField() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FacetsConfig config = new FacetsConfig();

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("a", "path"));
    doc.add(new FacetField("a", "path2"));
    expectThrows(IllegalArgumentException.class, () -> {
      config.build(taxoWriter, doc);
    });

    writer.close();
    IOUtils.close(taxoWriter, dir, taxoDir);
  }

  public void testSeparateIndexedFields() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig();
    config.setIndexFieldName("b", "$b");
    
    for(int i = atLeast(30); i > 0; --i) {
      Document doc = new Document();
      doc.add(new StringField("f", "v", Field.Store.NO));
      doc.add(new FacetField("a", "1"));
      doc.add(new FacetField("b", "1"));
      iw.addDocument(config.build(taxoWriter, doc));
    }
    
    DirectoryReader r = DirectoryReader.open(iw);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    
    FacetsCollector sfc = new FacetsCollector();
    newSearcher(r).search(new MatchAllDocsQuery(), sfc);
    Facets facets1 = getTaxonomyFacetCounts(taxoReader, config, sfc);
    Facets facets2 = getTaxonomyFacetCounts(taxoReader, config, sfc, "$b");
    assertEquals(r.maxDoc(), facets1.getTopChildren(10, "a").value.intValue());
    assertEquals(r.maxDoc(), facets2.getTopChildren(10, "b").value.intValue());
    iw.close();
    IOUtils.close(taxoWriter, taxoReader, taxoDir, r, indexDir);
  }
  
  public void testCountRoot() throws Exception {
    // LUCENE-4882: FacetsAccumulator threw NPE if a FacetRequest was defined on CP.EMPTY
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig();
    for(int i = atLeast(30); i > 0; --i) {
      Document doc = new Document();
      doc.add(new FacetField("a", "1"));
      doc.add(new FacetField("b", "1"));
      iw.addDocument(config.build(taxoWriter, doc));
    }
    
    DirectoryReader r = DirectoryReader.open(iw);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    
    FacetsCollector sfc = new FacetsCollector();
    newSearcher(r).search(new MatchAllDocsQuery(), sfc);
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, sfc);
    for (FacetResult result : facets.getAllDims(10)) {
      assertEquals(r.numDocs(), result.value.intValue());
    }

    iw.close();
    IOUtils.close(taxoWriter, taxoReader, taxoDir, r, indexDir);
  }

  public void testGetFacetResultsTwice() throws Exception {
    // LUCENE-4893: counts were multiplied as many times as getFacetResults was called.
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig();

    Document doc = new Document();
    doc.add(new FacetField("a", "1"));
    doc.add(new FacetField("b", "1"));
    iw.addDocument(config.build(taxoWriter, doc));
    
    DirectoryReader r = DirectoryReader.open(iw);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    final FacetsCollector sfc = new FacetsCollector();
    newSearcher(r).search(new MatchAllDocsQuery(), sfc);

    Facets facets = getTaxonomyFacetCounts(taxoReader, config, sfc);
    List<FacetResult> res1 = facets.getAllDims(10);
    List<FacetResult> res2 = facets.getAllDims(10);
    assertEquals("calling getFacetResults twice should return the .equals()=true result", res1, res2);

    iw.close();
    IOUtils.close(taxoWriter, taxoReader, taxoDir, r, indexDir);
  }
  
  public void testChildCount() throws Exception {
    // LUCENE-4885: FacetResult.numValidDescendants was not set properly by FacetsAccumulator
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig();
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(new FacetField("a", Integer.toString(i)));
      iw.addDocument(config.build(taxoWriter, doc));
    }
    
    DirectoryReader r = DirectoryReader.open(iw);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    
    FacetsCollector sfc = new FacetsCollector();
    newSearcher(r).search(new MatchAllDocsQuery(), sfc);
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, sfc);
    
    assertEquals(10, facets.getTopChildren(2, "a").childCount);

    iw.close();
    IOUtils.close(taxoWriter, taxoReader, taxoDir, r, indexDir);
  }

  private void indexTwoDocs(TaxonomyWriter taxoWriter, IndexWriter indexWriter, FacetsConfig config, boolean withContent) throws Exception {
    for (int i = 0; i < 2; i++) {
      Document doc = new Document();
      if (withContent) {
        doc.add(new StringField("f", "a", Field.Store.NO));
      }
      if (config != null) {
        doc.add(new FacetField("A", Integer.toString(i)));
        indexWriter.addDocument(config.build(taxoWriter, doc));
      } else {
        indexWriter.addDocument(doc);
      }
    }
    
    indexWriter.commit();
  }
  
  public void testSegmentsWithoutCategoriesOrResults() throws Exception {
    // tests the accumulator when there are segments with no results
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(NoMergePolicy.INSTANCE); // prevent merges
    IndexWriter indexWriter = new IndexWriter(indexDir, iwc);

    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    indexTwoDocs(taxoWriter, indexWriter, config, false); // 1st segment, no content, with categories
    indexTwoDocs(taxoWriter, indexWriter, null, true);         // 2nd segment, with content, no categories
    indexTwoDocs(taxoWriter, indexWriter, config, true);  // 3rd segment ok
    indexTwoDocs(taxoWriter, indexWriter, null, false);        // 4th segment, no content, or categories
    indexTwoDocs(taxoWriter, indexWriter, null, true);         // 5th segment, with content, no categories
    indexTwoDocs(taxoWriter, indexWriter, config, true);  // 6th segment, with content, with categories
    indexTwoDocs(taxoWriter, indexWriter, null, true);         // 7th segment, with content, no categories
    indexWriter.close();
    IOUtils.close(taxoWriter);

    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher indexSearcher = newSearcher(indexReader);
    
    // search for "f:a", only segments 1 and 3 should match results
    Query q = new TermQuery(new Term("f", "a"));
    FacetsCollector sfc = new FacetsCollector();
    indexSearcher.search(q, sfc);
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, sfc);
    FacetResult result = facets.getTopChildren(10, "A");
    assertEquals("wrong number of children", 2, result.labelValues.length);
    for (LabelAndValue labelValue : result.labelValues) {
      assertEquals("wrong weight for child " + labelValue.label, 2, labelValue.value.intValue());
    }

    IOUtils.close(indexReader, taxoReader, indexDir, taxoDir);
  }

  public void testRandom() throws Exception {
    String[] tokens = getRandomTokens(10);
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    RandomIndexWriter w = new RandomIndexWriter(random(), indexDir);
    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    int numDocs = atLeast(1000);
    int numDims = TestUtil.nextInt(random(), 1, 7);
    List<TestDoc> testDocs = getRandomDocs(tokens, numDocs, numDims);
    for(TestDoc testDoc : testDocs) {
      Document doc = new Document();
      doc.add(newStringField("content", testDoc.content, Field.Store.NO));
      for(int j=0;j<numDims;j++) {
        if (testDoc.dims[j] != null) {
          doc.add(new FacetField("dim" + j, testDoc.dims[j]));
        }
      }
      w.addDocument(config.build(tw, doc));
    }

    // NRT open
    IndexSearcher searcher = newSearcher(w.getReader());
    
    // NRT open
    TaxonomyReader tr = new DirectoryTaxonomyReader(tw);

    int iters = atLeast(100);
    for(int iter=0;iter<iters;iter++) {
      String searchToken = tokens[random().nextInt(tokens.length)];
      if (VERBOSE) {
        System.out.println("\nTEST: iter content=" + searchToken);
      }
      FacetsCollector fc = new FacetsCollector();
      FacetsCollector.search(searcher, new TermQuery(new Term("content", searchToken)), 10, fc);
      Facets facets = getTaxonomyFacetCounts(tr, config, fc);

      // Slow, yet hopefully bug-free, faceting:
      @SuppressWarnings({"rawtypes","unchecked"}) Map<String,Integer>[] expectedCounts = new HashMap[numDims];
      for(int i=0;i<numDims;i++) {
        expectedCounts[i] = new HashMap<>();
      }

      for(TestDoc doc : testDocs) {
        if (doc.content.equals(searchToken)) {
          for(int j=0;j<numDims;j++) {
            if (doc.dims[j] != null) {
              Integer v = expectedCounts[j].get(doc.dims[j]);
              if (v == null) {
                expectedCounts[j].put(doc.dims[j], 1);
              } else {
                expectedCounts[j].put(doc.dims[j], v.intValue() + 1);
              }
            }
          }
        }
      }

      List<FacetResult> expected = new ArrayList<>();
      for(int i=0;i<numDims;i++) {
        List<LabelAndValue> labelValues = new ArrayList<>();
        int totCount = 0;
        for(Map.Entry<String,Integer> ent : expectedCounts[i].entrySet()) {
          labelValues.add(new LabelAndValue(ent.getKey(), ent.getValue()));
          totCount += ent.getValue();
        }
        sortLabelValues(labelValues);
        if (totCount > 0) {
          expected.add(new FacetResult("dim" + i, new String[0], totCount, labelValues.toArray(new LabelAndValue[labelValues.size()]), labelValues.size()));
        }
      }

      // Sort by highest value, tie break by value:
      sortFacetResults(expected);

      List<FacetResult> actual = facets.getAllDims(10);

      // Messy: fixup ties
      sortTies(actual);

      assertEquals(expected, actual);
    }

    w.close();
    IOUtils.close(tw, searcher.getIndexReader(), tr, indexDir, taxoDir);
  }
}

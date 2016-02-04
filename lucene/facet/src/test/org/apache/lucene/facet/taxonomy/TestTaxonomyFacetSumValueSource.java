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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;

public class TestTaxonomyFacetSumValueSource extends FacetTestCase {

  public void testBasic() throws Exception {

    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FacetsConfig config = new FacetsConfig();

    // Reused across documents, to add the necessary facet
    // fields:
    Document doc = new Document();
    doc.add(new NumericDocValuesField("num", 10));
    doc.add(new FacetField("Author", "Bob"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new NumericDocValuesField("num", 20));
    doc.add(new FacetField("Author", "Lisa"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new NumericDocValuesField("num", 30));
    doc.add(new FacetField("Author", "Lisa"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new NumericDocValuesField("num", 40));
    doc.add(new FacetField("Author", "Susan"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new NumericDocValuesField("num", 45));
    doc.add(new FacetField("Author", "Frank"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    taxoWriter.close();

    // Aggregate the facet counts:
    FacetsCollector c = new FacetsCollector();

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query and one of the
    // Facets.search utility methods:
    searcher.search(new MatchAllDocsQuery(), c);

    TaxonomyFacetSumValueSource facets = new TaxonomyFacetSumValueSource(taxoReader, new FacetsConfig(), c, new IntFieldSource("num"));

    // Retrieve & verify results:
    assertEquals("dim=Author path=[] value=145.0 childCount=4\n  Lisa (50.0)\n  Frank (45.0)\n  Susan (40.0)\n  Bob (10.0)\n", facets.getTopChildren(10, "Author").toString());

    taxoReader.close();
    searcher.getIndexReader().close();
    dir.close();
    taxoDir.close();
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
    doc.add(new NumericDocValuesField("num", 10));
    doc.add(new FacetField("a", "foo1"));
    writer.addDocument(config.build(taxoWriter, doc));

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new NumericDocValuesField("num", 20));
    doc.add(new FacetField("a", "foo2"));
    doc.add(new FacetField("b", "bar1"));
    writer.addDocument(config.build(taxoWriter, doc));

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new NumericDocValuesField("num", 30));
    doc.add(new FacetField("a", "foo3"));
    doc.add(new FacetField("b", "bar2"));
    doc.add(new FacetField("c", "baz1"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    taxoWriter.close();

    FacetsCollector c = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);    

    TaxonomyFacetSumValueSource facets = new TaxonomyFacetSumValueSource(taxoReader, new FacetsConfig(), c, new IntFieldSource("num"));

    // Ask for top 10 labels for any dims that have counts:
    List<FacetResult> results = facets.getAllDims(10);

    assertEquals(3, results.size());
    assertEquals("dim=a path=[] value=60.0 childCount=3\n  foo3 (30.0)\n  foo2 (20.0)\n  foo1 (10.0)\n", results.get(0).toString());
    assertEquals("dim=b path=[] value=50.0 childCount=2\n  bar2 (30.0)\n  bar1 (20.0)\n", results.get(1).toString());
    assertEquals("dim=c path=[] value=30.0 childCount=1\n  baz1 (30.0)\n", results.get(2).toString());

    IOUtils.close(searcher.getIndexReader(), taxoReader, dir, taxoDir);
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
    doc.add(new NumericDocValuesField("num", 10));
    doc.add(new FacetField("a", "foo1"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    taxoWriter.close();

    FacetsCollector c = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);    

    TaxonomyFacetSumValueSource facets = new TaxonomyFacetSumValueSource(taxoReader, config, c, new IntFieldSource("num"));

    // Ask for top 10 labels for any dims that have counts:
    List<FacetResult> results = facets.getAllDims(10);
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

    IOUtils.close(searcher.getIndexReader(), taxoReader, dir, taxoDir);
  }

  public void testSumScoreAggregator() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));

    FacetsConfig config = new FacetsConfig();

    for(int i = atLeast(30); i > 0; --i) {
      Document doc = new Document();
      if (random().nextBoolean()) { // don't match all documents
        doc.add(new StringField("f", "v", Field.Store.NO));
      }
      doc.add(new FacetField("dim", "a"));
      iw.addDocument(config.build(taxoWriter, doc));
    }
    
    DirectoryReader r = DirectoryReader.open(iw);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    
    FacetsCollector fc = new FacetsCollector(true);
    BoostQuery csq = new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 2f);
    
    TopDocs td = FacetsCollector.search(newSearcher(r), csq, 10, fc);

    Facets facets = new TaxonomyFacetSumValueSource(taxoReader, config, fc, new TaxonomyFacetSumValueSource.ScoreValueSource());
    
    int expected = (int) (td.getMaxScore() * td.totalHits);
    assertEquals(expected, facets.getSpecificValue("dim", "a").intValue());

    iw.close();
    IOUtils.close(taxoWriter, taxoReader, taxoDir, r, indexDir);
  }
  
  public void testNoScore() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig();
    for (int i = 0; i < 4; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("price", (i+1)));
      doc.add(new FacetField("a", Integer.toString(i % 2)));
      iw.addDocument(config.build(taxoWriter, doc));
    }
    
    DirectoryReader r = DirectoryReader.open(iw);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    FacetsCollector sfc = new FacetsCollector();
    newSearcher(r).search(new MatchAllDocsQuery(), sfc);
    Facets facets = new TaxonomyFacetSumValueSource(taxoReader, config, sfc, new LongFieldSource("price"));
    assertEquals("dim=a path=[] value=10.0 childCount=2\n  1 (6.0)\n  0 (4.0)\n", facets.getTopChildren(10, "a").toString());

    iw.close();
    IOUtils.close(taxoWriter, taxoReader, taxoDir, r, indexDir);
  }

  public void testWithScore() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));

    FacetsConfig config = new FacetsConfig();
    for (int i = 0; i < 4; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("price", (i+1)));
      doc.add(new FacetField("a", Integer.toString(i % 2)));
      iw.addDocument(config.build(taxoWriter, doc));
    }
    
    DirectoryReader r = DirectoryReader.open(iw);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    ValueSource valueSource = new ValueSource() {
      @Override
      public FunctionValues getValues(@SuppressWarnings("rawtypes") Map context, LeafReaderContext readerContext) throws IOException {
        final Scorer scorer = (Scorer) context.get("scorer");
        assert scorer != null;
        return new DoubleDocValues(this) {
          @Override
          public double doubleVal(int document) {
            try {
              return scorer.score();
            } catch (IOException exception) {
              throw new RuntimeException(exception);
            }
          }
        };
      }

      @Override public boolean equals(Object o) { return o == this; }
      @Override public int hashCode() { return System.identityHashCode(this); }
      @Override public String description() { return "score()"; }
    };
    
    FacetsCollector fc = new FacetsCollector(true);
    // score documents by their 'price' field - makes asserting the correct counts for the categories easier
    Query q = new FunctionQuery(new LongFieldSource("price"));
    FacetsCollector.search(newSearcher(r), q, 10, fc);
    Facets facets = new TaxonomyFacetSumValueSource(taxoReader, config, fc, valueSource);
    
    assertEquals("dim=a path=[] value=10.0 childCount=2\n  1 (6.0)\n  0 (4.0)\n", facets.getTopChildren(10, "a").toString());

    iw.close();
    IOUtils.close(taxoWriter, taxoReader, taxoDir, r, indexDir);
  }

  public void testRollupValues() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("a", true);
    //config.setRequireDimCount("a", true);
    
    for (int i = 0; i < 4; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("price", (i+1)));
      doc.add(new FacetField("a", Integer.toString(i % 2), "1"));
      iw.addDocument(config.build(taxoWriter, doc));
    }
    
    DirectoryReader r = DirectoryReader.open(iw);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    ValueSource valueSource = new LongFieldSource("price");
    FacetsCollector sfc = new FacetsCollector();
    newSearcher(r).search(new MatchAllDocsQuery(), sfc);
    Facets facets = new TaxonomyFacetSumValueSource(taxoReader, config, sfc, valueSource);
    
    assertEquals("dim=a path=[] value=10.0 childCount=2\n  1 (6.0)\n  0 (4.0)\n", facets.getTopChildren(10, "a").toString());

    iw.close();
    IOUtils.close(taxoWriter, taxoReader, taxoDir, r, indexDir);
  }

  public void testCountAndSumScore() throws Exception {
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
    
    FacetsCollector fc = new FacetsCollector(true);
    FacetsCollector.search(newSearcher(r), new MatchAllDocsQuery(), 10, fc);
    
    Facets facets1 = getTaxonomyFacetCounts(taxoReader, config, fc);
    Facets facets2 = new TaxonomyFacetSumValueSource(new DocValuesOrdinalsReader("$b"), taxoReader, config, fc, new TaxonomyFacetSumValueSource.ScoreValueSource());

    assertEquals(r.maxDoc(), facets1.getTopChildren(10, "a").value.intValue());
    assertEquals(r.maxDoc(), facets2.getTopChildren(10, "b").value.doubleValue(), 1E-10);
    iw.close();
    IOUtils.close(taxoWriter, taxoReader, taxoDir, r, indexDir);
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
      testDoc.value = random().nextFloat();
      doc.add(new FloatDocValuesField("value", testDoc.value));
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

    ValueSource values = new FloatFieldSource("value");

    int iters = atLeast(100);
    for(int iter=0;iter<iters;iter++) {
      String searchToken = tokens[random().nextInt(tokens.length)];
      if (VERBOSE) {
        System.out.println("\nTEST: iter content=" + searchToken);
      }
      FacetsCollector fc = new FacetsCollector();
      FacetsCollector.search(searcher, new TermQuery(new Term("content", searchToken)), 10, fc);
      Facets facets = new TaxonomyFacetSumValueSource(tr, config, fc, values);

      // Slow, yet hopefully bug-free, faceting:
      @SuppressWarnings({"rawtypes","unchecked"}) Map<String,Float>[] expectedValues = new HashMap[numDims];
      for(int i=0;i<numDims;i++) {
        expectedValues[i] = new HashMap<>();
      }

      for(TestDoc doc : testDocs) {
        if (doc.content.equals(searchToken)) {
          for(int j=0;j<numDims;j++) {
            if (doc.dims[j] != null) {
              Float v = expectedValues[j].get(doc.dims[j]);
              if (v == null) {
                expectedValues[j].put(doc.dims[j], doc.value);
              } else {
                expectedValues[j].put(doc.dims[j], v.floatValue() + doc.value);
              }
            }
          }
        }
      }

      List<FacetResult> expected = new ArrayList<>();
      for(int i=0;i<numDims;i++) {
        List<LabelAndValue> labelValues = new ArrayList<>();
        double totValue = 0;
        for(Map.Entry<String,Float> ent : expectedValues[i].entrySet()) {
          labelValues.add(new LabelAndValue(ent.getKey(), ent.getValue()));
          totValue += ent.getValue();
        }
        sortLabelValues(labelValues);
        if (totValue > 0) {
          expected.add(new FacetResult("dim" + i, new String[0], totValue, labelValues.toArray(new LabelAndValue[labelValues.size()]), labelValues.size()));
        }
      }

      // Sort by highest value, tie break by value:
      sortFacetResults(expected);

      List<FacetResult> actual = facets.getAllDims(10);

      // Messy: fixup ties
      sortTies(actual);

      if (VERBOSE) {
        System.out.println("expected=\n" + expected.toString());
        System.out.println("actual=\n" + actual.toString());
      }

      assertFloatValuesEquals(expected, actual);
    }

    w.close();
    IOUtils.close(tw, searcher.getIndexReader(), tr, indexDir, taxoDir);
  }
}

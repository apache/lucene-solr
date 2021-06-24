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
package org.apache.lucene.facet.range;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.MultiFacets;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

public class TestRangeFacetCounts extends FacetTestCase {

  public void testBasicLong() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    NumericDocValuesField field = new NumericDocValuesField("field", 0L);
    doc.add(field);
    for(long l=0;l<100;l++) {
      field.setLongValue(l);
      w.addDocument(doc);
    }

    // Also add Long.MAX_VALUE
    field.setLongValue(Long.MAX_VALUE);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    FacetsCollector fc = new FacetsCollector();
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    Facets facets = new LongRangeFacetCounts("field", fc,
        new LongRange("less than 10", 0L, true, 10L, false),
        new LongRange("less than or equal to 10", 0L, true, 10L, true),
        new LongRange("over 90", 90L, false, 100L, false),
        new LongRange("90 or above", 90L, true, 100L, false),
        new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, true));

    FacetResult result = facets.getTopChildren(10, "field");
    assertEquals("dim=field path=[] value=22 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (1)\n",
                 result.toString());

    r.close();
    d.close();
  }

  public void testBasicLongMultiValued() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    // just index the same value twice each time and make sure we don't double count
    SortedNumericDocValuesField field1 = new SortedNumericDocValuesField("field", 0L);
    SortedNumericDocValuesField field2 = new SortedNumericDocValuesField("field", 0L);
    doc.add(field1);
    doc.add(field2);
    for (long l = 0; l < 100; l++) {
      field1.setLongValue(l);
      field2.setLongValue(l);
      w.addDocument(doc);
    }

    // Also add Long.MAX_VALUE
    field1.setLongValue(Long.MAX_VALUE);
    field2.setLongValue(Long.MAX_VALUE);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    FacetsCollector fc = new FacetsCollector();
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    Facets facets =
        new LongRangeFacetCounts(
            "field",
            fc,
            new LongRange("less than 10", 0L, true, 10L, false),
            new LongRange("less than or equal to 10", 0L, true, 10L, true),
            new LongRange("over 90", 90L, false, 100L, false),
            new LongRange("90 or above", 90L, true, 100L, false),
            new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, true));

    FacetResult result = facets.getTopChildren(10, "field");
    assertEquals(
        "dim=field path=[] value=22 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (1)\n",
        result.toString());

    r.close();
    d.close();
  }

  public void testBasicLongMultiValuedMixedSegmentTypes() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    SortedNumericDocValuesField field1 = new SortedNumericDocValuesField("field", 0L);
    SortedNumericDocValuesField field2 = new SortedNumericDocValuesField("field", 0L);
    // write docs as two segments (50 in each). the first segment will contain a mix of single- and
    // multi-value cases, while the second segment will be all single values.
    for (int l = 0; l < 100; l++) {
      field1.setLongValue(l);
      field2.setLongValue(l);
      Document doc = new Document();
      doc.add(field1);
      if (l == 0) {
        doc.add(field2);
      } else if (l < 50) {
        if (random().nextBoolean()) {
          doc.add(field2);
        }
      }
      w.addDocument(doc);
      if (l == 50) {
        w.commit();
      }
    }

    IndexReader r = w.getReader();
    w.close();

    FacetsCollector fc = new FacetsCollector();
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    Facets facets =
        new LongRangeFacetCounts(
            "field",
            fc,
            new LongRange("less than 10", 0L, true, 10L, false),
            new LongRange("less than or equal to 10", 0L, true, 10L, true),
            new LongRange("over 90", 90L, false, 100L, false),
            new LongRange("90 or above", 90L, true, 100L, false),
            new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, true));

    FacetResult result = facets.getTopChildren(10, "field");
    assertEquals(
        "dim=field path=[] value=21 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (0)\n",
                 result.toString());
    
    r.close();
    d.close();
  }

  public void testLongGetAllDims() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    NumericDocValuesField field = new NumericDocValuesField("field", 0L);
    doc.add(field);
    for(long l=0;l<100;l++) {
      field.setLongValue(l);
      w.addDocument(doc);
    }

    // Also add Long.MAX_VALUE
    field.setLongValue(Long.MAX_VALUE);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    FacetsCollector fc = new FacetsCollector();
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    Facets facets = new LongRangeFacetCounts("field", fc,
        new LongRange("less than 10", 0L, true, 10L, false),
        new LongRange("less than or equal to 10", 0L, true, 10L, true),
        new LongRange("over 90", 90L, false, 100L, false),
        new LongRange("90 or above", 90L, true, 100L, false),
        new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, true));

    List<FacetResult> result = facets.getAllDims(10);
    assertEquals(1, result.size());
    assertEquals("dim=field path=[] value=22 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (1)\n",
                 result.get(0).toString());
    
    r.close();
    d.close();
  }

  public void testUselessRange() {
    expectThrows(IllegalArgumentException.class, () -> {
      new LongRange("useless", 7, true, 6, true);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      new LongRange("useless", 7, true, 7, false);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      new DoubleRange("useless", 7.0, true, 6.0, true);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      new DoubleRange("useless", 7.0, true, 7.0, false);
    });
  }

  public void testLongMinMax() throws Exception {

    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    NumericDocValuesField field = new NumericDocValuesField("field", 0L);
    doc.add(field);
    field.setLongValue(Long.MIN_VALUE);
    w.addDocument(doc);
    field.setLongValue(0);
    w.addDocument(doc);
    field.setLongValue(Long.MAX_VALUE);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    FacetsCollector fc = new FacetsCollector();
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    Facets facets = new LongRangeFacetCounts("field", fc,
        new LongRange("min", Long.MIN_VALUE, true, Long.MIN_VALUE, true),
        new LongRange("max", Long.MAX_VALUE, true, Long.MAX_VALUE, true),
        new LongRange("all0", Long.MIN_VALUE, true, Long.MAX_VALUE, true),
        new LongRange("all1", Long.MIN_VALUE, false, Long.MAX_VALUE, true),
        new LongRange("all2", Long.MIN_VALUE, true, Long.MAX_VALUE, false),
        new LongRange("all3", Long.MIN_VALUE, false, Long.MAX_VALUE, false));

    FacetResult result = facets.getTopChildren(10, "field");
    assertEquals("dim=field path=[] value=3 childCount=6\n  min (1)\n  max (1)\n  all0 (3)\n  all1 (2)\n  all2 (2)\n  all3 (1)\n",
                 result.toString());
    
    r.close();
    d.close();
  }

  public void testOverlappedEndStart() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    NumericDocValuesField field = new NumericDocValuesField("field", 0L);
    doc.add(field);
    for(long l=0;l<100;l++) {
      field.setLongValue(l);
      w.addDocument(doc);
    }
    field.setLongValue(Long.MAX_VALUE);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    FacetsCollector fc = new FacetsCollector();
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    Facets facets = new LongRangeFacetCounts("field", fc,
        new LongRange("0-10", 0L, true, 10L, true),
        new LongRange("10-20", 10L, true, 20L, true),
        new LongRange("20-30", 20L, true, 30L, true),
        new LongRange("30-40", 30L, true, 40L, true));
    
    FacetResult result = facets.getTopChildren(10, "field");
    assertEquals("dim=field path=[] value=41 childCount=4\n  0-10 (11)\n  10-20 (11)\n  20-30 (11)\n  30-40 (11)\n",
                 result.toString());
    
    r.close();
    d.close();
  }

  public void testEmptyRangesSingleValued() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    NumericDocValuesField field = new NumericDocValuesField("field", 0L);
    doc.add(field);
    for (long l = 0; l < 100; l++) {
      field.setLongValue(l);
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.close();

    FacetsCollector fc = new FacetsCollector();
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    Facets facets = new LongRangeFacetCounts("field", fc);

    FacetResult result = facets.getTopChildren(10, "field");
    assertEquals("dim=field path=[] value=0 childCount=0\n", result.toString());

    r.close();
    d.close();
  }

  public void testEmptyRangesMultiValued() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    SortedNumericDocValuesField field1 = new SortedNumericDocValuesField("field", 0L);
    SortedNumericDocValuesField field2 = new SortedNumericDocValuesField("field", 0L);
    doc.add(field1);
    doc.add(field2);
    for (long l = 0; l < 100; l++) {
      field1.setLongValue(l);
      field2.setLongValue(l);
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.close();

    FacetsCollector fc = new FacetsCollector();
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    Facets facets = new LongRangeFacetCounts("field", fc);

    FacetResult result = facets.getTopChildren(10, "field");
    assertEquals("dim=field path=[] value=0 childCount=0\n", result.toString());

    r.close();
    d.close();
  }

  /** Tests single request that mixes Range and non-Range
   *  faceting, with DrillSideways and taxonomy. */
  public void testMixedRangeAndNonRangeTaxonomy() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Directory td = newDirectory();
    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(td, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();

    for (long l = 0; l < 100; l++) {
      Document doc = new Document();
      // For computing range facet counts:
      doc.add(new NumericDocValuesField("field", l));
      // For drill down by numeric range:
      doc.add(new LongPoint("field", l));

      if ((l&3) == 0) {
        doc.add(new FacetField("dim", "a"));
      } else {
        doc.add(new FacetField("dim", "b"));
      }
      w.addDocument(config.build(tw, doc));
    }

    final IndexReader r = w.getReader();

    final TaxonomyReader tr = new DirectoryTaxonomyReader(tw);

    IndexSearcher s = newSearcher(r, false, false);

    if (VERBOSE) {
      System.out.println("TEST: searcher=" + s);
    }

    DrillSideways ds = new DrillSideways(s, config, tr) {

        @Override
        protected Facets buildFacetsResult(FacetsCollector drillDowns, FacetsCollector[] drillSideways, String[] drillSidewaysDims) throws IOException {        
          FacetsCollector dimFC = drillDowns;
          FacetsCollector fieldFC = drillDowns;
          if (drillSideways != null) {
            for(int i=0;i<drillSideways.length;i++) {
              String dim = drillSidewaysDims[i];
              if (dim.equals("field")) {
                fieldFC = drillSideways[i];
              } else {
                dimFC = drillSideways[i];
              }
            }
          }

          Map<String,Facets> byDim = new HashMap<>();
          byDim.put("field",
                    new LongRangeFacetCounts("field", fieldFC,
                          new LongRange("less than 10", 0L, true, 10L, false),
                          new LongRange("less than or equal to 10", 0L, true, 10L, true),
                          new LongRange("over 90", 90L, false, 100L, false),
                          new LongRange("90 or above", 90L, true, 100L, false),
                          new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, false)));
          byDim.put("dim", getTaxonomyFacetCounts(taxoReader, config, dimFC));
          return new MultiFacets(byDim, null);
        }

        @Override
        protected boolean scoreSubDocsAtOnce() {
          return random().nextBoolean();
        }
      };

    // First search, no drill downs:
    DrillDownQuery ddq = new DrillDownQuery(config);
    DrillSidewaysResult dsr = ds.search(null, ddq, 10);

    assertEquals(100, dsr.hits.totalHits.value);
    assertEquals("dim=dim path=[] value=100 childCount=2\n  b (75)\n  a (25)\n", dsr.facets.getTopChildren(10, "dim").toString());
    assertEquals("dim=field path=[] value=21 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (0)\n",
                 dsr.facets.getTopChildren(10, "field").toString());

    // Second search, drill down on dim=b:
    ddq = new DrillDownQuery(config);
    ddq.add("dim", "b");
    dsr = ds.search(null, ddq, 10);

    assertEquals(75, dsr.hits.totalHits.value);
    assertEquals("dim=dim path=[] value=100 childCount=2\n  b (75)\n  a (25)\n", dsr.facets.getTopChildren(10, "dim").toString());
    assertEquals("dim=field path=[] value=16 childCount=5\n  less than 10 (7)\n  less than or equal to 10 (8)\n  over 90 (7)\n  90 or above (8)\n  over 1000 (0)\n",
                 dsr.facets.getTopChildren(10, "field").toString());

    // Third search, drill down on "less than or equal to 10":
    ddq = new DrillDownQuery(config);
    ddq.add("field", LongPoint.newRangeQuery("field", 0L, 10L));
    dsr = ds.search(null, ddq, 10);

    assertEquals(11, dsr.hits.totalHits.value);
    assertEquals("dim=dim path=[] value=11 childCount=2\n  b (8)\n  a (3)\n", dsr.facets.getTopChildren(10, "dim").toString());
    assertEquals("dim=field path=[] value=21 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (0)\n",
                 dsr.facets.getTopChildren(10, "field").toString());
    w.close();
    IOUtils.close(tw, tr, td, r, d);
  }

  public void testBasicDouble() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    DoubleDocValuesField field = new DoubleDocValuesField("field", 0.0);
    doc.add(field);
    for(long l=0;l<100;l++) {
      field.setDoubleValue(l);
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();

    FacetsCollector fc = new FacetsCollector();

    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);
    Facets facets = new DoubleRangeFacetCounts("field", fc,
        new DoubleRange("less than 10", 0.0, true, 10.0, false),
        new DoubleRange("less than or equal to 10", 0.0, true, 10.0, true),
        new DoubleRange("over 90", 90.0, false, 100.0, false),
        new DoubleRange("90 or above", 90.0, true, 100.0, false),
        new DoubleRange("over 1000", 1000.0, false, Double.POSITIVE_INFINITY, false));
                                         
    assertEquals("dim=field path=[] value=21 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (0)\n",
                 facets.getTopChildren(10, "field").toString());
    w.close();
    IOUtils.close(r, d);
  }

  public void testBasicDoubleMultiValued() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    // index the same value twice and make sure we don't double count
    SortedNumericDocValuesField field1 = new SortedNumericDocValuesField("field", 0);
    SortedNumericDocValuesField field2 = new SortedNumericDocValuesField("field", 0);
    doc.add(field1);
    doc.add(field2);
    for (long l = 0; l < 100; l++) {
      field1.setLongValue(NumericUtils.doubleToSortableLong(l));
      field2.setLongValue(NumericUtils.doubleToSortableLong(l));
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();

    FacetsCollector fc = new FacetsCollector();

    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);
    Facets facets =
            new DoubleRangeFacetCounts(
                    "field",
                    fc,
                    new DoubleRange("less than 10", 0.0, true, 10.0, false),
                    new DoubleRange("less than or equal to 10", 0.0, true, 10.0, true),
                    new DoubleRange("over 90", 90.0, false, 100.0, false),
                    new DoubleRange("90 or above", 90.0, true, 100.0, false),
                    new DoubleRange("over 1000", 1000.0, false, Double.POSITIVE_INFINITY, false));

    assertEquals(
            "dim=field path=[] value=21 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (0)\n",
            facets.getTopChildren(10, "field").toString());
    w.close();
    IOUtils.close(r, d);
  }

  public void testBasicDoubleMultiValuedMixedSegmentTypes() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    SortedNumericDocValuesField field1 = new SortedNumericDocValuesField("field", 0L);
    SortedNumericDocValuesField field2 = new SortedNumericDocValuesField("field", 0L);
    // write docs as two segments (50 in each). the first segment will contain a mix of single- and
    // multi-value cases, while the second segment will be all single values.
    for (int l = 0; l < 100; l++) {
      field1.setLongValue(NumericUtils.doubleToSortableLong(l));
      field2.setLongValue(NumericUtils.doubleToSortableLong(l));
      Document doc = new Document();
      doc.add(field1);
      if (l == 0) {
        doc.add(field2);
      } else if (l < 50) {
        if (random().nextBoolean()) {
          doc.add(field2);
        }
      }
      w.addDocument(doc);
      if (l == 50) {
        w.commit();
      }
    }

    IndexReader r = w.getReader();
    w.close();

    FacetsCollector fc = new FacetsCollector();
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    Facets facets =
            new DoubleRangeFacetCounts(
                    "field",
                    fc,
                    new DoubleRange("less than 10", 0.0, true, 10.0, false),
                    new DoubleRange("less than or equal to 10", 0.0, true, 10.0, true),
                    new DoubleRange("over 90", 90.0, false, 100.0, false),
                    new DoubleRange("90 or above", 90.0, true, 100.0, false),
                    new DoubleRange("over 1000", 1000.0, false, Double.POSITIVE_INFINITY, false));

    FacetResult result = facets.getTopChildren(10, "field");
    assertEquals(
            "dim=field path=[] value=21 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (0)\n",
            result.toString());

    r.close();
    d.close();
  }

  public void testRandomLongsSingleValued() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int numDocs = atLeast(1000);
    if (VERBOSE) {
      System.out.println("TEST: numDocs=" + numDocs);
    }
    long[] values = new long[numDocs];
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      long v = random().nextLong();
      values[i] = v;
      doc.add(new NumericDocValuesField("field", v));
      doc.add(new LongPoint("field", v));
      w.addDocument(doc);
      minValue = Math.min(minValue, v);
      maxValue = Math.max(maxValue, v);
    }
    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r, false);
    FacetsConfig config = new FacetsConfig();
    
    int numIters = atLeast(10);
    for(int iter=0;iter<numIters;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      int numRange = TestUtil.nextInt(random(), 1, 100);
      LongRange[] ranges = new LongRange[numRange];
      int[] expectedCounts = new int[numRange];
      long minAcceptedValue = Long.MAX_VALUE;
      long maxAcceptedValue = Long.MIN_VALUE;
      for(int rangeID=0;rangeID<numRange;rangeID++) {
        long min;
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          LongRange prevRange = ranges[random().nextInt(rangeID)];
          if (random().nextBoolean()) {
            min = prevRange.min;
          } else {
            min = prevRange.max;
          }
        } else {
          min = random().nextLong();
        }
        long max;
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          LongRange prevRange = ranges[random().nextInt(rangeID)];
          if (random().nextBoolean()) {
            max = prevRange.min;
          } else {
            max = prevRange.max;
          }
        } else {
          max = random().nextLong();
        }

        if (min > max) {
          long x = min;
          min = max;
          max = x;
        }
        boolean minIncl;
        boolean maxIncl;

        // NOTE: max - min >= 0 is here to handle the common overflow case!
        if (max - min >= 0 && max - min < 2) {
          // If max == min or max == min+1, we always do inclusive, else we might pass an empty range and hit exc from LongRange's ctor:
          minIncl = true;
          maxIncl = true;
        } else {
          minIncl = random().nextBoolean();
          maxIncl = random().nextBoolean();
        }
        ranges[rangeID] = new LongRange("r" + rangeID, min, minIncl, max, maxIncl);
        if (VERBOSE) {
          System.out.println("  range " + rangeID + ": " + ranges[rangeID]);      
        }

        // Do "slow but hopefully correct" computation of
        // expected count:
        for(int i=0;i<numDocs;i++) {
          boolean accept = true;
          if (minIncl) {
            accept &= values[i] >= min;
          } else {
            accept &= values[i] > min;
          }
          if (maxIncl) {
            accept &= values[i] <= max;
          } else {
            accept &= values[i] < max;
          }
          if (accept) {
            expectedCounts[rangeID]++;
            minAcceptedValue = Math.min(minAcceptedValue, values[i]);
            maxAcceptedValue = Math.max(maxAcceptedValue, values[i]);
          }
        }
      }

      FacetsCollector sfc = new FacetsCollector();
      s.search(new MatchAllDocsQuery(), sfc);
      Query fastMatchQuery;
      if (random().nextBoolean()) {
        if (random().nextBoolean()) {
          fastMatchQuery = LongPoint.newRangeQuery("field", minValue, maxValue);
        } else {
          fastMatchQuery = LongPoint.newRangeQuery("field", minAcceptedValue, maxAcceptedValue);
        }
      } else {
        fastMatchQuery = null;
      }
      LongValuesSource vs = LongValuesSource.fromLongField("field");
      Facets facets = new LongRangeFacetCounts("field", vs, sfc, fastMatchQuery, ranges);
      FacetResult result = facets.getTopChildren(10, "field");
      assertEquals(numRange, result.labelValues.length);
      for(int rangeID=0;rangeID<numRange;rangeID++) {
        if (VERBOSE) {
          System.out.println("  range " + rangeID + " expectedCount=" + expectedCounts[rangeID]);
        }
        LabelAndValue subNode = result.labelValues[rangeID];
        assertEquals("r" + rangeID, subNode.label);
        assertEquals(expectedCounts[rangeID], subNode.value.intValue());

        LongRange range = ranges[rangeID];

        // Test drill-down:
        DrillDownQuery ddq = new DrillDownQuery(config);
        if (random().nextBoolean()) {
          ddq.add("field", LongPoint.newRangeQuery("field", range.min, range.max));
        } else {
          ddq.add("field", range.getQuery(fastMatchQuery, vs));
        }
        assertEquals(expectedCounts[rangeID], s.count(ddq));
      }
    }

    w.close();
    IOUtils.close(r, dir);
  }

  public void testRandomLongsMultiValued() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int numDocs = atLeast(1000);
    if (VERBOSE) {
      System.out.println("TEST: numDocs=" + numDocs);
    }
    long[][] values = new long[numDocs][];
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numVals = RandomNumbers.randomIntBetween(random(), 1, 50);
      if (random().nextInt(10) == 0) {
        numVals = 1; // make sure we have ample testing of single-value cases
      }
      values[i] = new long[numVals];
      for (int j = 0; j < numVals; j++) {
        long v = random().nextLong();
        values[i][j] = v;
        doc.add(new SortedNumericDocValuesField("field", v));
        doc.add(new LongPoint("field", v));
        minValue = Math.min(minValue, v);
        maxValue = Math.max(maxValue, v);
      }
      w.addDocument(doc);
    }
    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r, false);
    FacetsConfig config = new FacetsConfig();

    int numIters = atLeast(10);
    for (int iter = 0; iter < numIters; iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      int numRange = TestUtil.nextInt(random(), 1, 100);
      LongRange[] ranges = new LongRange[numRange];
      int[] expectedCounts = new int[numRange];
      long minAcceptedValue = Long.MAX_VALUE;
      long maxAcceptedValue = Long.MIN_VALUE;
      for (int rangeID = 0; rangeID < numRange; rangeID++) {
        long min;
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          LongRange prevRange = ranges[random().nextInt(rangeID)];
          if (random().nextBoolean()) {
            min = prevRange.min;
          } else {
            min = prevRange.max;
          }
        } else {
          min = random().nextLong();
        }
        long max;
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          LongRange prevRange = ranges[random().nextInt(rangeID)];
          if (random().nextBoolean()) {
            max = prevRange.min;
          } else {
            max = prevRange.max;
          }
        } else {
          max = random().nextLong();
        }

        if (min > max) {
          long x = min;
          min = max;
          max = x;
        }
        boolean minIncl;
        boolean maxIncl;

        // NOTE: max - min >= 0 is here to handle the common overflow case!
        if (max - min >= 0 && max - min < 2) {
          // If max == min or max == min+1, we always do inclusive, else we might pass an empty
          // range and hit exc from LongRange's ctor:
          minIncl = true;
          maxIncl = true;
        } else {
          minIncl = random().nextBoolean();
          maxIncl = random().nextBoolean();
        }
        ranges[rangeID] = new LongRange("r" + rangeID, min, minIncl, max, maxIncl);
        if (VERBOSE) {
          System.out.println("  range " + rangeID + ": " + ranges[rangeID]);
        }

        // Do "slow but hopefully correct" computation of
        // expected count:
        for (int i = 0; i < numDocs; i++) {
          for (int j = 0; j < values[i].length; j++) {
            boolean accept = true;
            if (minIncl) {
              accept &= values[i][j] >= min;
            } else {
              accept &= values[i][j] > min;
            }
            if (maxIncl) {
              accept &= values[i][j] <= max;
            } else {
              accept &= values[i][j] < max;
            }
            if (accept) {
              expectedCounts[rangeID]++;
              minAcceptedValue = Math.min(minAcceptedValue, values[i][j]);
              maxAcceptedValue = Math.max(maxAcceptedValue, values[i][j]);
              break; // ensure each doc can contribute at most 1 count to each range
            }
          }
        }
      }

      FacetsCollector sfc = new FacetsCollector();
      s.search(new MatchAllDocsQuery(), sfc);
      Query fastMatchQuery;
      if (random().nextBoolean()) {
        if (random().nextBoolean()) {
          fastMatchQuery = LongPoint.newRangeQuery("field", minValue, maxValue);
        } else {
          fastMatchQuery = LongPoint.newRangeQuery("field", minAcceptedValue, maxAcceptedValue);
        }
      } else {
        fastMatchQuery = null;
      }
      Facets facets = new LongRangeFacetCounts("field", null, sfc, fastMatchQuery, ranges);
      FacetResult result = facets.getTopChildren(10, "field");
      assertEquals(numRange, result.labelValues.length);
      for (int rangeID = 0; rangeID < numRange; rangeID++) {
        if (VERBOSE) {
          System.out.println("  range " + rangeID + " expectedCount=" + expectedCounts[rangeID]);
        }
        LabelAndValue subNode = result.labelValues[rangeID];
        assertEquals("r" + rangeID, subNode.label);
        assertEquals(expectedCounts[rangeID], subNode.value.intValue());

        LongRange range = ranges[rangeID];

        // Test drill-down:
        DrillDownQuery ddq = new DrillDownQuery(config);
        if (random().nextBoolean()) {
          ddq.add("field", LongPoint.newRangeQuery("field", range.min, range.max));
        } else {
          ddq.add(
              "field",
              SortedNumericDocValuesField.newSlowRangeQuery("field", range.min, range.max));
        }
        assertEquals(expectedCounts[rangeID], s.count(ddq));
      }
    }

    w.close();
    IOUtils.close(r, dir);
  }

  public void testRandomDoublesSingleValued() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int numDocs = atLeast(1000);
    double[] values = new double[numDocs];
    double minValue = Double.POSITIVE_INFINITY;
    double maxValue = Double.NEGATIVE_INFINITY;
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      double v = random().nextDouble();
      values[i] = v;
      doc.add(new DoubleDocValuesField("field", v));
      doc.add(new DoublePoint("field", v));
      w.addDocument(doc);
      minValue = Math.min(minValue, v);
      maxValue = Math.max(maxValue, v);
    }
    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r, false);
    FacetsConfig config = new FacetsConfig();
    
    int numIters = atLeast(10);
    for(int iter=0;iter<numIters;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      int numRange = TestUtil.nextInt(random(), 1, 5);
      DoubleRange[] ranges = new DoubleRange[numRange];
      int[] expectedCounts = new int[numRange];
      double minAcceptedValue = Double.POSITIVE_INFINITY;
      double maxAcceptedValue = Double.NEGATIVE_INFINITY;
      for(int rangeID=0;rangeID<numRange;rangeID++) {
        double min;
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          DoubleRange prevRange = ranges[random().nextInt(rangeID)];
          if (random().nextBoolean()) {
            min = prevRange.min;
          } else {
            min = prevRange.max;
          }
        } else {
          min = random().nextDouble();
        }
        double max;
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          DoubleRange prevRange = ranges[random().nextInt(rangeID)];
          if (random().nextBoolean()) {
            max = prevRange.min;
          } else {
            max = prevRange.max;
          }
        } else {
          max = random().nextDouble();
        }

        if (min > max) {
          double x = min;
          min = max;
          max = x;
        }

        boolean minIncl;
        boolean maxIncl;
        
        long minAsLong = NumericUtils.doubleToSortableLong(min);
        long maxAsLong = NumericUtils.doubleToSortableLong(max);
        // NOTE: maxAsLong - minAsLong >= 0 is here to handle the common overflow case!
        if (maxAsLong - minAsLong >= 0 && maxAsLong - minAsLong < 2) {
          minIncl = true;
          maxIncl = true;
        } else {
          minIncl = random().nextBoolean();
          maxIncl = random().nextBoolean();
        }
        ranges[rangeID] = new DoubleRange("r" + rangeID, min, minIncl, max, maxIncl);

        // Do "slow but hopefully correct" computation of
        // expected count:
        for(int i=0;i<numDocs;i++) {
          boolean accept = true;
          if (minIncl) {
            accept &= values[i] >= min;
          } else {
            accept &= values[i] > min;
          }
          if (maxIncl) {
            accept &= values[i] <= max;
          } else {
            accept &= values[i] < max;
          }
          if (accept) {
            expectedCounts[rangeID]++;
            minAcceptedValue = Math.min(minAcceptedValue, values[i]);
            maxAcceptedValue = Math.max(maxAcceptedValue, values[i]);
          }
        }
      }

      FacetsCollector sfc = new FacetsCollector();
      s.search(new MatchAllDocsQuery(), sfc);
      Query fastMatchFilter;
      if (random().nextBoolean()) {
        if (random().nextBoolean()) {
          fastMatchFilter = DoublePoint.newRangeQuery("field", minValue, maxValue);
        } else {
          fastMatchFilter = DoublePoint.newRangeQuery("field", minAcceptedValue, maxAcceptedValue);
        }
      } else {
        fastMatchFilter = null;
      }
      DoubleValuesSource vs = DoubleValuesSource.fromDoubleField("field");
      Facets facets = new DoubleRangeFacetCounts("field", vs, sfc, fastMatchFilter, ranges);
      FacetResult result = facets.getTopChildren(10, "field");
      assertEquals(numRange, result.labelValues.length);
      for(int rangeID=0;rangeID<numRange;rangeID++) {
        if (VERBOSE) {
          System.out.println("  range " + rangeID + " expectedCount=" + expectedCounts[rangeID]);
        }
        LabelAndValue subNode = result.labelValues[rangeID];
        assertEquals("r" + rangeID, subNode.label);
        assertEquals(expectedCounts[rangeID], subNode.value.intValue());

        DoubleRange range = ranges[rangeID];

        // Test drill-down:
        DrillDownQuery ddq = new DrillDownQuery(config);
        if (random().nextBoolean()) {
          ddq.add("field", DoublePoint.newRangeQuery("field", range.min, range.max));
        } else {
          ddq.add("field", range.getQuery(fastMatchFilter, vs));
        }

        assertEquals(expectedCounts[rangeID], s.count(ddq));
      }
    }

    w.close();
    IOUtils.close(r, dir);
  }

  public void testRandomDoublesMultiValued() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int numDocs = atLeast(1000);
    double[][] values = new double[numDocs][];
    double minValue = Double.POSITIVE_INFINITY;
    double maxValue = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numVals = RandomNumbers.randomIntBetween(random(), 1, 50);
      if (random().nextInt(10) == 0) {
        numVals = 1; // make sure we have ample testing of single-value cases
      }
      values[i] = new double[numVals];
      for (int j = 0; j < numVals; j++) {
        double v = random().nextDouble();
        values[i][j] = v;
        doc.add(new SortedNumericDocValuesField("field", Double.doubleToLongBits(v)));
        doc.add(new DoublePoint("field", v));
        minValue = Math.min(minValue, v);
        maxValue = Math.max(maxValue, v);
      }
      w.addDocument(doc);
    }
    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r, false);
    FacetsConfig config = new FacetsConfig();

    int numIters = atLeast(10);
    for (int iter = 0; iter < numIters; iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      int numRange = TestUtil.nextInt(random(), 1, 5);
      DoubleRange[] ranges = new DoubleRange[numRange];
      int[] expectedCounts = new int[numRange];
      double minAcceptedValue = Double.POSITIVE_INFINITY;
      double maxAcceptedValue = Double.NEGATIVE_INFINITY;
      for (int rangeID = 0; rangeID < numRange; rangeID++) {
        double min;
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          DoubleRange prevRange = ranges[random().nextInt(rangeID)];
          if (random().nextBoolean()) {
            min = prevRange.min;
          } else {
            min = prevRange.max;
          }
        } else {
          min = random().nextDouble();
        }
        double max;
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          DoubleRange prevRange = ranges[random().nextInt(rangeID)];
          if (random().nextBoolean()) {
            max = prevRange.min;
          } else {
            max = prevRange.max;
          }
        } else {
          max = random().nextDouble();
        }

        if (min > max) {
          double x = min;
          min = max;
          max = x;
        }

        boolean minIncl;
        boolean maxIncl;

        long minAsLong = NumericUtils.doubleToSortableLong(min);
        long maxAsLong = NumericUtils.doubleToSortableLong(max);
        // NOTE: maxAsLong - minAsLong >= 0 is here to handle the common overflow case!
        if (maxAsLong - minAsLong >= 0 && maxAsLong - minAsLong < 2) {
          minIncl = true;
          maxIncl = true;
        } else {
          minIncl = random().nextBoolean();
          maxIncl = random().nextBoolean();
        }
        ranges[rangeID] = new DoubleRange("r" + rangeID, min, minIncl, max, maxIncl);

        // Do "slow but hopefully correct" computation of
        // expected count:
        for (int i = 0; i < numDocs; i++) {
          for (int j = 0; j < values[i].length; j++) {
            boolean accept = true;
            if (minIncl) {
              accept &= values[i][j] >= min;
            } else {
              accept &= values[i][j] > min;
            }
            if (maxIncl) {
              accept &= values[i][j] <= max;
            } else {
              accept &= values[i][j] < max;
            }
            if (accept) {
              expectedCounts[rangeID]++;
              minAcceptedValue = Math.min(minAcceptedValue, values[i][j]);
              maxAcceptedValue = Math.max(maxAcceptedValue, values[i][j]);
              break; // ensure each doc can contribute at most 1 count to each range
            }
          }
        }
      }

      FacetsCollector sfc = new FacetsCollector();
      s.search(new MatchAllDocsQuery(), sfc);
      Query fastMatchFilter;
      if (random().nextBoolean()) {
        if (random().nextBoolean()) {
          fastMatchFilter = DoublePoint.newRangeQuery("field", minValue, maxValue);
        } else {
          fastMatchFilter = DoublePoint.newRangeQuery("field", minAcceptedValue, maxAcceptedValue);
        }
      } else {
        fastMatchFilter = null;
      }
      Facets facets = new DoubleRangeFacetCounts("field", null, sfc, fastMatchFilter, ranges);
      FacetResult result = facets.getTopChildren(10, "field");
      assertEquals(numRange, result.labelValues.length);
      for (int rangeID = 0; rangeID < numRange; rangeID++) {
        if (VERBOSE) {
          System.out.println("  range " + rangeID + " expectedCount=" + expectedCounts[rangeID]);
        }
        LabelAndValue subNode = result.labelValues[rangeID];
        assertEquals("r" + rangeID, subNode.label);
        assertEquals(expectedCounts[rangeID], subNode.value.intValue());

        DoubleRange range = ranges[rangeID];

        // Test drill-down:
        DrillDownQuery ddq = new DrillDownQuery(config);
        if (random().nextBoolean()) {
          ddq.add("field", DoublePoint.newRangeQuery("field", range.min, range.max));
        } else {
          ddq.add(
              "field",
              SortedNumericDocValuesField.newSlowRangeQuery(
                  "field", Double.doubleToLongBits(range.min), Double.doubleToLongBits(range.max)));
        }

        assertEquals(expectedCounts[rangeID], s.count(ddq));
      }
    }

    w.close();
    IOUtils.close(r, dir);
  }

  // LUCENE-5178
  public void testMissingValues() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    NumericDocValuesField field = new NumericDocValuesField("field", 0L);
    doc.add(field);
    for(long l=0;l<100;l++) {
      if (l % 5 == 0) {
        // Every 5th doc is missing the value:
        w.addDocument(new Document());
        continue;
      }
      field.setLongValue(l);
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();

    FacetsCollector fc = new FacetsCollector();

    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);
    Facets facets = new LongRangeFacetCounts("field", fc,
        new LongRange("less than 10", 0L, true, 10L, false),
        new LongRange("less than or equal to 10", 0L, true, 10L, true),
        new LongRange("over 90", 90L, false, 100L, false),
        new LongRange("90 or above", 90L, true, 100L, false),
        new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, false));
    
    assertEquals("dim=field path=[] value=16 childCount=5\n  less than 10 (8)\n  less than or equal to 10 (8)\n  over 90 (8)\n  90 or above (8)\n  over 1000 (0)\n",
                 facets.getTopChildren(10, "field").toString());

    w.close();
    IOUtils.close(r, d);
  }

  public void testMissingValuesMultiValued() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    // index the same field twice to test multi-valued logic
    SortedNumericDocValuesField field1 = new SortedNumericDocValuesField("field", 0L);
    SortedNumericDocValuesField field2 = new SortedNumericDocValuesField("field", 0L);
    doc.add(field1);
    doc.add(field2);
    for (long l = 0; l < 100; l++) {
      if (l % 5 == 0) {
        // Every 5th doc is missing the value:
        w.addDocument(new Document());
        continue;
      }
      field1.setLongValue(l);
      field2.setLongValue(l);
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();

    FacetsCollector fc = new FacetsCollector();

    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);
    Facets facets =
            new LongRangeFacetCounts(
                    "field",
                    fc,
                    new LongRange("less than 10", 0L, true, 10L, false),
                    new LongRange("less than or equal to 10", 0L, true, 10L, true),
                    new LongRange("over 90", 90L, false, 100L, false),
                    new LongRange("90 or above", 90L, true, 100L, false),
                    new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, false));

    assertEquals(
            "dim=field path=[] value=16 childCount=5\n  less than 10 (8)\n  less than or equal to 10 (8)\n  over 90 (8)\n  90 or above (8)\n  over 1000 (0)\n",
            facets.getTopChildren(10, "field").toString());

    w.close();
    IOUtils.close(r, d);
  }

  private static class UsedQuery extends Query {

    private final AtomicBoolean used;
    private final Query in;

    UsedQuery(Query in, AtomicBoolean used) {
      this.in = in;
      this.used = used;
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) &&
             in.equals(((UsedQuery) other).in);
    }

    @Override
    public int hashCode() {
      return classHash() + in.hashCode();
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      final Query inRewritten = in.rewrite(reader);
      if (in != inRewritten) {
        return new UsedQuery(inRewritten, used);
      }
      return super.rewrite(reader);
    }

    @Override
    public void visit(QueryVisitor visitor) {
      in.visit(visitor);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      final Weight in = this.in.createWeight(searcher, scoreMode, boost);
      return new FilterWeight(in) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          used.set(true);
          return in.scorer(context);
        }
      };
    }

    @Override
    public String toString(String field) {
      return "UsedQuery(" + in + ")";
    }

  }

  private static class PlusOneValuesSource extends DoubleValuesSource {

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      return new DoubleValues() {
        int doc = -1;
        @Override
        public double doubleValue() throws IOException {
          return doc + 1;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          this.doc = doc;
          return true;
        }
      };
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) throws IOException {
      return Explanation.match(docId + 1, "");
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return obj.getClass() == PlusOneValuesSource.class;
    }

    @Override
    public String toString() {
      return null;
    }
  }

  public void testCustomDoubleValuesSource() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    
    Document doc = new Document();
    writer.addDocument(doc);
    writer.addDocument(doc);
    writer.addDocument(doc);

    // Test wants 3 docs in one segment:
    writer.forceMerge(1);

    final DoubleValuesSource vs = new PlusOneValuesSource();

    FacetsConfig config = new FacetsConfig();
    
    FacetsCollector fc = new FacetsCollector();

    IndexReader r = writer.getReader();

    IndexSearcher s = newSearcher(r, false, false);
    s.search(new MatchAllDocsQuery(), fc);

    final DoubleRange[] ranges = new DoubleRange[] {
        new DoubleRange("< 1", 0.0, true, 1.0, false),
        new DoubleRange("< 2", 0.0, true, 2.0, false),
        new DoubleRange("< 5", 0.0, true, 5.0, false),
        new DoubleRange("< 10", 0.0, true, 10.0, false),
        new DoubleRange("< 20", 0.0, true, 20.0, false),
        new DoubleRange("< 50", 0.0, true, 50.0, false)};

    final Query fastMatchFilter;
    final AtomicBoolean filterWasUsed = new AtomicBoolean();
    if (random().nextBoolean()) {
      // Sort of silly:
      final Query in = new MatchAllDocsQuery();
      fastMatchFilter = new UsedQuery(in, filterWasUsed);
    } else {
      fastMatchFilter = null;
    }

    if (VERBOSE) {
      System.out.println("TEST: fastMatchFilter=" + fastMatchFilter);
    }

    Facets facets = new DoubleRangeFacetCounts("field", vs, fc, fastMatchFilter, ranges);

    assertEquals("dim=field path=[] value=3 childCount=6\n  < 1 (0)\n  < 2 (1)\n  < 5 (3)\n  < 10 (3)\n  < 20 (3)\n  < 50 (3)\n", facets.getTopChildren(10, "field").toString());
    assertTrue(fastMatchFilter == null || filterWasUsed.get());

    DrillDownQuery ddq = new DrillDownQuery(config);
    ddq.add("field", ranges[1].getQuery(fastMatchFilter, vs));

    // Test simple drill-down:
    assertEquals(1, s.search(ddq, 10).totalHits.value);

    // Test drill-sideways after drill-down
    DrillSideways ds = new DrillSideways(s, config, (TaxonomyReader) null) {

        @Override
        protected Facets buildFacetsResult(FacetsCollector drillDowns, FacetsCollector[] drillSideways, String[] drillSidewaysDims) throws IOException {        
          assert drillSideways.length == 1;
          return new DoubleRangeFacetCounts("field", vs, drillSideways[0], fastMatchFilter, ranges);
        }

        @Override
        protected boolean scoreSubDocsAtOnce() {
          return random().nextBoolean();
        }
      };


    DrillSidewaysResult dsr = ds.search(ddq, 10);
    assertEquals(1, dsr.hits.totalHits.value);
    assertEquals("dim=field path=[] value=3 childCount=6\n  < 1 (0)\n  < 2 (1)\n  < 5 (3)\n  < 10 (3)\n  < 20 (3)\n  < 50 (3)\n",
                 dsr.facets.getTopChildren(10, "field").toString());

    writer.close();
    IOUtils.close(r, dir);
  }

  public void testLongRangeEquals() throws Exception {
    assertEquals(new LongRange("field", -7, true, 17, false),
                 new LongRange("field", -7, true, 17, false));
    assertEquals(new LongRange("field", -7, true, 17, false).hashCode(),
                 new LongRange("field", -7, true, 17, false).hashCode());
    assertFalse(new LongRange("field", -7, true, 17, false).equals(new LongRange("field", -7, true, 17, true)));
    assertFalse(new LongRange("field", -7, true, 17, false).hashCode() ==
                new LongRange("field", -7, true, 17, true).hashCode());
    assertFalse(new LongRange("field", -7, true, 17, false).equals(new LongRange("field", -7, true, 18, false)));
    assertFalse(new LongRange("field", -7, true, 17, false).hashCode() ==
                new LongRange("field", -7, true, 18, false).hashCode());
  }

  public void testDoubleRangeEquals() throws Exception {
    assertEquals(new DoubleRange("field", -7d, true, 17d, false),
                 new DoubleRange("field", -7d, true, 17d, false));
    assertEquals(new DoubleRange("field", -7d, true, 17d, false).hashCode(),
                 new DoubleRange("field", -7d, true, 17d, false).hashCode());
    assertFalse(new DoubleRange("field", -7d, true, 17d, false).equals(new DoubleRange("field", -7d, true, 17d, true)));
    assertFalse(new DoubleRange("field", -7d, true, 17d, false).hashCode() ==
                new DoubleRange("field", -7d, true, 17d, true).hashCode());
    assertFalse(new DoubleRange("field", -7d, true, 17d, false).equals(new DoubleRange("field", -7d, true, 18d, false)));
    assertFalse(new DoubleRange("field", -7d, true, 17d, false).hashCode() ==
                new DoubleRange("field", -7d, true, 18, false).hashCode());
  }
}

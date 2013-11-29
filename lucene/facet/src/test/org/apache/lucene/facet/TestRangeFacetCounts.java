package org.apache.lucene.facet;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util._TestUtil;


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
    field.setLongValue(Long.MAX_VALUE);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    FacetsCollector fc = new FacetsCollector();
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    RangeFacetCounts facets = new RangeFacetCounts("field", fc,
        new LongRange("less than 10", 0L, true, 10L, false),
        new LongRange("less than or equal to 10", 0L, true, 10L, true),
        new LongRange("over 90", 90L, false, 100L, false),
        new LongRange("90 or above", 90L, true, 100L, false),
        new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, true));
    
    FacetResult result = facets.getTopChildren(10, "field");
    assertEquals("value=101 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (1)\n",
                 result.toString());
    
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
      doc.add(new LongField("field", l, Field.Store.NO));

      if ((l&3) == 0) {
        doc.add(new FacetField("dim", "a"));
      } else {
        doc.add(new FacetField("dim", "b"));
      }
      w.addDocument(config.build(tw, doc));
    }

    final IndexReader r = w.getReader();

    final TaxonomyReader tr = new DirectoryTaxonomyReader(tw);

    IndexSearcher s = newSearcher(r);

    DrillSideways ds = new DrillSideways(s, config, tr) {

        @Override
        protected Facets buildFacetsResult(FacetsCollector drillDowns, FacetsCollector[] drillSideways, String[] drillSidewaysDims) throws IOException {        
          // nocommit this is awkward... can we improve?
          // nocommit is drillDowns allowed to be null?
          // should it?
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

          Map<String,Facets> byDim = new HashMap<String,Facets>();
          byDim.put("field",
                    new RangeFacetCounts("field", fieldFC,
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

    assertEquals(100, dsr.hits.totalHits);
    assertEquals("value=100 childCount=2\n  b (75)\n  a (25)\n", dsr.facets.getTopChildren(10, "dim").toString());
    assertEquals("value=100 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (0)\n",
                 dsr.facets.getTopChildren(10, "field").toString());

    // Second search, drill down on dim=b:
    ddq = new DrillDownQuery(config);
    ddq.add("dim", "b");
    dsr = ds.search(null, ddq, 10);

    assertEquals(75, dsr.hits.totalHits);
    assertEquals("value=100 childCount=2\n  b (75)\n  a (25)\n", dsr.facets.getTopChildren(10, "dim").toString());
    assertEquals("value=75 childCount=5\n  less than 10 (7)\n  less than or equal to 10 (8)\n  over 90 (7)\n  90 or above (8)\n  over 1000 (0)\n",
                 dsr.facets.getTopChildren(10, "field").toString());

    // Third search, drill down on "less than or equal to 10":
    ddq = new DrillDownQuery(config);
    ddq.add("field", NumericRangeQuery.newLongRange("field", 0L, 10L, true, true));
    dsr = ds.search(null, ddq, 10);

    assertEquals(11, dsr.hits.totalHits);
    assertEquals("value=11 childCount=2\n  b (8)\n  a (3)\n", dsr.facets.getTopChildren(10, "dim").toString());
    assertEquals("value=100 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (0)\n",
                 dsr.facets.getTopChildren(10, "field").toString());
    IOUtils.close(tw, tr, td, w, r, d);
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
    Facets facets = new RangeFacetCounts("field", fc,
        new DoubleRange("less than 10", 0.0, true, 10.0, false),
        new DoubleRange("less than or equal to 10", 0.0, true, 10.0, true),
        new DoubleRange("over 90", 90.0, false, 100.0, false),
        new DoubleRange("90 or above", 90.0, true, 100.0, false),
        new DoubleRange("over 1000", 1000.0, false, Double.POSITIVE_INFINITY, false));
                                         
    assertEquals("value=100 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (0)\n",
                 facets.getTopChildren(10, "field").toString());

    IOUtils.close(w, r, d);
  }

  public void testBasicFloat() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    FloatDocValuesField field = new FloatDocValuesField("field", 0.0f);
    doc.add(field);
    for(long l=0;l<100;l++) {
      field.setFloatValue(l);
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();

    FacetsCollector fc = new FacetsCollector();

    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    Facets facets = new RangeFacetCounts("field", fc,
        new FloatRange("less than 10", 0.0f, true, 10.0f, false),
        new FloatRange("less than or equal to 10", 0.0f, true, 10.0f, true),
        new FloatRange("over 90", 90.0f, false, 100.0f, false),
        new FloatRange("90 or above", 90.0f, true, 100.0f, false),
        new FloatRange("over 1000", 1000.0f, false, Float.POSITIVE_INFINITY, false));
    
    assertEquals("value=100 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (9)\n  90 or above (10)\n  over 1000 (0)\n",
                 facets.getTopChildren(10, "field").toString());
    
    IOUtils.close(w, r, d);
  }

  public void testRandomLongs() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int numDocs = atLeast(1000);
    long[] values = new long[numDocs];
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      long v = random().nextLong();
      values[i] = v;
      doc.add(new NumericDocValuesField("field", v));
      doc.add(new LongField("field", v, Field.Store.NO));
      w.addDocument(doc);
    }
    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r);
    FacetsConfig config = new FacetsConfig();
    
    int numIters = atLeast(10);
    for(int iter=0;iter<numIters;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      int numRange = _TestUtil.nextInt(random(), 1, 5);
      LongRange[] ranges = new LongRange[numRange];
      int[] expectedCounts = new int[numRange];
      for(int rangeID=0;rangeID<numRange;rangeID++) {
        long min = random().nextLong();
        long max = random().nextLong();
        if (min > max) {
          long x = min;
          min = max;
          max = x;
        }
        boolean minIncl = random().nextBoolean();
        boolean maxIncl = random().nextBoolean();
        ranges[rangeID] = new LongRange("r" + rangeID, min, minIncl, max, maxIncl);

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
          }
        }
      }

      FacetsCollector sfc = new FacetsCollector();
      s.search(new MatchAllDocsQuery(), sfc);
      Facets facets = new RangeFacetCounts("field", sfc, ranges);
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
        ddq.add("field", NumericRangeQuery.newLongRange("field", range.min, range.max, range.minInclusive, range.maxInclusive));
        assertEquals(expectedCounts[rangeID], s.search(ddq, 10).totalHits);
      }
    }

    IOUtils.close(w, r, dir);
  }

  public void testRandomFloats() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int numDocs = atLeast(1000);
    float[] values = new float[numDocs];
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      float v = random().nextFloat();
      values[i] = v;
      doc.add(new FloatDocValuesField("field", v));
      doc.add(new FloatField("field", v, Field.Store.NO));
      w.addDocument(doc);
    }
    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r);
    FacetsConfig config = new FacetsConfig();
    
    int numIters = atLeast(10);
    for(int iter=0;iter<numIters;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      int numRange = _TestUtil.nextInt(random(), 1, 5);
      FloatRange[] ranges = new FloatRange[numRange];
      int[] expectedCounts = new int[numRange];
      for(int rangeID=0;rangeID<numRange;rangeID++) {
        float min = random().nextFloat();
        float max = random().nextFloat();
        if (min > max) {
          float x = min;
          min = max;
          max = x;
        }
        boolean minIncl = random().nextBoolean();
        boolean maxIncl = random().nextBoolean();
        ranges[rangeID] = new FloatRange("r" + rangeID, min, minIncl, max, maxIncl);

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
          }
        }
      }

      FacetsCollector sfc = new FacetsCollector();
      s.search(new MatchAllDocsQuery(), sfc);
      Facets facets = new RangeFacetCounts("field", sfc, ranges);
      FacetResult result = facets.getTopChildren(10, "field");
      assertEquals(numRange, result.labelValues.length);
      for(int rangeID=0;rangeID<numRange;rangeID++) {
        if (VERBOSE) {
          System.out.println("  range " + rangeID + " expectedCount=" + expectedCounts[rangeID]);
        }
        LabelAndValue subNode = result.labelValues[rangeID];
        assertEquals("r" + rangeID, subNode.label);
        assertEquals(expectedCounts[rangeID], subNode.value.intValue());

        FloatRange range = ranges[rangeID];

        // Test drill-down:
        DrillDownQuery ddq = new DrillDownQuery(config);
        ddq.add("field", NumericRangeQuery.newFloatRange("field", range.min, range.max, range.minInclusive, range.maxInclusive));
        assertEquals(expectedCounts[rangeID], s.search(ddq, 10).totalHits);
      }
    }

    IOUtils.close(w, r, dir);
  }

  public void testRandomDoubles() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int numDocs = atLeast(1000);
    double[] values = new double[numDocs];
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      double v = random().nextDouble();
      values[i] = v;
      doc.add(new DoubleDocValuesField("field", v));
      doc.add(new DoubleField("field", v, Field.Store.NO));
      w.addDocument(doc);
    }
    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r);
    FacetsConfig config = new FacetsConfig();
    
    int numIters = atLeast(10);
    for(int iter=0;iter<numIters;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      int numRange = _TestUtil.nextInt(random(), 1, 5);
      DoubleRange[] ranges = new DoubleRange[numRange];
      int[] expectedCounts = new int[numRange];
      for(int rangeID=0;rangeID<numRange;rangeID++) {
        double min = random().nextDouble();
        double max = random().nextDouble();
        if (min > max) {
          double x = min;
          min = max;
          max = x;
        }
        boolean minIncl = random().nextBoolean();
        boolean maxIncl = random().nextBoolean();
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
          }
        }
      }

      FacetsCollector sfc = new FacetsCollector();
      s.search(new MatchAllDocsQuery(), sfc);
      Facets facets = new RangeFacetCounts("field", sfc, ranges);
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
        ddq.add("field", NumericRangeQuery.newDoubleRange("field", range.min, range.max, range.minInclusive, range.maxInclusive));
        assertEquals(expectedCounts[rangeID], s.search(ddq, 10).totalHits);
      }
    }

    IOUtils.close(w, r, dir);
  }

  // LUCENE-5178
  public void testMissingValues() throws Exception {
    assumeTrue("codec does not support docsWithField", defaultCodecSupportsDocsWithField());
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

    FacetsCollector sfc = new FacetsCollector();

    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), sfc);
    Facets facets = new RangeFacetCounts("field", sfc,
        new LongRange("less than 10", 0L, true, 10L, false),
        new LongRange("less than or equal to 10", 0L, true, 10L, true),
        new LongRange("over 90", 90L, false, 100L, false),
        new LongRange("90 or above", 90L, true, 100L, false),
        new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, false));
    
    assertEquals("value=100 childCount=5\n  less than 10 (8)\n  less than or equal to 10 (8)\n  over 90 (8)\n  90 or above (8)\n  over 1000 (0)\n",
                 facets.getTopChildren(10, "field").toString());

    IOUtils.close(w, r, d);
  }
}

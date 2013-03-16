package org.apache.lucene.facet.sortedset;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.FacetTestUtils;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.DrillDownQuery;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

public class TestSortedSetDocValuesFacets extends FacetTestCase {

  // NOTE: TestDrillSideways.testRandom also sometimes
  // randomly uses SortedSetDV

  public void testSortedSetDocValuesAccumulator() throws Exception {
    assumeTrue("Test requires SortedSetDV support", defaultCodecSupportsSortedSet());
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Use a custom delim char to make sure the impls
    // respect it:
    final char delim = ':';
    FacetIndexingParams fip = new FacetIndexingParams() {
        @Override
        public char getFacetDelimChar() {
          return delim;
        }
      };

    SortedSetDocValuesFacetFields dvFields = new SortedSetDocValuesFacetFields(fip);

    Document doc = new Document();
    // Mixup order we add these paths, to verify tie-break
    // order is by label (unicode sort) and has nothing to
    // do w/ order we added them:
    List<CategoryPath> paths = new ArrayList<CategoryPath>();
    paths.add(new CategoryPath("a", "foo"));
    paths.add(new CategoryPath("a", "bar"));
    paths.add(new CategoryPath("a", "zoo"));
    Collections.shuffle(paths, random());

    paths.add(new CategoryPath("b", "baz"));
    paths.add(new CategoryPath("b" + FacetIndexingParams.DEFAULT_FACET_DELIM_CHAR, "bazfoo"));

    dvFields.addFields(doc, paths);

    writer.addDocument(doc);
    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    dvFields.addFields(doc, Collections.singletonList(new CategoryPath("a", "foo")));
    writer.addDocument(doc);

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    List<FacetRequest> requests = new ArrayList<FacetRequest>();
    requests.add(new CountFacetRequest(new CategoryPath("a"), 10));
    requests.add(new CountFacetRequest(new CategoryPath("b"), 10));
    requests.add(new CountFacetRequest(new CategoryPath("b" + FacetIndexingParams.DEFAULT_FACET_DELIM_CHAR), 10));

    final boolean doDimCount = random().nextBoolean();

    CategoryListParams clp = new CategoryListParams() {
        @Override
        public OrdinalPolicy getOrdinalPolicy(String dimension) {
          return doDimCount ? OrdinalPolicy.NO_PARENTS : OrdinalPolicy.ALL_BUT_DIMENSION;
        }
      };

    FacetSearchParams fsp = new FacetSearchParams(new FacetIndexingParams(clp), requests);

    // Per-top-reader state:
    SortedSetDocValuesReaderState state = new SortedSetDocValuesReaderState(fip, searcher.getIndexReader());
    
    //SortedSetDocValuesCollector c = new SortedSetDocValuesCollector(state);
    //SortedSetDocValuesCollectorMergeBySeg c = new SortedSetDocValuesCollectorMergeBySeg(state);

    FacetsCollector c = FacetsCollector.create(new SortedSetDocValuesAccumulator(fsp, state));

    searcher.search(new MatchAllDocsQuery(), c);

    //List<FacetResult> results = c.getFacetResults(requests);
    List<FacetResult> results = c.getFacetResults();

    assertEquals(3, results.size());

    int dimCount = doDimCount ? 4 : 0;
    assertEquals("a (" + dimCount + ")\n  foo (2)\n  bar (1)\n  zoo (1)\n", FacetTestUtils.toSimpleString(results.get(0)));

    dimCount = doDimCount ? 1 : 0;
    assertEquals("b (" + dimCount + ")\n  baz (1)\n", FacetTestUtils.toSimpleString(results.get(1)));

    dimCount = doDimCount ? 1 : 0;
    assertEquals("b" + FacetIndexingParams.DEFAULT_FACET_DELIM_CHAR + " (" + dimCount + ")\n  bazfoo (1)\n", FacetTestUtils.toSimpleString(results.get(2)));

    // DrillDown:

    DrillDownQuery q = new DrillDownQuery(fip);
    q.add(new CategoryPath("a", "foo"));
    q.add(new CategoryPath("b", "baz"));
    TopDocs hits = searcher.search(q, 1);
    assertEquals(1, hits.totalHits);

    q = new DrillDownQuery(fip);
    q.add(new CategoryPath("a"));
    hits = searcher.search(q, 1);
    assertEquals(2, hits.totalHits);

    searcher.getIndexReader().close();
    dir.close();
  }
}

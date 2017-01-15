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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestParallelDrillSideways extends TestDrillSideways {

  private static ExecutorService executor;

  @BeforeClass
  public static void prepareExecutor() {
    executor = Executors.newCachedThreadPool(new NamedThreadFactory("TestParallelDrillSideways"));
  }

  @AfterClass
  public static void shutdownExecutor() {
    executor.shutdown();
    executor = null;
  }

  protected DrillSideways getNewDrillSideways(IndexSearcher searcher, FacetsConfig config,
          SortedSetDocValuesReaderState state) {
    return new DrillSideways(searcher, config, null, state, executor);
  }

  protected DrillSideways getNewDrillSideways(IndexSearcher searcher, FacetsConfig config, TaxonomyReader taxoReader) {
    return new DrillSideways(searcher, config, taxoReader, null, executor);
  }

  protected DrillSideways getNewDrillSidewaysScoreSubdocsAtOnce(IndexSearcher searcher, FacetsConfig config,
          TaxonomyReader taxoReader) {
    return new DrillSideways(searcher, config, taxoReader, null, executor) {
      @Override
      protected boolean scoreSubDocsAtOnce() {
        return true;
      }
    };
  }

  protected DrillSideways getNewDrillSidewaysBuildFacetsResult(IndexSearcher searcher, FacetsConfig config,
          TaxonomyReader taxoReader) {
    return new DrillSideways(searcher, config, taxoReader, null, executor) {
      @Override
      protected Facets buildFacetsResult(FacetsCollector drillDowns, FacetsCollector[] drillSideways,
              String[] drillSidewaysDims) throws IOException {
        Map<String, Facets> drillSidewaysFacets = new HashMap<>();
        Facets drillDownFacets = getTaxonomyFacetCounts(taxoReader, config, drillDowns);
        if (drillSideways != null) {
          for (int i = 0; i < drillSideways.length; i++) {
            drillSidewaysFacets.put(drillSidewaysDims[i], getTaxonomyFacetCounts(taxoReader, config, drillSideways[i]));
          }
        }

        if (drillSidewaysFacets.isEmpty()) {
          return drillDownFacets;
        } else {
          return new MultiFacets(drillSidewaysFacets, drillDownFacets);
        }

      }
    };
  }

}

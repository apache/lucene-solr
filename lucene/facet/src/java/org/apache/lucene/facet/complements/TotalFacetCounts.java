package org.apache.lucene.facet.complements;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.Aggregator;
import org.apache.lucene.facet.search.CategoryListIterator;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.CountingAggregator;
import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.StandardFacetsAccumulator;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.util.PartitionsUtils;
import org.apache.lucene.facet.util.ScoredDocIdsUtils;
import org.apache.lucene.index.IndexReader;

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

/**
 * Maintain Total Facet Counts per partition, for given parameters:
 * <ul> 
 *  <li>Index reader of an index</li>
 *  <li>Taxonomy index reader</li>
 *  <li>Facet indexing params (and particularly the category list params)</li>
 *  <li></li>
 * </ul>
 * The total facet counts are maintained as an array of arrays of integers, 
 * where a separate array is kept for each partition.
 * 
 * @lucene.experimental
 */
public class TotalFacetCounts {
  
  /** total facet counts per partition: totalCounts[partition][ordinal%partitionLength] */
  private int[][] totalCounts = null;
  
  private final TaxonomyReader taxonomy;
  private final FacetIndexingParams facetIndexingParams;

  private final static AtomicInteger atomicGen4Test = new AtomicInteger(1);
  /** Creation type for test purposes */
  enum CreationType { Computed, Loaded } // for testing
  final int gen4test;
  final CreationType createType4test;
  
  /** 
   * Construct by key - from index Directory or by recomputing.
   */
  private TotalFacetCounts (TaxonomyReader taxonomy, FacetIndexingParams facetIndexingParams,
      int[][] counts, CreationType createType4Test) {
    this.taxonomy = taxonomy;
    this.facetIndexingParams = facetIndexingParams;
    this.totalCounts = counts;
    this.createType4test = createType4Test;
    this.gen4test = atomicGen4Test.incrementAndGet();
  }

  /**
   * Fill a partition's array with the TotalCountsArray values.
   * @param partitionArray array to fill
   * @param partition number of required partition 
   */
  public void fillTotalCountsForPartition(int[] partitionArray, int partition) {
    int partitionSize = partitionArray.length;
    int[] countArray = totalCounts[partition];
    if (countArray == null) {
      countArray = new int[partitionSize];
      totalCounts[partition] = countArray;
    }
    int length = Math.min(partitionSize, countArray.length);
    System.arraycopy(countArray, 0, partitionArray, 0, length);
  }
  
  /**
   * Return the total count of an input category
   * @param ordinal ordinal of category whose total count is required 
   */
  public int getTotalCount(int ordinal) {
    int partition = PartitionsUtils.partitionNumber(facetIndexingParams,ordinal);
    int offset = ordinal % PartitionsUtils.partitionSize(facetIndexingParams, taxonomy);
    return totalCounts[partition][offset];
  }
  
  static TotalFacetCounts loadFromFile(File inputFile, TaxonomyReader taxonomy, 
      FacetIndexingParams facetIndexingParams) throws IOException {
    DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(inputFile)));
    try {
      int[][] counts = new int[dis.readInt()][];
      for (int i=0; i<counts.length; i++) {
        int size = dis.readInt();
        if (size<0) {
          counts[i] = null;
        } else {
          counts[i] = new int[size];
          for (int j=0; j<size; j++) {
            counts[i][j] = dis.readInt();
          }
        }
      }
      return new TotalFacetCounts(taxonomy, facetIndexingParams, counts, CreationType.Loaded);
    } finally {
      dis.close();
    }
  }

  static void storeToFile(File outputFile, TotalFacetCounts tfc) throws IOException {
    DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
    try {
      dos.writeInt(tfc.totalCounts.length);
      for (int[] counts : tfc.totalCounts) {
        if (counts == null) {
          dos.writeInt(-1);
        } else {
          dos.writeInt(counts.length);
          for (int i : counts) {
            dos.writeInt(i);
          }
        }
      }
    } finally {
      dos.close();
    }
  }
  
  // needed because FacetSearchParams do not allow empty FacetRequests
  private static final FacetRequest DUMMY_REQ = new CountFacetRequest(CategoryPath.EMPTY, 1);

  static TotalFacetCounts compute(final IndexReader indexReader, final TaxonomyReader taxonomy, 
      final FacetIndexingParams facetIndexingParams) throws IOException {
    int partitionSize = PartitionsUtils.partitionSize(facetIndexingParams, taxonomy);
    final int[][] counts = new int[(int) Math.ceil(taxonomy.getSize()  /(float) partitionSize)][partitionSize];
    FacetSearchParams newSearchParams = new FacetSearchParams(facetIndexingParams, DUMMY_REQ); 
      //createAllListsSearchParams(facetIndexingParams,  this.totalCounts);
    StandardFacetsAccumulator sfa = new StandardFacetsAccumulator(newSearchParams, indexReader, taxonomy) {
      @Override
      protected HashMap<CategoryListIterator, Aggregator> getCategoryListMap(
          FacetArrays facetArrays, int partition) throws IOException {
        
        Aggregator aggregator = new CountingAggregator(counts[partition]);
        HashMap<CategoryListIterator, Aggregator> map = new HashMap<CategoryListIterator, Aggregator>();
        for (CategoryListParams clp: facetIndexingParams.getAllCategoryListParams()) {
          map.put(clp.createCategoryListIterator(partition), aggregator);
        }
        return map;
      }
    };
    sfa.setComplementThreshold(StandardFacetsAccumulator.DISABLE_COMPLEMENT);
    sfa.accumulate(ScoredDocIdsUtils.createAllDocsScoredDocIDs(indexReader));
    return new TotalFacetCounts(taxonomy, facetIndexingParams, counts, CreationType.Computed);
  }
  
}
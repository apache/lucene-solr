package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.index.IndexReader;

import org.apache.lucene.facet.search.aggregator.Aggregator;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.IntermediateFacetResult;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.util.PartitionsUtils;
import org.apache.lucene.facet.util.ScoredDocIdsUtils;

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
 * Standard implementation for {@link FacetsAccumulator}, utilizing partitions to save on memory.
 * <p>
 * Why partitions? Because if there are say 100M categories out of which 
 * only top K are required, we must first compute value for all 100M categories
 * (going over all documents) and only then could we select top K. 
 * This is made easier on memory by working in partitions of distinct categories: 
 * Once a values for a partition are found, we take the top K for that 
 * partition and work on the next partition, them merge the top K of both, 
 * and so forth, thereby computing top K with RAM needs for the size of 
 * a single partition rather than for the size of all the 100M categories.
 * <p>
 * Decision on partitions size is done at indexing time, and the facet information
 * for each partition is maintained separately.
 * <p>
 * <u>Implementation detail:</u> Since facets information of each partition is 
 * maintained in a separate "category list", we can be more efficient
 * at search time, because only the facet info for a single partition 
 * need to be read while processing that partition. 
 * 
 * @lucene.experimental
 */
public class StandardFacetsAccumulator extends FacetsAccumulator {

  private static final Logger logger = Logger.getLogger(StandardFacetsAccumulator.class.getName());

  protected final IntArrayAllocator intArrayAllocator;
  protected final FloatArrayAllocator floatArrayAllocator;

  protected int partitionSize;
  protected int maxPartitions;
  protected boolean isUsingComplements;

  private TotalFacetCounts totalFacetCounts;

  private Object accumulateGuard;

  public StandardFacetsAccumulator(FacetSearchParams searchParams, IndexReader indexReader,
      TaxonomyReader taxonomyReader, IntArrayAllocator intArrayAllocator,
      FloatArrayAllocator floatArrayAllocator) {
    
    super(searchParams,indexReader,taxonomyReader);
    int realPartitionSize = intArrayAllocator == null || floatArrayAllocator == null 
              ? PartitionsUtils.partitionSize(searchParams, taxonomyReader) : -1; // -1 if not needed.
    this.intArrayAllocator = intArrayAllocator != null 
        ? intArrayAllocator
        // create a default one if null was provided
        : new IntArrayAllocator(realPartitionSize, 1);
    this.floatArrayAllocator = floatArrayAllocator != null 
        ? floatArrayAllocator
        // create a default one if null provided
        : new FloatArrayAllocator(realPartitionSize, 1);
    // can only be computed later when docids size is known
    isUsingComplements = false;
    partitionSize = PartitionsUtils.partitionSize(searchParams, taxonomyReader);
    maxPartitions = (int) Math.ceil(this.taxonomyReader.getSize() / (double) partitionSize);
    accumulateGuard = new Object();
  }

  public StandardFacetsAccumulator(FacetSearchParams searchParams, IndexReader indexReader,
      TaxonomyReader taxonomyReader) {
    
    this(searchParams, indexReader, taxonomyReader, null, null);
  }

  @Override
  public List<FacetResult> accumulate(ScoredDocIDs docids) throws IOException {

    // synchronize to prevent calling two accumulate()'s at the same time.
    // We decided not to synchronize the method because that might mislead
    // users to feel encouraged to call this method simultaneously.
    synchronized (accumulateGuard) {

      // only now we can compute this
      isUsingComplements = shouldComplement(docids);

      if (isUsingComplements) {
        try {
          totalFacetCounts = TotalFacetCountsCache.getSingleton()
            .getTotalCounts(indexReader, taxonomyReader,
                searchParams.getFacetIndexingParams(), searchParams.getClCache());
          if (totalFacetCounts != null) {
            docids = ScoredDocIdsUtils.getComplementSet(docids, indexReader);
          } else {
            isUsingComplements = false;
          }
        } catch (UnsupportedOperationException e) {
          // TODO (Facet): this exception is thrown from TotalCountsKey if the
          // IndexReader used does not support getVersion(). We should re-think
          // this: is this tiny detail worth disabling total counts completely
          // for such readers? Currently, it's not supported by Parallel and
          // MultiReader, which might be problematic for several applications.
          // We could, for example, base our "isCurrent" logic on something else
          // than the reader's version. Need to think more deeply about it.
          if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, "IndexReader used does not support completents: ", e);
          }
          isUsingComplements = false;
        } catch (IOException e) {
          if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, "Failed to load/calculate total counts (complement counting disabled): ", e);
          }
          // silently fail if for some reason failed to load/save from/to dir 
          isUsingComplements = false;
        } catch (Exception e) {
          // give up: this should not happen!
          IOException ioEx = new IOException(
              "PANIC: Got unexpected exception while trying to get/calculate total counts: "
              +e.getMessage());
          ioEx.initCause(e);
          throw ioEx;
        }
      }

      docids = actualDocsToAccumulate(docids);

      FacetArrays facetArrays = new FacetArrays(intArrayAllocator, floatArrayAllocator);

      HashMap<FacetRequest, IntermediateFacetResult> fr2tmpRes = new HashMap<FacetRequest, IntermediateFacetResult>();

      try {
        for (int part = 0; part < maxPartitions; part++) {

          // fill arrays from category lists
          fillArraysForPartition(docids, facetArrays, part);

          int offset = part * partitionSize;

          // for each partition we go over all requests and handle
          // each, where
          // the request maintains the merged result.
          // In this implementation merges happen after each
          // partition,
          // but other impl could merge only at the end.
          for (FacetRequest fr : searchParams.getFacetRequests()) {
            FacetResultsHandler frHndlr = fr.createFacetResultsHandler(taxonomyReader);
            IntermediateFacetResult res4fr = frHndlr.fetchPartitionResult(facetArrays, offset);
            IntermediateFacetResult oldRes = fr2tmpRes.get(fr);
            if (oldRes != null) {
              res4fr = frHndlr.mergeResults(oldRes, res4fr);
            }
            fr2tmpRes.put(fr, res4fr);
          }
        }
      } finally {
        facetArrays.free();
      }

      // gather results from all requests into a list for returning them
      List<FacetResult> res = new ArrayList<FacetResult>();
      for (FacetRequest fr : searchParams.getFacetRequests()) {
        FacetResultsHandler frHndlr = fr.createFacetResultsHandler(taxonomyReader);
        IntermediateFacetResult tmpResult = fr2tmpRes.get(fr); 
        if (tmpResult == null) {
          continue; // do not add a null to the list.
        }
        FacetResult facetRes = frHndlr.renderFacetResult(tmpResult); 
        // final labeling if allowed (because labeling is a costly operation)
        if (isAllowLabeling()) {
          frHndlr.labelResult(facetRes);
        }
        res.add(facetRes);
      }

      return res;
    }
  }

  /**
   * Set the actual set of documents over which accumulation should take place.
   * <p>
   * Allows to override the set of documents to accumulate for. Invoked just
   * before actual accumulating starts. From this point that set of documents
   * remains unmodified. Default implementation just returns the input
   * unchanged.
   * 
   * @param docids
   *          candidate documents to accumulate for
   * @return actual documents to accumulate for
   */
  protected ScoredDocIDs actualDocsToAccumulate(ScoredDocIDs docids) throws IOException {
    return docids;
  }

  /** Check if it is worth to use complements */
  protected boolean shouldComplement(ScoredDocIDs docids) {
    return 
      mayComplement() && 
      (docids.size() > indexReader.numDocs() * getComplementThreshold()) ;
  }

  /**
   * Iterate over the documents for this partition and fill the facet arrays with the correct
   * count/complement count/value.
   * @throws IOException If there is a low-level I/O error.
   */
  private final void fillArraysForPartition(ScoredDocIDs docids,
      FacetArrays facetArrays, int partition) throws IOException {
    
    if (isUsingComplements) {
      initArraysByTotalCounts(facetArrays, partition, docids.size());
    } else {
      facetArrays.free(); // to get a cleared array for this partition
    }

    HashMap<CategoryListIterator, Aggregator> categoryLists = getCategoryListMap(
        facetArrays, partition);

    for (Entry<CategoryListIterator, Aggregator> entry : categoryLists.entrySet()) {
      CategoryListIterator categoryList = entry.getKey();
      if (!categoryList.init()) {
        continue;
      }

      Aggregator categorator = entry.getValue();
      ScoredDocIDsIterator iterator = docids.iterator();
      while (iterator.next()) {
        int docID = iterator.getDocID();
        if (!categoryList.skipTo(docID)) {
          continue;
        }
        categorator.setNextDoc(docID, iterator.getScore());
        long ordinal;
        while ((ordinal = categoryList.nextCategory()) <= Integer.MAX_VALUE) {
          categorator.aggregate((int) ordinal);
        }
      }
    }
  }

  /**
   * Init arrays for partition by total counts, optionally applying a factor
   */
  private final void initArraysByTotalCounts(FacetArrays facetArrays, int partition, int nAccumulatedDocs) {
    int[] intArray = facetArrays.getIntArray();
    totalFacetCounts.fillTotalCountsForPartition(intArray, partition);
    double totalCountsFactor = getTotalCountsFactor();
    // fix total counts, but only if the effect of this would be meaningfull. 
    if (totalCountsFactor < 0.99999) {
      int delta = nAccumulatedDocs + 1;
      for (int i = 0; i < intArray.length; i++) {
        intArray[i] *= totalCountsFactor;
        // also translate to prevent loss of non-positive values
        // due to complement sampling (ie if sampled docs all decremented a certain category). 
        intArray[i] += delta; 
      }
    }
  }

  /**
   * Expert: factor by which counts should be multiplied when initializing
   * the count arrays from total counts.
   * Default implementation for this returns 1, which is a no op.  
   * @return a factor by which total counts should be multiplied
   */
  protected double getTotalCountsFactor() {
    return 1;
  }

  /**
   * Create an {@link Aggregator} and a {@link CategoryListIterator} for each
   * and every {@link FacetRequest}. Generating a map, matching each
   * categoryListIterator to its matching aggregator.
   * <p>
   * If two CategoryListIterators are served by the same aggregator, a single
   * aggregator is returned for both.
   * 
   * <b>NOTE: </b>If a given category list iterator is needed with two different
   * aggregators (e.g counting and association) - an exception is thrown as this
   * functionality is not supported at this time.
   */
  protected HashMap<CategoryListIterator, Aggregator> getCategoryListMap(FacetArrays facetArrays,
      int partition) throws IOException {
    
    HashMap<CategoryListIterator, Aggregator> categoryLists = new HashMap<CategoryListIterator, Aggregator>();

    for (FacetRequest facetRequest : searchParams.getFacetRequests()) {
      Aggregator categoryAggregator = facetRequest.createAggregator(
          isUsingComplements, facetArrays, indexReader,  taxonomyReader);

      CategoryListIterator cli = 
        facetRequest.createCategoryListIterator(indexReader, taxonomyReader, searchParams, partition);
      
      // get the aggregator
      Aggregator old = categoryLists.put(cli, categoryAggregator);

      if (old != null && !old.equals(categoryAggregator)) {
        // TODO (Facet): create a more meaningful RE class, and throw it.
        throw new RuntimeException(
        "Overriding existing category list with different aggregator. THAT'S A NO NO!");
      }
      // if the aggregator is the same we're covered
    }

    return categoryLists;
  }
}
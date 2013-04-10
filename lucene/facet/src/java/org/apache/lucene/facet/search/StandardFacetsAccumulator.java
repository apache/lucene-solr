package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.facet.complements.TotalFacetCounts;
import org.apache.lucene.facet.complements.TotalFacetCountsCache;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.partitions.IntermediateFacetResult;
import org.apache.lucene.facet.partitions.PartitionsFacetResultsHandler;
import org.apache.lucene.facet.search.FacetRequest.ResultMode;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.util.PartitionsUtils;
import org.apache.lucene.facet.util.ScoredDocIdsUtils;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.IntsRef;

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

  /**
   * Default threshold for using the complements optimization.
   * If accumulating facets for a document set larger than this ratio of the index size than 
   * perform the complement optimization.
   * @see #setComplementThreshold(double) for more info on the complements optimization.  
   */
  public static final double DEFAULT_COMPLEMENT_THRESHOLD = 0.6;

  /**
   * Passing this to {@link #setComplementThreshold(double)} will disable using complement optimization.
   */
  public static final double DISABLE_COMPLEMENT = Double.POSITIVE_INFINITY; // > 1 actually

  /**
   * Passing this to {@link #setComplementThreshold(double)} will force using complement optimization.
   */
  public static final double FORCE_COMPLEMENT = 0; // <=0  

  protected int partitionSize;
  protected int maxPartitions;
  protected boolean isUsingComplements;

  private TotalFacetCounts totalFacetCounts;

  private Object accumulateGuard;

  private double complementThreshold;
  
  public StandardFacetsAccumulator(FacetSearchParams searchParams, IndexReader indexReader, 
      TaxonomyReader taxonomyReader) {
    this(searchParams, indexReader, taxonomyReader, new FacetArrays(
        PartitionsUtils.partitionSize(searchParams.indexingParams, taxonomyReader)));
  }

  public StandardFacetsAccumulator(FacetSearchParams searchParams, IndexReader indexReader,
      TaxonomyReader taxonomyReader, FacetArrays facetArrays) {
    super(searchParams, indexReader, taxonomyReader, facetArrays);
    
    // can only be computed later when docids size is known
    isUsingComplements = false;
    partitionSize = PartitionsUtils.partitionSize(searchParams.indexingParams, taxonomyReader);
    maxPartitions = (int) Math.ceil(this.taxonomyReader.getSize() / (double) partitionSize);
    accumulateGuard = new Object();
  }

  // TODO: this should be removed once we clean the API
  public List<FacetResult> accumulate(ScoredDocIDs docids) throws IOException {

    // synchronize to prevent calling two accumulate()'s at the same time.
    // We decided not to synchronize the method because that might mislead
    // users to feel encouraged to call this method simultaneously.
    synchronized (accumulateGuard) {

      // only now we can compute this
      isUsingComplements = shouldComplement(docids);

      if (isUsingComplements) {
        try {
          totalFacetCounts = TotalFacetCountsCache.getSingleton().getTotalCounts(indexReader, taxonomyReader, 
              searchParams.indexingParams);
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
          throw new IOException("PANIC: Got unexpected exception while trying to get/calculate total counts", e);
        }
      }

      docids = actualDocsToAccumulate(docids);

      HashMap<FacetRequest, IntermediateFacetResult> fr2tmpRes = new HashMap<FacetRequest, IntermediateFacetResult>();

      try {
        for (int part = 0; part < maxPartitions; part++) {

          // fill arrays from category lists
          fillArraysForPartition(docids, facetArrays, part);

          int offset = part * partitionSize;

          // for each partition we go over all requests and handle
          // each, where the request maintains the merged result.
          // In this implementation merges happen after each partition,
          // but other impl could merge only at the end.
          final HashSet<FacetRequest> handledRequests = new HashSet<FacetRequest>();
          for (FacetRequest fr : searchParams.facetRequests) {
            // Handle and merge only facet requests which were not already handled.  
            if (handledRequests.add(fr)) {
              PartitionsFacetResultsHandler frHndlr = createFacetResultsHandler(fr);
              IntermediateFacetResult res4fr = frHndlr.fetchPartitionResult(offset);
              IntermediateFacetResult oldRes = fr2tmpRes.get(fr);
              if (oldRes != null) {
                res4fr = frHndlr.mergeResults(oldRes, res4fr);
              }
              fr2tmpRes.put(fr, res4fr);
            } 
          }
        }
      } finally {
        facetArrays.free();
      }

      // gather results from all requests into a list for returning them
      List<FacetResult> res = new ArrayList<FacetResult>();
      for (FacetRequest fr : searchParams.facetRequests) {
        PartitionsFacetResultsHandler frHndlr = createFacetResultsHandler(fr);
        IntermediateFacetResult tmpResult = fr2tmpRes.get(fr);
        if (tmpResult == null) {
          // Add empty FacetResult:
          res.add(emptyResult(taxonomyReader.getOrdinal(fr.categoryPath), fr));
          continue;
        }
        FacetResult facetRes = frHndlr.renderFacetResult(tmpResult);
        // final labeling if allowed (because labeling is a costly operation)
        frHndlr.labelResult(facetRes);
        res.add(facetRes);
      }

      return res;
    }
  }

  /** check if all requests are complementable */
  protected boolean mayComplement() {
    for (FacetRequest freq : searchParams.facetRequests) {
      if (!(freq instanceof CountFacetRequest)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected PartitionsFacetResultsHandler createFacetResultsHandler(FacetRequest fr) {
    if (fr.getResultMode() == ResultMode.PER_NODE_IN_TREE) {
      return new TopKInEachNodeHandler(taxonomyReader, fr, facetArrays);
    } else {
      return new TopKFacetResultsHandler(taxonomyReader, fr, facetArrays);
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
    return mayComplement() && (docids.size() > indexReader.numDocs() * getComplementThreshold()) ;
  }

  /**
   * Iterate over the documents for this partition and fill the facet arrays with the correct
   * count/complement count/value.
   */
  private final void fillArraysForPartition(ScoredDocIDs docids, FacetArrays facetArrays, int partition) 
      throws IOException {
    
    if (isUsingComplements) {
      initArraysByTotalCounts(facetArrays, partition, docids.size());
    } else {
      facetArrays.free(); // to get a cleared array for this partition
    }

    HashMap<CategoryListIterator, Aggregator> categoryLists = getCategoryListMap(facetArrays, partition);

    IntsRef ordinals = new IntsRef(32); // a reasonable start capacity for most common apps
    for (Entry<CategoryListIterator, Aggregator> entry : categoryLists.entrySet()) {
      final ScoredDocIDsIterator iterator = docids.iterator();
      final CategoryListIterator categoryListIter = entry.getKey();
      final Aggregator aggregator = entry.getValue();
      Iterator<AtomicReaderContext> contexts = indexReader.leaves().iterator();
      AtomicReaderContext current = null;
      int maxDoc = -1;
      while (iterator.next()) {
        int docID = iterator.getDocID();
        if (docID >= maxDoc) {
          boolean iteratorDone = false;
          do { // find the segment which contains this document
            if (!contexts.hasNext()) {
              throw new RuntimeException("ScoredDocIDs contains documents outside this reader's segments !?");
            }
            current = contexts.next();
            maxDoc = current.docBase + current.reader().maxDoc();
            if (docID < maxDoc) { // segment has docs, check if it has categories
              boolean validSegment = categoryListIter.setNextReader(current);
              validSegment &= aggregator.setNextReader(current);
              if (!validSegment) { // if categoryList or aggregtor say it's an invalid segment, skip all docs
                while (docID < maxDoc && iterator.next()) {
                  docID = iterator.getDocID();
                }
                if (docID < maxDoc) {
                  iteratorDone = true;
                }
              }
            }
          } while (docID >= maxDoc);
          if (iteratorDone) { // iterator finished, terminate the loop
            break;
          }
        }
        docID -= current.docBase;
        categoryListIter.getOrdinals(docID, ordinals);
        if (ordinals.length == 0) {
          continue; // document does not have category ordinals
        }
        aggregator.aggregate(docID, iterator.getScore(), ordinals);
      }
    }
  }

  /** Init arrays for partition by total counts, optionally applying a factor */
  private final void initArraysByTotalCounts(FacetArrays facetArrays, int partition, int nAccumulatedDocs) {
    int[] intArray = facetArrays.getIntArray();
    totalFacetCounts.fillTotalCountsForPartition(intArray, partition);
    double totalCountsFactor = getTotalCountsFactor();
    // fix total counts, but only if the effect of this would be meaningful. 
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

    FacetIndexingParams indexingParams = searchParams.indexingParams;
    for (FacetRequest facetRequest : searchParams.facetRequests) {
      Aggregator categoryAggregator = facetRequest.createAggregator(isUsingComplements, facetArrays, taxonomyReader);

      CategoryListIterator cli = indexingParams.getCategoryListParams(facetRequest.categoryPath).createCategoryListIterator(partition);
      
      // get the aggregator
      Aggregator old = categoryLists.put(cli, categoryAggregator);

      if (old != null && !old.equals(categoryAggregator)) {
        throw new RuntimeException("Overriding existing category list with different aggregator");
      }
      // if the aggregator is the same we're covered
    }

    return categoryLists;
  }
  
  @Override
  public List<FacetResult> accumulate(List<MatchingDocs> matchingDocs) throws IOException {
    return accumulate(new MatchingDocsAsScoredDocIDs(matchingDocs));
  }

  /**
   * Returns the complement threshold.
   * @see #setComplementThreshold(double)
   */
  public double getComplementThreshold() {
    return complementThreshold;
  }

  /**
   * Set the complement threshold.
   * This threshold will dictate whether the complements optimization is applied.
   * The optimization is to count for less documents. It is useful when the same 
   * FacetSearchParams are used for varying sets of documents. The first time 
   * complements is used the "total counts" are computed - counting for all the 
   * documents in the collection. Then, only the complementing set of documents
   * is considered, and used to decrement from the overall counts, thereby 
   * walking through less documents, which is faster.
   * <p>
   * For the default settings see {@link #DEFAULT_COMPLEMENT_THRESHOLD}.
   * <p>
   * To forcing complements in all cases pass {@link #FORCE_COMPLEMENT}.
   * This is mostly useful for testing purposes, as forcing complements when only 
   * tiny fraction of available documents match the query does not make sense and 
   * would incur performance degradations.
   * <p>
   * To disable complements pass {@link #DISABLE_COMPLEMENT}.
   * @param complementThreshold the complement threshold to set
   * @see #getComplementThreshold()
   */
  public void setComplementThreshold(double complementThreshold) {
    this.complementThreshold = complementThreshold;
  }

  /** Returns true if complements are enabled. */
  public boolean isUsingComplements() {
    return isUsingComplements;
  }
  
}

package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.IndexReader;

import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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
 * Driver for Accumulating facets of faceted search requests over given
 * documents.
 * 
 * @lucene.experimental
 */
public abstract class FacetsAccumulator {

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

  private double complementThreshold = DEFAULT_COMPLEMENT_THRESHOLD;  

  protected final TaxonomyReader taxonomyReader;
  protected final IndexReader indexReader;
  protected FacetSearchParams searchParams;

  private boolean allowLabeling = true;

  public FacetsAccumulator(FacetSearchParams searchParams,
                            IndexReader indexReader,
                            TaxonomyReader taxonomyReader) {
    this.indexReader = indexReader;
    this.taxonomyReader = taxonomyReader;
    this.searchParams = searchParams;
  }

  /**
   * Accumulate facets over given documents, according to facet requests in effect.
   * @param docids documents (and their scores) for which facets are Accumulated.
   * @return Accumulated facets.  
   * @throws IOException on error.
   */
  // internal API note: it was considered to move the docids into the constructor as well, 
  // but this prevents nice extension capabilities, especially in the way that 
  // Sampling Accumulator works with the (any) delegated accumulator.
  public abstract List<FacetResult> accumulate(ScoredDocIDs docids) throws IOException;

  /**
   * @return the complement threshold
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
   */
  public void setComplementThreshold(double complementThreshold) {
    this.complementThreshold = complementThreshold;
  }

  /**
   * Check if labeling is allowed for this accumulator.
   * <p>
   * By default labeling is allowed.
   * This allows one accumulator to invoke other accumulators for accumulation
   * but keep to itself the responsibility of labeling.
   * This might br handy since labeling is a costly operation. 
   * @return true of labeling is allowed for this accumulator
   * @see #setAllowLabeling(boolean)
   */
  protected boolean isAllowLabeling() {
    return allowLabeling;
  }

  /**
   * Set whether labeling is allowed for this accumulator.
   * @param allowLabeling new setting for allow labeling
   * @see #isAllowLabeling()
   */
  protected void setAllowLabeling(boolean allowLabeling) {
    this.allowLabeling = allowLabeling;
  }

  /** check if all requests are complementable */
  protected boolean mayComplement() {
    for (FacetRequest freq:searchParams.getFacetRequests()) {
      if (!freq.supportsComplements()) {
        return false;
      }
    }
    return true;
  }
}
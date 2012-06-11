package org.apache.lucene.facet.search.results;

import java.io.IOException;

import org.apache.lucene.facet.search.FacetResultsHandler;
import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.sampling.SampleFixer;
import org.apache.lucene.facet.taxonomy.CategoryPath;
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
 * Result of faceted search for a certain taxonomy node.
 * 
 * @lucene.experimental
 */
public interface FacetResultNode {

  /**
   * String representation of this facet result node.
   * Use with caution: might return a very long string.
   * @param prefix prefix for each result line
   */
  public String toString(String prefix);

  /**
   * Ordinal of the category of this result.
   */
  public int getOrdinal();

  /**
   * Category path of the category of this result, or null if not computed, 
   * because the application did not request to compute it. 
   * To force computing the label in case not yet computed use
   * {@link #getLabel(TaxonomyReader)}.
   * @see FacetRequest#getNumLabel()
   * @see #getLabel(TaxonomyReader)
   */
  public CategoryPath getLabel();

  /**
   * Category path of the category of this result.
   * If not already computed, will be computed now. 
   * <p> 
   * Use with <b>caution</b>: loading a label for results is costly, performance wise.
   * Therefore force labels loading only when really needed.   
   * @param taxonomyReader taxonomy reader for forcing (lazy) labeling of this result. 
   * @throws IOException on error
   * @see FacetRequest#getNumLabel()
   */
  public CategoryPath getLabel(TaxonomyReader taxonomyReader) throws IOException;

  /**
   * Value of this result - usually either count or a value derived from some
   * computing on the association of it.
   */
  public double getValue();

  /**
   * Value of screened out sub results.
   * <p>
   * If only part of valid results are returned, e.g. because top K were requested,
   * provide info on "what else is there under this result node".
   */
  public double getResidue();

  /**
   * Contained sub results.
   * These are either child facets, if a tree result was requested, or simply descendants, in case
   * tree result was not requested. In the first case, all returned are both descendants of 
   * this node in the taxonomy and siblings of each other in the taxonomy.
   * In the latter case they are only guaranteed to be descendants of 
   * this node in the taxonomy.  
   */
  public Iterable<? extends FacetResultNode> getSubResults();

  /**
   * Number of sub results
   */
  public int getNumSubResults();

  /**
   * Expert: Set a new value for this result node.
   * <p>
   * Allows to modify the value of this facet node. 
   * Used for example to tune a sampled value, e.g. by 
   * {@link SampleFixer#fixResult(org.apache.lucene.facet.search.ScoredDocIDs, FacetResult)}  
   * @param value the new value to set
   * @see #getValue()
   * @see FacetResultsHandler#rearrangeFacetResult(FacetResult)
   */
  public void setValue(double value);

}
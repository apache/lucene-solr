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
package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.FacetComponent.FacetBase;

/**
 * Models a single instance of a "pivot" specified by a {@link FacetParams#FACET_PIVOT} 
 * param, which may contain multiple nested fields.
 *
 * This class is also used to coordinate the refinement requests needed from various 
 * shards when doing processing a distributed request
 */
public class PivotFacet extends FacetBase {

  /** 
   * Local param used to indicate that refinements are required on a pivot. Should
   * also be used as the prefix for concatenating with the value to determine the
   * name of the multi-valued param that will contain all of the values needed for 
   * refinement.
   */
  public static final String REFINE_PARAM = "fpt";
  
  // TODO: is this really needed? can't we just loop over 0<=i<rb.shards.length ?
  public final BitSet knownShards = new BitSet();
  
  private final Map<Integer, List<PivotFacetValue>> queuedRefinements = new HashMap<>();
  
  // if null, then either we haven't collected any responses from shards
  // or all the shards that have responded so far haven't had any values for the top
  // field of this pivot.  May be null forever if no doc in any shard has a value 
  // for the top field of the pivot
  private PivotFacetField pivotFacetField;
  
  public PivotFacet(ResponseBuilder rb, String facetStr) {
    super(rb, FacetParams.FACET_PIVOT, facetStr);
  }
  
  /**
   * Tracks that the specified shard needs to be asked to refine the specified 
   * {@link PivotFacetValue} 
   * 
   * @see #getQueuedRefinements
   */
  public void addRefinement(int shardNumber, PivotFacetValue value) {
    
    if (!queuedRefinements.containsKey(shardNumber)) {
      queuedRefinements.put(shardNumber, new ArrayList<PivotFacetValue>());
    }
    
    queuedRefinements.get(shardNumber).add(value);
  }
  
  /**
   * An immutable List of the {@link PivotFacetValue}s that need to be
   * refined for this pivot.  Once these refinements have been processed, 
   * the caller should clear them using {@link #removeAllRefinementsForShard}
   *
   * @see #addRefinement
   * @see #removeAllRefinementsForShard
   * @return a list of the values to refine, or an empty list.
   */
  public List<PivotFacetValue> getQueuedRefinements(int shardNumber) {
    List<PivotFacetValue> raw = queuedRefinements.get(shardNumber);
    if (null == raw) {
      raw = Collections.<PivotFacetValue>emptyList();
    }
    return Collections.unmodifiableList(raw);
  }

  /**
   * Clears the list of queued refinements for the specified shard
   *
   * @see #addRefinement
   * @see #getQueuedRefinements
   */
  public void removeAllRefinementsForShard(int shardNumber) {
    queuedRefinements.remove(shardNumber);
  }
  
  /**
   * If true, then additional refinement requests are needed to flesh out the correct
   * counts for this Pivot
   *
   * @see #getQueuedRefinements
   */
  public boolean isRefinementsRequired() {
    return ! queuedRefinements.isEmpty();
  }
  
  /** 
   * A recursive method for generating <code>NamedLists</code> for this pivot
   * suitable for including in a pivot facet response to the original distributed request.
   *
   * @see PivotFacetField#trim
   * @see PivotFacetField#convertToListOfNamedLists
   */
  public List<NamedList<Object>> getTrimmedPivotsAsListOfNamedLists() {
    if (null == pivotFacetField) {
      // no values in any shard for the top field of this pivot
      return Collections.<NamedList<Object>>emptyList();
    }

    pivotFacetField.trim();
    return pivotFacetField.convertToListOfNamedLists();
  }  

  /** 
   * A recursive method for determining which {@link PivotFacetValue}s need to be
   * refined for this pivot.
   *
   * @see PivotFacetField#queuePivotRefinementRequests
   */
  public void queuePivotRefinementRequests() {
    if (null == pivotFacetField) return; // NOOP

    pivotFacetField.sort();
    pivotFacetField.queuePivotRefinementRequests(this);
  }
  
  /**
   * Recursively merges the response from the specified shard, tracking the known shards.
   * 
   * @see PivotFacetField#contributeFromShard
   * @see PivotFacetField#createFromListOfNamedLists
   */
  public void mergeResponseFromShard(int shardNumber, ResponseBuilder rb, List<NamedList<Object>> response) {
    
    knownShards.set(shardNumber);
    if (pivotFacetField == null) {
      pivotFacetField = PivotFacetField.createFromListOfNamedLists(shardNumber, rb,  null,  response);
    } else {
      pivotFacetField.contributeFromShard(shardNumber, rb, response);
    }
  }

  public String toString() {
    return "[" + facetStr + "] | " + this.getKey();
  }
}

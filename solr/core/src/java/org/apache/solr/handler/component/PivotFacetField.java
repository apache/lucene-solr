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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;


/**
 * Models a single field somewhere in a hierarchy of fields as part of a pivot facet.  
 * This pivot field contains {@link PivotFacetValue}s which may each contain a nested
 * {@link PivotFacetField} child.  This <code>PivotFacetField</code> may itself 
 * be a child of a {@link PivotFacetValue} parent.
 *
 * @see PivotFacetValue
 * @see PivotFacetFieldValueCollection
 */
@SuppressWarnings("rawtypes")
public class PivotFacetField {
  
  public final String field;

  // null if this is a top level pivot, 
  // otherwise the value of the parent pivot we are nested under
  public final PivotFacetValue parentValue;

  public final PivotFacetFieldValueCollection valueCollection;
  
  // Facet parameters relating to this field
  private final int facetFieldLimit;
  private final int facetFieldMinimumCount;
  private final int facetFieldOffset;  
  private final String facetFieldSort;

  private final Map<Integer, Integer> numberOfValuesContributedByShard = new HashMap<>();
  private final Map<Integer, Integer> shardLowestCount = new HashMap<>();

  private boolean needRefinementAtThisLevel = true;
    
  private PivotFacetField(ResponseBuilder rb, PivotFacetValue parent, String fieldName) {
    
    field = fieldName;
    parentValue = parent;
    
    // facet params
    SolrParams parameters = rb.req.getParams();
    facetFieldMinimumCount = parameters.getFieldInt(field, FacetParams.FACET_PIVOT_MINCOUNT, 1);
    facetFieldOffset = parameters.getFieldInt(field, FacetParams.FACET_OFFSET, 0);
    facetFieldLimit = parameters.getFieldInt(field, FacetParams.FACET_LIMIT, 100);
    String defaultSort = (facetFieldLimit > 0) ? FacetParams.FACET_SORT_COUNT : FacetParams.FACET_SORT_INDEX;
    facetFieldSort = parameters.getFieldParam(field, FacetParams.FACET_SORT, defaultSort);

    valueCollection = new PivotFacetFieldValueCollection(facetFieldMinimumCount, facetFieldOffset, facetFieldLimit, facetFieldSort);
    
    if ( (facetFieldLimit < 0) || 
         // TODO: possible refinement issue if limit=0 & mincount=0 & missing=true
         // (ie: we only want the missing count for this field)
         (facetFieldLimit <= 0 && facetFieldMinimumCount == 0) ||
         (facetFieldSort.equals(FacetParams.FACET_SORT_INDEX) && facetFieldMinimumCount <= 0) 
         ) {
      // in any of these cases, there's no need to refine this level of the pivot
      needRefinementAtThisLevel = false;
    }
  }

  /** 
   * A recursive method that walks up the tree of pivot fields/values to build 
   * a list of String representations of the values that lead down to this 
   * PivotFacetField.
   *
   * @return A mutable List of the pivot values leading down to this pivot field, 
   *      will never be null but may contain nulls and may be empty if this is a top 
   *      level pivot field
   * @see PivotFacetValue#getValuePath
   */
  public List<String> getValuePath() {
    if (null != parentValue) {
      return parentValue.getValuePath();
    }
    return new ArrayList<String>(3);
  }

  /**
   * A recursive method to construct a new <code>PivotFacetField</code> object from 
   * the contents of the {@link NamedList}s provided by the specified shard, relative 
   * to a parent value (if this is not the top field in the pivot hierarchy)
   *
   * The associated child {@link PivotFacetValue}s will be recursively built as well.
   *
   * @see PivotFacetValue#createFromNamedList
   * @param shardNumber the id of the shard that provided this data
   * @param rb The response builder of the current request
   * @param owner the parent value in the current pivot (may be null)
   * @param pivotValues the data from the specified shard for this pivot field, may be null or empty
   * @return the new PivotFacetField, null if pivotValues is null or empty.
   */
  public static PivotFacetField createFromListOfNamedLists(int shardNumber, ResponseBuilder rb, PivotFacetValue owner, List<NamedList<Object>> pivotValues) {
    
    if (null == pivotValues || pivotValues.size() <= 0) return null;
    
    NamedList<Object> firstValue = pivotValues.get(0);
    PivotFacetField createdPivotFacetField 
      = new PivotFacetField(rb, owner, PivotFacetHelper.getField(firstValue));
    
    int lowestCount = Integer.MAX_VALUE;
    
    for (NamedList<Object> pivotValue : pivotValues) {
      
      lowestCount = Math.min(lowestCount, PivotFacetHelper.getCount(pivotValue));
      
      PivotFacetValue newValue = PivotFacetValue.createFromNamedList
        (shardNumber, rb, createdPivotFacetField, pivotValue);
      createdPivotFacetField.valueCollection.add(newValue);
    }
      
    createdPivotFacetField.shardLowestCount.put(shardNumber,  lowestCount);
    createdPivotFacetField.numberOfValuesContributedByShard.put(shardNumber, pivotValues.size());

    return createdPivotFacetField;
  }
  
  /**
   * Destructive method that recursively prunes values from the data structure 
   * based on the counts for those values and the effective sort, mincount, limit, 
   * and offset being used for each field.
   * <p>
   * This method should only be called after all refinement is completed just prior 
   * calling {@link #convertToListOfNamedLists}
   * </p>
   *
   * @see PivotFacet#getTrimmedPivotsAsListOfNamedLists
   * @see PivotFacetFieldValueCollection#trim
   */
  public void trim() {
    // SOLR-6331...
    //
    // we can probably optimize the memory usage by trimming each level of the pivot once
    // we know we've fully refined the values at that level 
    // (ie: fold this logic into refineNextLevelOfFacets)
    this.valueCollection.trim();
  }
  
  /**
   * Recursively sorts the collection of values associated with this field, and 
   * any sub-pivots those values have.
   *
   * @see FacetParams#FACET_SORT
   * @see PivotFacetFieldValueCollection#sort
   */
  public void sort() {
    this.valueCollection.sort();
  }
  
  /** 
   * A recursive method for generating <code>NamedLists</code> from this field 
   * suitable for including in a pivot facet response to the original distributed request.
   */
  public List<NamedList<Object>> convertToListOfNamedLists() { 
    
    List<NamedList<Object>> convertedPivotList = null;
    
    if (valueCollection.size() > 0) {
      convertedPivotList = new LinkedList<>();
      for (PivotFacetValue pivot : valueCollection)
        convertedPivotList.add(pivot.convertToNamedList());
    }
  
    return convertedPivotList;
  }     

  /** 
   * A recursive method for determining which {@link PivotFacetValue}s need to be
   * refined for this pivot.
   *
   * @see PivotFacet#queuePivotRefinementRequests
   */
  public void queuePivotRefinementRequests(PivotFacet pf) {
    
    if (needRefinementAtThisLevel) {

      if (0 < facetFieldMinimumCount) {
        // missing is always a candidate for refinement if at least one shard met the minimum
        PivotFacetValue missing = valueCollection.getMissingValue();
        if (null != missing) {
          processDefiniteCandidateElement(pf, valueCollection.getMissingValue());
        }
      }

      if (! valueCollection.getExplicitValuesList().isEmpty()) {

        if (FacetParams.FACET_SORT_COUNT.equals(facetFieldSort)) {
          // we only need to things that are currently in our limit,
          // or might be in our limit if we get increased counts from shards that
          // didn't include this value the first time
          final int indexOfCountThreshold 
            = Math.min(valueCollection.getExplicitValuesListSize(), 
                       facetFieldOffset + facetFieldLimit) - 1;
          final int countThreshold = valueCollection.getAt(indexOfCountThreshold).getCount();
          
          int positionInResults = 0;
          
          for (PivotFacetValue value : valueCollection.getExplicitValuesList()) {
            if (positionInResults <= indexOfCountThreshold) {
              // This element is within the top results, so we need to get information 
              // from all of the shards.
              processDefiniteCandidateElement(pf, value);
            } else {
              // This element is not within the top results, but may still need to be refined.
              processPossibleCandidateElement(pf, value, countThreshold);
            }
            
            positionInResults++;
          }
        } else { // FACET_SORT_INDEX
          // everything needs refined to see what the per-shard mincount excluded
          for (PivotFacetValue value : valueCollection.getExplicitValuesList()) {
            processDefiniteCandidateElement(pf, value);
          }
        }
      }

      needRefinementAtThisLevel = false;
    }
      
    if ( pf.isRefinementsRequired() ) {
      // if any refinements are needed, then we need to stop and wait to
      // see how the picture may change before drilling down to child pivot fields 
      return;
    } else {
      // Since outstanding requests have been filled, then we can drill down 
      // to the next deeper level and check it.
      refineNextLevelOfFacets(pf);
    }
  }
      
  /**
   * Adds refinement requests for the value for each shard that has not already contributed 
   * a count for this value.
   */
  private void processDefiniteCandidateElement(PivotFacet pf, PivotFacetValue value) {
    
    for (int shard = pf.knownShards.nextSetBit(0); 
         0 <= shard; 
         shard = pf.knownShards.nextSetBit(shard+1)) {   
      if ( ! value.shardHasContributed(shard) ) {
        if ( // if we're doing index order, we need to refine anything  
             // (mincount may have excluded from a shard)
            FacetParams.FACET_SORT_INDEX.equals(facetFieldSort)
            || (// 'missing' value isn't affected by limit, needs refined if shard didn't provide
                null == value.getValue() ||
                // if we are doing count order, we need to refine if the limit was hit
                // (if not, the shard doesn't have the value or it would have returned already)
                numberOfValuesContributedByShardWasLimitedByFacetFieldLimit(shard))) {
          pf.addRefinement(shard, value);
        }
      }
    }  
  }

  private boolean numberOfValuesContributedByShardWasLimitedByFacetFieldLimit(int shardNumber) {
    return facetFieldLimit <= numberOfValuesContributedByShard(shardNumber);
  }
  
  private int numberOfValuesContributedByShard(final int shardNumber) { 
    return numberOfValuesContributedByShard.containsKey(shardNumber)
      ? numberOfValuesContributedByShard.get(shardNumber) 
      : 0;
  }
  
  /** 
   * Checks the {@link #lowestCountContributedbyShard} for each shard, combined with the 
   * counts we already know, to see if this value is a viable candidate -- 
   * <b>Does not make sense when using {@link FacetParams#FACET_SORT_INDEX}</b>
   *
   * @see #processDefiniteCandidateElement
   */
  private void processPossibleCandidateElement(PivotFacet pf, PivotFacetValue value, 
                                               final int refinementThreshold) {
    
    assert FacetParams.FACET_SORT_COUNT.equals(facetFieldSort)
      : "Method only makes sense when sorting by count";

    int maxPossibleCountAfterRefinement = value.getCount();
    
    for (int shard = pf.knownShards.nextSetBit(0); 
         0 <= shard;
         shard = pf.knownShards.nextSetBit(shard+1)) {
      if ( ! value.shardHasContributed(shard) ) {
        maxPossibleCountAfterRefinement += lowestCountContributedbyShard(shard);
      }
    }
    
    if (refinementThreshold <= maxPossibleCountAfterRefinement) {
      processDefiniteCandidateElement(pf, value);
    }
  }
   
  private int lowestCountContributedbyShard(int shardNumber) {
    return (shardLowestCount.containsKey(shardNumber))
      ? shardLowestCount.get(shardNumber) 
      : 0;
  }
  
  private void refineNextLevelOfFacets(PivotFacet pf) {

    List<PivotFacetValue> explicitValsToRefine 
      = valueCollection.getNextLevelValuesToRefine();
    
    for (PivotFacetValue value : explicitValsToRefine) {
      if (null != value.getChildPivot()) {
        value.getChildPivot().queuePivotRefinementRequests(pf);
      }
    }

    PivotFacetValue missing = this.valueCollection.getMissingValue();
    if(null != missing && null != missing.getChildPivot()) {
      missing.getChildPivot().queuePivotRefinementRequests(pf);
    }
  }
  
  private void incrementShardValueCount(int shardNumber) {
    if (!numberOfValuesContributedByShard.containsKey(shardNumber)) {
      numberOfValuesContributedByShard.put(shardNumber, 1);
    } else {
      numberOfValuesContributedByShard.put(shardNumber, numberOfValuesContributedByShard.get(shardNumber)+1);
    }
  }
  
  private void contributeValueFromShard(int shardNumber, ResponseBuilder rb, NamedList<Object> shardValue) {
    
    incrementShardValueCount(shardNumber);

    Comparable value = PivotFacetHelper.getValue(shardValue);
    int count = PivotFacetHelper.getCount(shardValue);
    
    // We're changing values so we most mark the collection as dirty
    valueCollection.markDirty();
    
    if ( ( !shardLowestCount.containsKey(shardNumber) )
         || shardLowestCount.get(shardNumber) > count) {
      shardLowestCount.put(shardNumber,  count);
    }
    
    PivotFacetValue facetValue = valueCollection.get(value);
    if (null == facetValue) {
      // never seen before, we need to create it from scratch
      facetValue = PivotFacetValue.createFromNamedList(shardNumber, rb, this, shardValue);
      this.valueCollection.add(facetValue);
    } else {
      facetValue.mergeContributionFromShard(shardNumber, rb, shardValue);
    }
  }
  
  /**
   * Recursively merges the contributions from the specified shard for each 
   * {@link PivotFacetValue} represended in the <code>response</code>.
   * 
   * @see PivotFacetValue#mergeContributionFromShard
   * @param shardNumber the id of the shard that provided this data
   * @param rb The response builder of the current request
   * @param response the data from the specified shard for this pivot field, may be null
   */
  public void contributeFromShard(int shardNumber, ResponseBuilder rb, List<NamedList<Object>> response) {
    if (null == response) return;

    for (NamedList<Object> responseValue : response) {
      contributeValueFromShard(shardNumber, rb, responseValue);
    }
  }
  
  public String toString(){
    return String.format(Locale.ROOT, "P:%s F:%s V:%s",
                         parentValue, field, valueCollection);
  }
}

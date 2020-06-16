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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.common.params.FacetParams;

/**
 * Emcapsulates a collection of {@link PivotFacetValue}s associated with a 
 * {@link PivotFacetField} withs pecial tracking of a {@link PivotFacetValue} 
 * corrisponding to the <code>null</code> value when {@link FacetParams#FACET_MISSING} 
 * is used.
 *
 * @see #markDirty
 * @see PivotFacetField
 * @see PivotFacetValue
 */
@SuppressWarnings("rawtypes")
public class PivotFacetFieldValueCollection implements Iterable<PivotFacetValue> {
  private List<PivotFacetValue> explicitValues;  
  private PivotFacetValue missingValue;
  private Map<Comparable, PivotFacetValue> valuesMap;
  private boolean dirty = true;

  //Facet parameters relating to this field
  private final int facetFieldMinimumCount;
  private final int facetFieldOffset;  
  private final int facetFieldLimit;
  private final String facetFieldSort;
  
  
  public PivotFacetFieldValueCollection(int minCount, int offset, int limit, String fieldSort){
    this.explicitValues = new ArrayList<>();
    this.valuesMap = new HashMap<>(); 
    this.facetFieldMinimumCount = minCount;
    this.facetFieldOffset = offset;
    this.facetFieldLimit = limit;
    this.facetFieldSort = fieldSort;
  }

  /**
   * Indicates that the values in this collection have been modified by the caller.
   *
   * Any caller that manipulates the {@link PivotFacetValue}s contained in this collection
   * must call this method after doing so.
   */
  public void markDirty() {
    dirty = true;
  }

  /**
   * The {@link PivotFacetValue} with corisponding to a a value of 
   * <code>null</code> when {@link FacetParams#FACET_MISSING} is used.
   * 
   * @return the appropriate <code>PivotFacetValue</code> object, may be null 
   *         if we "missing" is not in use, or if it does not meat the mincount.
   */
  public PivotFacetValue getMissingValue(){
    return missingValue;
  }

  /** 
   * Read-Only access to the Collection of {@link PivotFacetValue}s corrisponding to 
   * non-missing values.
   *
   * @see #getMissingValue
   */
  public List<PivotFacetValue> getExplicitValuesList() {
    return Collections.unmodifiableList(explicitValues);
  }

  /** 
   * Size of {@link #getExplicitValuesList}
   */
  public int getExplicitValuesListSize() {
    return this.explicitValues.size();
  }
  
  /** 
   * Total number of {@link PivotFacetValue}s, including the "missing" value if used.
   *
   * @see #getMissingValue
   * @see #getExplicitValuesList
   */
  public int size() {
    return this.getExplicitValuesListSize() + (this.missingValue == null ? 0 : 1);
  }
  
  /**
   * Returns the appropriate sub-list of the explicit values that need to be refined, 
   * based on the {@link FacetParams#FACET_OFFSET} &amp; {@link FacetParams#FACET_LIMIT} 
   * for this field.
   *
   * @see #getExplicitValuesList
   * @see List#subList
   */
  public List<PivotFacetValue> getNextLevelValuesToRefine() {
    final int numRefinableValues = getExplicitValuesListSize();
    if (numRefinableValues < facetFieldOffset) {
      return Collections.<PivotFacetValue>emptyList();
    }
    
    final int offsetPlusCount = (facetFieldLimit >= 0) 
      ? Math.min(facetFieldLimit + facetFieldOffset, numRefinableValues) 
      : numRefinableValues;
    
    if (1 < facetFieldMinimumCount && facetFieldSort.equals(FacetParams.FACET_SORT_INDEX)) {
      // we have to skip any values that (still) don't meet the mincount
      //
      // TODO: in theory we could avoid this extra check by trimming sooner (SOLR-6331)
      // but since that's a destructive op that blows away the `valuesMap` which we (might?) still need
      // (and pre-emptively skips the offsets) we're avoiding re-working that optimization
      // for now until/unless someone gives it more careful thought...
      final List<PivotFacetValue> results = new ArrayList<>(numRefinableValues);
      for (PivotFacetValue pivotValue : explicitValues) {
        if (pivotValue.getCount() >= facetFieldMinimumCount) {
          results.add(pivotValue);
          if (numRefinableValues <= results.size()) {
            break;
          }
        }
      }
      return results;
    }
    
    // in the non "sort==count OR mincount==1" situation, we can just return the first N values
    // because any viable candidate is already in the top N
    return getExplicitValuesList().subList(facetFieldOffset,  offsetPlusCount);
  }
  
  /**
   * Fast lookup to retrieve a {@link PivotFacetValue} from this collection if it 
   * exists
   *
   * @param value of the <code>PivotFacetValue</code> to lookup, if 
   *        <code>null</code> this returns the same as {@link #getMissingValue}
   * @return the corrisponding <code>PivotFacetValue</code> or null if there is 
   *        no <code>PivotFacetValue</code> in this collection corrisponding to 
   *        the specified value.
   */
  public PivotFacetValue get(Comparable value){
    return valuesMap.get(value);
  }
  
  /**
   * Fetchs a {@link PivotFacetValue} from this collection via the index, may not 
   * be used to fetch the <code>PivotFacetValue</code> corrisponding to the missing-value.
   *
   * @see #getExplicitValuesList
   * @see List#get(int)
   * @see #getMissingValue
   */
  public PivotFacetValue getAt(int index){
    return explicitValues.get(index);
  }
  
  /**
   * Adds a {@link PivotFacetValue} to this collection -- callers must not use this 
   * method if a {@link PivotFacetValue} with the same value already exists in this collection
   */
  public void add(PivotFacetValue pfValue) {
    Comparable val = pfValue.getValue();
    assert ! this.valuesMap.containsKey(val) 
      : "Must not add duplicate PivotFacetValue with redundent inner value";

    dirty = true;
    if(null == val) {
      this.missingValue = pfValue;
    } else {
      this.explicitValues.add(pfValue);
    }
    this.valuesMap.put(val, pfValue);
  }


  /**
   * Destructive method that recursively prunes values from the data structure 
   * based on the counts for those values and the effective sort, mincount, limit, 
   * and offset being used for each field.
   * <p>
   * This method should only be called after all refinement is completed.
   * </p>
   *
   * @see PivotFacetField#trim
   * @see PivotFacet#getTrimmedPivotsAsListOfNamedLists
   */
  public void trim() {   // NOTE: destructive
    // TODO: see comment in PivotFacetField about potential optimization
    // (ie: trim as we refine)
    trimNonNullValues(); 
    trimNullValue();
  }
  
  private void trimNullValue(){
    if (missingValue == null) {
      return;
    }

    if (missingValue.getCount() >= facetFieldMinimumCount){
      if (null != missingValue.getChildPivot()) {
        missingValue.getChildPivot().trim();
      }
    } else { // missing count less than mincount
      missingValue = null;
    }
  }
  
  private void trimNonNullValues(){
    if (explicitValues != null && explicitValues.size() > 0) {
      
      sort();
      
      ArrayList<PivotFacetValue> trimmedValues = new ArrayList<>();
      
      int facetsSkipped = 0;
      
      for (PivotFacetValue pivotValue : explicitValues) {
        
        if (pivotValue.getCount() >= facetFieldMinimumCount) {
          if (facetsSkipped >= facetFieldOffset) {
            trimmedValues.add(pivotValue);
            if (pivotValue.getChildPivot() != null) {
              pivotValue.getChildPivot().trim();
            }
            if (facetFieldLimit > 0 && trimmedValues.size() >= facetFieldLimit) {
              break;
            }
          } else {
            facetsSkipped++;
          }
        }
      }
      
      explicitValues = trimmedValues;
      valuesMap.clear();
    }
  }
  
  /**
   * Sorts the collection and recursively sorts the collections assocaited with 
   * any sub-pivots.
   *
   * @see FacetParams#FACET_SORT
   * @see PivotFacetField#sort
   */
  public void sort() {
    
    if (dirty) {
      if (facetFieldSort.equals(FacetParams.FACET_SORT_COUNT)) {
        Collections.sort(this.explicitValues, new PivotFacetCountComparator());
      } else if (facetFieldSort.equals(FacetParams.FACET_SORT_INDEX)) {
        Collections.sort(this.explicitValues, new PivotFacetValueComparator());
      }
      dirty = false;
    }
    
    for (PivotFacetValue value : this.explicitValues)
      if (value.getChildPivot() != null) {
        value.getChildPivot().sort();
      }
   
    if (missingValue != null && missingValue.getChildPivot() != null) {
      missingValue.getChildPivot().sort();
    }
  }

  /**
   * Iterator over all elements in this Collection, including the result of 
   * {@link #getMissingValue} as the last element (if it exists)
   */
  @Override
  public Iterator<PivotFacetValue> iterator() {
    Iterator<PivotFacetValue> it = new Iterator<PivotFacetValue>() {
      private final Iterator valuesIterator = explicitValues.iterator();
      private boolean shouldGiveMissingValue = (missingValue != null);
      
      @Override
      public boolean hasNext() {
        return valuesIterator.hasNext() || shouldGiveMissingValue;
      }
      
      @Override
      public PivotFacetValue next() {
        while(valuesIterator.hasNext()){
          return (PivotFacetValue) valuesIterator.next();
        }
        //else
        if(shouldGiveMissingValue){
          shouldGiveMissingValue = false;
          return missingValue;
        }
        return null;
      }
      
      @Override
      public void remove() {
        throw new UnsupportedOperationException("Can't remove from this iterator");
      }
    };
    return it;
  }
    
  /** Sorts {@link PivotFacetValue} instances by their count */
  public static class PivotFacetCountComparator implements Comparator<PivotFacetValue> {
    public int compare(PivotFacetValue left, PivotFacetValue right) {
      int countCmp = right.getCount() - left.getCount();
      return (0 != countCmp) ? countCmp : 
        compareWithNullLast(left.getValue(), right.getValue());
    }    
  }
  
  /** Sorts {@link PivotFacetValue} instances by their value */
  public static class PivotFacetValueComparator implements Comparator<PivotFacetValue> {
    public int compare(PivotFacetValue left, PivotFacetValue right) {
      return compareWithNullLast(left.getValue(), right.getValue());
    }
  }
  
  /**
   * A helper method for use in <code>Comparator</code> classes where object properties 
   * are <code>Comparable</code> but may be null.
   */
  @SuppressWarnings({"unchecked"})
  static int compareWithNullLast(final Comparable o1, final Comparable o2) {
    if (null == o1) {
      if (null == o2) {
        return 0;
      }
      return 1; // o1 is null, o2 is not
    }
    if (null == o2) {
      return -1; // o2 is null, o1 is not
    }
    return o1.compareTo(o2);
  }
  
  public String toString(){
    return String.format(Locale.ROOT, "Values:%s | Missing:%s ", explicitValues, missingValue);
  }
}



/** Licensed to the Apache Software Foundation (ASF) under one or more
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

package org.apache.lucene.spatial.tier;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.HashMap;

import org.apache.lucene.search.Filter;
import org.apache.lucene.spatial.tier.DistanceHandler.Precision;

/**
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public abstract class DistanceFilter extends Filter {

  final protected Filter startingFilter;
  protected Precision precise;
  protected Map<Integer,Double> distances;
  protected double distance;

  protected int nextDocBase; 
  protected final WeakHashMap<String,Double> distanceLookupCache;

  /** Filters the startingFilter by precise distance
   *  checking filter */
  public DistanceFilter(Filter startingFilter, double distance) {
    if (startingFilter == null) {
      throw new IllegalArgumentException("please provide a non-null startingFilter; you can use QueryWrapperFilter(MatchAllDocsQuery) as a no-op filter");
    }
    this.startingFilter = startingFilter;
    this.distance = distance;

    // NOTE: neither of the distance filters use precision
    // now - if we turn that on, we'll need to pass top
    // reader into here
    // setPrecision(reader.maxDoc());

    /* store calculated distances for reuse by other components */
    distances = new HashMap<Integer,Double>();

    // create an intermediate cache to avoid recomputing
    //   distances for the same point 
    //   TODO: Why is this a WeakHashMap? 
    distanceLookupCache = new WeakHashMap<String,Double>();
  }

  public Map<Integer,Double> getDistances(){
    return distances;
  }
  
  public Double getDistance(int docid){
    return distances.get(docid);
  }
  
  public void setDistances(Map<Integer, Double> distances) {
    this.distances = distances;
  }

  /** You must call this before re-using this DistanceFilter
   *  across searches */
  public void reset() {
    nextDocBase = 0;
  }

  /** Returns true if <code>o</code> is equal to this. */
  public abstract boolean equals(Object o);

  /** Returns a hash code value for this object.*/
  public abstract int hashCode();

  /*
  private void setPrecision(int maxDocs) {
    precise = Precision.EXACT;
    
    if (maxDocs > 1000 && distance > 10) {
      precise = Precision.TWENTYFEET;
    }
    
    if (maxDocs > 10000 && distance > 10){
      precise = Precision.TWOHUNDREDFEET;
    }
  }
  */
}

/**
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

package org.apache.lucene.spatial.tier;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.queries.ChainedFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.spatial.geohash.GeoHashDistanceFilter;

/**
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class DistanceQueryBuilder {
  
  private final double lat;
  private final double lng;
  private final double miles;
  private final Filter filter;
  final DistanceFilter distanceFilter;

  /**
   * Create a distance query using
   * a boundary box wrapper around a more precise
   * DistanceFilter.
   * 
   * @param lat The latitude to search around
   * @param lng the Longitude to search around
   * @param miles The radius to search within
   * @param latField The name of the field containing the latitude
   * @param lngField The name of the field containing the longitude
   * @param tierFieldPrefix The prefix of the tier
   * @param needPrecise if true, then distance is calculated in addition to tier info
   * @param minTierIndexed The minimum tier level indexed
   * @param maxTierIndexed The maximum tier level indexed
   */
  public DistanceQueryBuilder (double lat, double lng, double miles, 
      String latField, String lngField, String tierFieldPrefix, boolean needPrecise, int minTierIndexed, int maxTierIndexed) {

    this.lat = lat;
    this.lng = lng;
    this.miles = miles;
    
    CartesianPolyFilterBuilder cpf = new CartesianPolyFilterBuilder(tierFieldPrefix, minTierIndexed, maxTierIndexed);
    Filter cartesianFilter = cpf.getBoundingArea(lat, lng, miles);

    /* create precise distance filter */
    if (needPrecise) {
      filter = distanceFilter = new LatLongDistanceFilter(cartesianFilter, lat, lng, miles, latField, lngField);
    } else {
      filter = cartesianFilter;
      distanceFilter = null;
    }
  }

  /**
   * Create a distance query using
   * a boundary box wrapper around a more precise
   * DistanceFilter.
   * 
   * @param lat
   * @param lng
   * @param miles
   */
  public DistanceQueryBuilder (double lat, double lng, double miles, 
      String geoHashFieldPrefix, String tierFieldPrefix, boolean needPrecise,  int minTierIndexed, int maxTierIndexed){

    this.lat = lat;
    this.lng = lng;
    this.miles = miles;
    
    CartesianPolyFilterBuilder cpf = new CartesianPolyFilterBuilder(tierFieldPrefix, minTierIndexed, maxTierIndexed);
    Filter cartesianFilter = cpf.getBoundingArea(lat, lng, miles);

    /* create precise distance filter */
    if (needPrecise) {
      filter = distanceFilter = new GeoHashDistanceFilter(cartesianFilter, lat, lng, miles, geoHashFieldPrefix);
    } else {
      filter = cartesianFilter;
      distanceFilter = null;
    }
  }

  
  /**
  * Create a distance query using
  * a boundary box wrapper around a more precise
  * DistanceFilter.
  */
  public Filter getFilter() {
    if (distanceFilter != null) {
      distanceFilter.reset();
    }
    return filter;
  }
  
  public Filter getFilter(Query query) {
    // Chain the Query (as filter) with our distance filter
    if (distanceFilter != null) {
      distanceFilter.reset();
    }
    QueryWrapperFilter qf = new QueryWrapperFilter(query);
    return new ChainedFilter(new Filter[] {qf, filter},
                             ChainedFilter.AND);
  }

  public DistanceFilter getDistanceFilter() {
    return distanceFilter;
  }
    
  public Query getQuery(Query query){
    return new ConstantScoreQuery(getFilter(query));
  }
  
  public double getLat() {
    return lat;
  }

  public double getLng() {
    return lng;
  }

  public double getMiles() {
    return miles;
  }
    
  @Override
  public String toString() {
    return "DistanceQuery lat: " + lat + " lng: " + lng + " miles: "+ miles;
  }
}

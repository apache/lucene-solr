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
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.spatial.SerialChainFilter;
import org.apache.lucene.spatial.geohash.GeoHashDistanceFilter;


public class DistanceQueryBuilder {

  private static final long serialVersionUID = 1L;
  
  public BoundaryBoxFilter latFilter;
  public BoundaryBoxFilter lngFilter;
  public DistanceFilter distanceFilter;
  
  private final double lat;
  private final double lng;
  private final double miles;
  private Filter cartesianFilter;
  
  /**
   * Create a distance query using
   * a boundary box wrapper around a more precise
   * DistanceFilter.
   * 
   * @see SerialChainFilter
   * @param lat
   * @param lng
   * @param miles
   */
  public DistanceQueryBuilder (double lat, double lng, double miles, 
      String latField, String lngField, String tierFieldPrefix,boolean needPrecise){

    this.lat = lat;
    this.lng = lng;
    this.miles = miles;
    
    
    CartesianPolyFilterBuilder cpf = new CartesianPolyFilterBuilder(tierFieldPrefix);
    cartesianFilter = cpf.getBoundingArea(lat, lng, (int)miles);

    /* create precise distance filter */
    if( needPrecise)
    	distanceFilter = new LatLongDistanceFilter(lat, lng, miles, latField, lngField);
    
  }

  /**
   * Create a distance query using
   * a boundary box wrapper around a more precise
   * DistanceFilter.
   * 
   * @see SerialChainFilter
   * @param lat
   * @param lng
   * @param miles
   */
  public DistanceQueryBuilder (double lat, double lng, double miles, 
      String geoHashFieldPrefix, String tierFieldPrefix,boolean needPrecise){

    this.lat = lat;
    this.lng = lng;
    this.miles = miles;
    
    
    CartesianPolyFilterBuilder cpf = new CartesianPolyFilterBuilder(tierFieldPrefix);
    cartesianFilter = cpf.getBoundingArea(lat, lng, (int)miles);

    /* create precise distance filter */
    if( needPrecise)
    	distanceFilter = new GeoHashDistanceFilter(lat, lng, miles, geoHashFieldPrefix);
    
  }

  
   /**
  * Create a distance query using
  * a boundary box wrapper around a more precise
  * DistanceFilter.
  * 
  * @see SerialChainFilter
  * @param lat
  * @param lng
  * @param miles
  */
  public Filter getFilter() {
    return new SerialChainFilter(new Filter[] {cartesianFilter, distanceFilter},
                    new int[] {SerialChainFilter.AND,
                           SerialChainFilter.SERIALAND});
  }
  
  public Filter getFilter(Query query) {
    QueryWrapperFilter qf = new QueryWrapperFilter(query);
    
    
    return new SerialChainFilter(new Filter[] {cartesianFilter, qf, distanceFilter},
          new int[] {SerialChainFilter.AND, 
              SerialChainFilter.AND,
              SerialChainFilter.SERIALAND});
  
  }
    
  public Query getQuery() {
      return new ConstantScoreQuery(getFilter());
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

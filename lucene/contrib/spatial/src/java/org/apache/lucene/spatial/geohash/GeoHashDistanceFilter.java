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

package org.apache.lucene.spatial.geohash;

import java.io.IOException;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldCache.DocTerms;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.FilteredDocIdSet;
import org.apache.lucene.spatial.DistanceUtils;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.spatial.tier.DistanceFilter;


/** <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */

public class GeoHashDistanceFilter extends DistanceFilter {
  
  private double lat;
  private double lng;
  private String geoHashField;
  
  /**
   * Provide a distance filter based from a center point with a radius
   * in miles
   * @param startingFilter
   * @param lat
   * @param lng
   * @param miles
   */
  public GeoHashDistanceFilter(Filter startingFilter, double lat, double lng, double miles, String geoHashField) {
    super(startingFilter, miles);
    this.lat = lat;
    this.lng = lng;
    this.geoHashField = geoHashField;
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {

    final DocTerms geoHashValues = FieldCache.DEFAULT.getTerms(context.reader, geoHashField);
    final BytesRef br = new BytesRef();

    final int docBase = nextDocBase;
    nextDocBase += context.reader.maxDoc();

    return new FilteredDocIdSet(startingFilter.getDocIdSet(context, acceptDocs)) {
      @Override
      public boolean match(int doc) {

        // TODO: cutover to BytesRef so we don't have to
        // make String here
        String geoHash = geoHashValues.getTerm(doc, br).utf8ToString();
        double[] coords = GeoHashUtils.decode(geoHash);
        double x = coords[0];
        double y = coords[1];
      
        // round off lat / longs if necessary
        //      x = DistanceHandler.getPrecision(x, precise);
        //      y = DistanceHandler.getPrecision(y, precise);
        Double cachedDistance = distanceLookupCache.get(geoHash);
        double d;
      
        if (cachedDistance != null) {
          d = cachedDistance.doubleValue();
        } else {
          d = DistanceUtils.getDistanceMi(lat, lng, x, y);
          distanceLookupCache.put(geoHash, d);
        }

        if (d < distance){
          distances.put(doc+docBase, d);
          return true;
        } else {
          return false;
        }
      }
    };
  }

  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof GeoHashDistanceFilter)) return false;
    GeoHashDistanceFilter other = (GeoHashDistanceFilter) o;

    if (!this.startingFilter.equals(other.startingFilter) ||
        this.distance != other.distance ||
        this.lat != other.lat ||
        this.lng != other.lng ||
        !this.geoHashField.equals(other.geoHashField) ) {
      return false;
    }
    return true;
  }

  /** Returns a hash code value for this object.*/
  @Override
  public int hashCode() {
    int h = Double.valueOf(distance).hashCode();
    h ^= startingFilter.hashCode();
    h ^= Double.valueOf(lat).hashCode();
    h ^= Double.valueOf(lng).hashCode();
    h ^= geoHashField.hashCode();
    
    return h;
  }
}

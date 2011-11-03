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

import java.io.IOException;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.FilteredDocIdSet;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.spatial.DistanceUtils;


/**
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */
public class LatLongDistanceFilter extends DistanceFilter {
  
  double lat;
  double lng;
  String latField;
  String lngField;

  int nextOffset = 0;
  
  /**
   * Provide a distance filter based from a center point with a radius
   * in miles.
   * @param startingFilter Filter to start from
   * @param lat
   * @param lng
   * @param miles
   * @param latField
   * @param lngField
   */
  public LatLongDistanceFilter(Filter startingFilter, double lat, double lng, double miles, String latField, String lngField) {
    super(startingFilter, miles);
    this.lat = lat;
    this.lng = lng;
    this.latField = latField;
    this.lngField = lngField;
  }
  
  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {

    final double[] latIndex = FieldCache.DEFAULT.getDoubles(context.reader, latField);
    final double[] lngIndex = FieldCache.DEFAULT.getDoubles(context.reader, lngField);

    final int docBase = nextDocBase;
    nextDocBase += context.reader.maxDoc();

    return new FilteredDocIdSet(startingFilter.getDocIdSet(context, acceptDocs)) {
      @Override
      protected boolean match(int doc) {
        double x = latIndex[doc];
        double y = lngIndex[doc];
      
        // round off lat / longs if necessary
        //      x = DistanceHandler.getPrecision(x, precise);
        //      y = DistanceHandler.getPrecision(y, precise);
      
        String ck = Double.toString(x)+","+Double.toString(y);
        Double cachedDistance = distanceLookupCache.get(ck);

        double d;
        if (cachedDistance != null){
          d = cachedDistance.doubleValue();
        } else {
          d = DistanceUtils.getDistanceMi(lat, lng, x, y);
          distanceLookupCache.put(ck, d);
        }

        if (d < distance) {
          // Save distances, so they can be pulled for
          // sorting after filtering is done:
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
    if (!(o instanceof LatLongDistanceFilter)) return false;
    LatLongDistanceFilter other = (LatLongDistanceFilter) o;

    if (!this.startingFilter.equals(other.startingFilter) ||
        this.distance != other.distance ||
        this.lat != other.lat ||
        this.lng != other.lng ||
        !this.latField.equals(other.latField) ||
        !this.lngField.equals(other.lngField)) {
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
    h ^= latField.hashCode();
    h ^= lngField.hashCode();
    return h;
  }
}

package org.apache.solr.search.function.distance;
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

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.queries.function.DocValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.spatial.geohash.GeoHashUtils;

import java.util.Map;
import java.io.IOException;


/**
 * Takes in a latitude and longitude ValueSource and produces a GeoHash.
 * <p/>
 * Ex: geohash(lat, lon)
 *
 * <p/>
 * Note, there is no reciprocal function for this.
 **/
public class GeohashFunction extends ValueSource {
  protected ValueSource lat, lon;

  public GeohashFunction(ValueSource lat, ValueSource lon) {
    this.lat = lat;
    this.lon = lon;
  }

  protected String name() {
    return "geohash";
  }

  @Override
  public DocValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final DocValues latDV = lat.getValues(context, readerContext);
    final DocValues lonDV = lon.getValues(context, readerContext);


    return new DocValues() {

      @Override
      public String strVal(int doc) {
        return GeoHashUtils.encode(latDV.doubleVal(doc), lonDV.doubleVal(doc));
      }

      @Override
      public String toString(int doc) {
        StringBuilder sb = new StringBuilder();
        sb.append(name()).append('(');
        sb.append(latDV.toString(doc)).append(',').append(lonDV.toString(doc));
        sb.append(')');
        return sb.toString();
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof GeohashFunction)) return false;

    GeohashFunction that = (GeohashFunction) o;

    if (!lat.equals(that.lat)) return false;
    if (!lon.equals(that.lon)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = lat.hashCode();
    result = 29 * result - lon.hashCode();
    return result;
  }

  @Override  
  public String description() {
    StringBuilder sb = new StringBuilder();
    sb.append(name()).append('(');
    sb.append(lat).append(',').append(lon);
    sb.append(')');
    return sb.toString();
  }
}

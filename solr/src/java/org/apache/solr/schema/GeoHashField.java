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

package org.apache.solr.schema;

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.geohash.GeoHashUtils;
import org.apache.lucene.spatial.DistanceUtils;
import org.apache.lucene.spatial.tier.InvalidGeoException;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.search.SpatialOptions;
import org.apache.solr.search.function.LiteralValueSource;
import org.apache.solr.search.function.ValueSourceRangeFilter;
import org.apache.solr.search.function.distance.GeohashHaversineFunction;


import java.io.IOException;

/**
 * This is a class that represents a <a
 * href="http://en.wikipedia.org/wiki/Geohash">Geohash</a> field. The field is
 * provided as a lat/lon pair and is internally represented as a string.
 *
 * @see org.apache.lucene.spatial.DistanceUtils#parseLatitudeLongitude(double[], String)
 */
public class GeoHashField extends FieldType implements SpatialQueryable {


  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    return getStringSort(field, top);
  }

    //QUESTION: Should we do a fast and crude one?  Or actually check distances
  //Fast and crude could use EdgeNGrams, but that would require a different
  //encoding.  Plus there are issues around the Equator/Prime Meridian
  public Query createSpatialQuery(QParser parser, SpatialOptions options) {
    double [] point = new double[0];
    try {
      point = DistanceUtils.parsePointDouble(null, options.pointStr, 2);
    } catch (InvalidGeoException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
    String geohash = GeoHashUtils.encode(point[0], point[1]);
    //TODO: optimize this
    return new SolrConstantScoreQuery(new ValueSourceRangeFilter(new GeohashHaversineFunction(getValueSource(options.field, parser),
            new LiteralValueSource(geohash), options.radius), "0", String.valueOf(options.distance), true, true));
  }

  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f)
          throws IOException {
    writer.writeStr(name, toExternal(f), false);
  }


  @Override
  public String toExternal(Fieldable f) {
    double[] latLon = GeoHashUtils.decode(f.stringValue());
    return latLon[0] + "," + latLon[1];
  }


  @Override
  public String toInternal(String val) {
    // validate that the string is of the form
    // latitude, longitude
    double[] latLon = new double[0];
    try {
      latLon = DistanceUtils.parseLatitudeLongitude(null, val);
    } catch (InvalidGeoException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
    return GeoHashUtils.encode(latLon[0], latLon[1]);
  }


  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    field.checkFieldCacheSource(parser);
    return new StrFieldSource(field.name);
  }


}

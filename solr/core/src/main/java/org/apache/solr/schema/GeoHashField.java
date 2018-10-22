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
package org.apache.solr.schema;

import java.io.IOException;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.LiteralValueSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.search.SpatialOptions;
import org.apache.solr.search.function.ValueSourceRangeFilter;
import org.apache.solr.search.function.distance.GeohashHaversineFunction;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.apache.solr.util.SpatialUtils;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.shape.Point;

/**
 * This is a class that represents a <a
 * href="http://en.wikipedia.org/wiki/Geohash">Geohash</a> field. The field is
 * provided as a lat/lon pair and is internally represented as a string.
 *
 * @deprecated use {@link LatLonPointSpatialField} instead
 */
@Deprecated
public class GeoHashField extends FieldType implements SpatialQueryable {

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    return getStringSort(field, top);
  }
  
  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return Type.SORTED_SET_BINARY;
    } else {
      return Type.SORTED;
    }
  }

    //QUESTION: Should we do a fast and crude one?  Or actually check distances
  //Fast and crude could use EdgeNGrams, but that would require a different
  //encoding.  Plus there are issues around the Equator/Prime Meridian
  @Override
  public Query createSpatialQuery(QParser parser, SpatialOptions options) {
    String geohash = toInternal(options.pointStr);
    //TODO: optimize this
    return new SolrConstantScoreQuery(new ValueSourceRangeFilter(new GeohashHaversineFunction(getValueSource(options.field, parser),
            new LiteralValueSource(geohash), options.radius), "0", String.valueOf(options.distance), true, true));
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f)
          throws IOException {
    writer.writeStr(name, toExternal(f), false);
  }

  @Override
  public String toExternal(IndexableField f) {
    Point p = GeohashUtils.decode(f.stringValue(), SpatialContext.GEO);
    return p.getY() + "," + p.getX();
  }

  @Override
  public String toInternal(String val) {
    Point point = SpatialUtils.parsePointSolrException(val, SpatialContext.GEO);
    return GeohashUtils.encodeLatLon(point.getY(), point.getX());
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    field.checkFieldCacheSource();
    return new StrFieldSource(field.name);
  }

  @Override
  public double getSphereRadius() {
    return DistanceUtils.EARTH_MEAN_RADIUS_KM;
  }

}

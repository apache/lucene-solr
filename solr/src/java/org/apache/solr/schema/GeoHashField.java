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
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.geohash.GeoHashUtils;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.response.XMLWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.distance.DistanceUtils;

import java.io.IOException;

/**
 * This is a class that represents a <a
 * href="http://en.wikipedia.org/wiki/Geohash">Geohash</a> field. The field is
 * provided as a lat/lon pair and is internally represented as a string.
 *
 * @see org.apache.solr.search.function.distance.DistanceUtils#parseLatitudeLongitude(double[], String)
 */
public class GeoHashField extends FieldType {


  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    return getStringSort(field, top);
  }

  @Override
  public void write(XMLWriter xmlWriter, String name, Fieldable f)
          throws IOException {
    xmlWriter.writeStr(name, toExternal(f));
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
    double[] latLon = DistanceUtils.parseLatitudeLongitude(null, val);
    return GeoHashUtils.encode(latLon[0], latLon[1]);
  }


  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    return new StrFieldSource(field.name);
  }


}

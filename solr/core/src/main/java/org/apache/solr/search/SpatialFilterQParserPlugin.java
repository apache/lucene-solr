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
package org.apache.solr.search;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Creates a spatial Filter based on the type of spatial point used.
 * <p>
 * The field must implement {@link org.apache.solr.schema.SpatialQueryable}
 * <p>
 * All units are in Kilometers
 * <p>
 * Syntax:
 * <pre>{!geofilt sfield=&lt;location_field&gt; pt=&lt;lat,lon&gt; d=&lt;distance&gt;}</pre>
 * <p>
 * Parameters:
 * <ul>
 * <li>sfield - The field to filter on. Required.</li>
 * <li>pt - The point to use as a reference.  Must match the dimension of the field. Required.</li>
 * <li>d - The distance in km.  Required.</li>
 * </ul>
 * The distance measure used currently depends on the FieldType.  LatLonType defaults to using haversine, PointType defaults to Euclidean (2-norm).
 * <p>
 * Examples:
 * <pre>fq={!geofilt sfield=store pt=10.312,-20.556 d=3.5}</pre>
 * <pre>fq={!geofilt sfield=store}&amp;pt=10.312,-20&amp;d=3.5</pre>
 * <pre>fq={!geofilt}&amp;sfield=store&amp;pt=10.312,-20&amp;d=3.5</pre>
 * <p>
 * Note: The geofilt for LatLonType is capable of also producing scores equal to the computed distance from the point
 * to the field, making it useful as a component of the main query or a boosting query.
 */
public class SpatialFilterQParserPlugin extends QParserPlugin {
  public static final String NAME = "geofilt";

  @Override
  public QParser createParser(String qstr, SolrParams localParams,
                              SolrParams params, SolrQueryRequest req) {

    return new SpatialFilterQParser(qstr, localParams, params, req, false);
  }

}


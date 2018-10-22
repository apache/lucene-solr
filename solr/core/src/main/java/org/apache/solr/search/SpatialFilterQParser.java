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
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.SpatialParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.SpatialQueryable;



/**
 * @see SpatialFilterQParserPlugin
 */
public class SpatialFilterQParser extends QParser {
  boolean bbox;  // do bounding box only

  public SpatialFilterQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req, boolean bbox) {
    super(qstr, localParams, params, req);
    this.bbox = bbox;
  }

  

  @Override
  public Query parse() throws SyntaxError {
    //if more than one, we need to treat them as a point...
    //TODO: Should we accept multiple fields
    String[] fields = localParams.getParams("f");
    if (fields == null || fields.length == 0) {
      String field = getParam(SpatialParams.FIELD);
      if (field == null)
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, " missing sfield for spatial request");
      fields = new String[] {field};
    }
    
    String pointStr = getParam(SpatialParams.POINT);
    if (pointStr == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, SpatialParams.POINT + " missing.");
    }

    double dist = -1;
    String distS = getParam(SpatialParams.DISTANCE);
    if (distS != null) dist = Double.parseDouble(distS);

    if (dist < 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, SpatialParams.DISTANCE + " must be >= 0");
    }

    String measStr = localParams.get(SpatialParams.MEASURE);
    //TODO: Need to do something with Measures
    Query result = null;
    //fields is valid at this point
    if (fields.length == 1) {
      SchemaField sf = req.getSchema().getField(fields[0]);
      FieldType type = sf.getType();

      if (type instanceof SpatialQueryable) {
        SpatialQueryable queryable = ((SpatialQueryable)type);
        double radius = localParams.getDouble(SpatialParams.SPHERE_RADIUS, queryable.getSphereRadius());
        SpatialOptions opts = new SpatialOptions(pointStr, dist, sf, measStr, radius);
        opts.bbox = bbox;
        result = queryable.createSpatialQuery(this, opts);
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The field " + fields[0]
                + " does not support spatial filtering");
      }
    } else {// fields.length > 1
      //TODO: Not sure about this just yet, is there a way to delegate, or do we just have a helper class?
      //Seems like we could just use FunctionQuery, but then what about scoring
      /*List<ValueSource> sources = new ArrayList<ValueSource>(fields.length);
      for (String field : fields) {
        SchemaField sf = schema.getField(field);
        sources.add(sf.getType().getValueSource(sf, this));
      }
      MultiValueSource vs = new VectorValueSource(sources);
      ValueSourceRangeFilter rf = new ValueSourceRangeFilter(vs, "0", String.valueOf(dist), true, true);
      result = new SolrConstantScoreQuery(rf);*/
    }

    return result;
  }
}

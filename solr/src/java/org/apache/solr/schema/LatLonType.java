package org.apache.solr.schema;
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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.DistanceUtils;
import org.apache.lucene.spatial.tier.InvalidGeoException;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.response.XMLWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SpatialOptions;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.VectorValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Represents a Latitude/Longitude as a 2 dimensional point.  Latitude is <b>always</b> specified first.
 * Can also, optionally, integrate in Spatial Tile capabilities.  The default is for tile fields from 4 - 15,
 * just as in the SpatialTileField that we are extending.
 */
public class LatLonType extends AbstractSubTypeFieldType implements SpatialQueryable {
  protected static final int LAT = 0;
  protected static final int LONG = 1;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    //TODO: refactor this, as we are creating the suffix cache twice, since the super.init does it too
    createSuffixCache(3);//we need three extra fields: one for the storage field, two for the lat/lon
  }

  @Override
  public Fieldable[] createFields(SchemaField field, String externalVal, float boost) {
    //we could have tileDiff + 3 fields (two for the lat/lon, one for storage)
    Fieldable[] f = new Fieldable[(field.indexed() ? 2 : 0) + (field.stored() ? 1 : 0)];
    if (field.indexed()) {
      int i = 0;
      double[] latLon = new double[0];
      try {
        latLon = DistanceUtils.parseLatitudeLongitude(null, externalVal);
      } catch (InvalidGeoException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
      //latitude
      f[i] = subField(field, i).createField(String.valueOf(latLon[LAT]), boost);
      i++;
      //longitude
      f[i] = subField(field, i).createField(String.valueOf(latLon[LONG]), boost);

    }

    if (field.stored()) {
      f[f.length - 1] = createField(field.getName(), externalVal,
              getFieldStore(field, externalVal), Field.Index.NO, Field.TermVector.NO,
              false, false, boost);
    }
    return f;
  }

  public Query createSpatialQuery(QParser parser, SpatialOptions options) {
    BooleanQuery result = new BooleanQuery();
    double[] point = new double[0];
    try {
      point = DistanceUtils.parseLatitudeLongitude(options.pointStr);
    } catch (InvalidGeoException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    //Get the distance
    double[] ur;
    double[] ll;
    if (options.measStr == null || options.measStr.equals("hsin")) {
      ur = DistanceUtils.latLonCornerDegs(point[LAT], point[LONG], options.distance, null, true, options.radius);
      ll = DistanceUtils.latLonCornerDegs(point[LAT], point[LONG], options.distance, null, false, options.radius);
    } else {
      ur = DistanceUtils.vectorBoxCorner(point, null, options.distance, true);
      ll = DistanceUtils.vectorBoxCorner(point, null, options.distance, false);
    }

    SchemaField subSF;
    Query range;

    double angDistDegs = DistanceUtils.angularDistance(options.distance,
            options.radius) * DistanceUtils.RADIANS_TO_DEGREES;
    //for the poles, do something slightly different
    if (point[LAT] + angDistDegs > 90.0) { //we cross the north pole
      //we don't need a longitude boundary at all

      double minLat = Math.min(ll[LAT], ur[LAT]);
      subSF = subField(options.field, LAT);
      range = subSF.getType().getRangeQuery(parser, subSF,
              String.valueOf(minLat),
              "90", true, true);
      result.add(range, BooleanClause.Occur.MUST);
    } else if (point[LAT] - angDistDegs < -90.0) {//we cross the south pole
      subSF = subField(options.field, LAT);
      double maxLat = Math.max(ll[LAT], ur[LAT]);
      range = subSF.getType().getRangeQuery(parser, subSF,
              "-90", String.valueOf(maxLat), true, true);
      result.add(range, BooleanClause.Occur.MUST);
    } else{
        //Latitude
        //we may need to generate multiple queries depending on the range
        //Are we crossing the 180 deg. longitude, if so, we need to do some special things
        if (ll[LONG] > 0.0 && ur[LONG] < 0.0) {
          //TODO: refactor into common code, etc.
          //Now check other side of the Equator
          if (ll[LAT] < 0.0 && ur[LAT] > 0.0) {
            addEquatorialBoundary(parser, options, result, ur[LAT], ll[LAT]);
          } //check poles
          else {
            subSF = subField(options.field, LAT);
            //not crossing the equator
            range = subSF.getType().getRangeQuery(parser, subSF,
                    String.valueOf(ll[LAT]),
                    String.valueOf(ur[LAT]), true, true);
            result.add(range, BooleanClause.Occur.MUST);
          }
          //Longitude
          addMeridianBoundary(parser, options, result, ur[LONG], ll[LONG], "180.0", "-180.0");

        } else if (ll[LONG] < 0.0 && ur[LONG] > 0.0) {//prime meridian (0 degrees
          //Now check other side of the Equator
          if (ll[LAT] < 0.0 && ur[LAT] > 0.0) {
            addEquatorialBoundary(parser, options, result, ur[LAT], ll[LAT]);
          } else {
            subSF = subField(options.field, LAT);
            //not crossing the equator
            range = subSF.getType().getRangeQuery(parser, subSF,
                    String.valueOf(ll[LAT]),
                    String.valueOf(ur[LAT]), true, true);
            result.add(range, BooleanClause.Occur.MUST);
          }
          //Longitude
          addMeridianBoundary(parser, options, result, ur[LONG], ll[LONG], "0.0", ".0");

        } else {// we are all in the Eastern or Western hemi
          //Now check other side of the Equator
          if (ll[LAT] < 0.0 && ur[LAT] > 0.0) {
            addEquatorialBoundary(parser, options, result, ur[LAT], ll[LAT]);
          } else {//we are all in either the Northern or the Southern Hemi.
            //TODO: nice to move this up so that it is the first thing and we can avoid the extra checks since
            //this is actually the most likely case
            subSF = subField(options.field, LAT);
            range = subSF.getType().getRangeQuery(parser, subSF,
                    String.valueOf(ll[LAT]),
                    String.valueOf(ur[LAT]), true, true);
            result.add(range, BooleanClause.Occur.MUST);

          }
          //Longitude, all in the same hemi
          subSF = subField(options.field, LONG);
          range = subSF.getType().getRangeQuery(parser, subSF,
                  String.valueOf(ll[LONG]),
                  String.valueOf(ur[LONG]), true, true);
          result.add(range, BooleanClause.Occur.MUST);
        }
      }

      return result;
    }

    /**
     * Add a boundary condition around a meridian
     * @param parser
     * @param options
     * @param result
     * @param upperRightLon
     * @param lowerLeftLon
     * @param eastern
     * @param western
     */

  private void addMeridianBoundary(QParser parser, SpatialOptions options, BooleanQuery result, double upperRightLon,
                                    double lowerLeftLon, String eastern, String western) {
    SchemaField subSF;
    Query range;
    BooleanQuery lonQ = new BooleanQuery();
    subSF = subField(options.field, LONG);
    //Eastern Hemisphere
    range = subSF.getType().getRangeQuery(parser, subSF,
            String.valueOf(lowerLeftLon),
            eastern, true, true);
    lonQ.add(range, BooleanClause.Occur.SHOULD);
    //Western hemi
    range = subSF.getType().getRangeQuery(parser, subSF,
            western,
            String.valueOf(upperRightLon), true, true);
    lonQ.add(range, BooleanClause.Occur.SHOULD);
    //One or the other must occur
    result.add(lonQ, BooleanClause.Occur.MUST);
  }

  /**
   * Add query conditions for boundaries like the equator, poles and meridians
   *
   * @param parser
   * @param options
   * @param result
   * @param upperRight
   * @param lowerLeft
   */
  protected void addEquatorialBoundary(QParser parser, SpatialOptions options, BooleanQuery result, double upperRight, double lowerLeft) {
    SchemaField subSF;
    Query range;
    BooleanQuery tmpQ = new BooleanQuery();
    subSF = subField(options.field, LAT);
    //southern hemi.
    range = subSF.getType().getRangeQuery(parser, subSF,
            String.valueOf(lowerLeft),
            "0", true, true);
    tmpQ.add(range, BooleanClause.Occur.SHOULD);
    //northern hemi
    range = subSF.getType().getRangeQuery(parser, subSF,
            "0", String.valueOf(upperRight), true, true);
    tmpQ.add(range, BooleanClause.Occur.SHOULD);
    //One or the other must occur
    result.add(tmpQ, BooleanClause.Occur.MUST);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    ArrayList<ValueSource> vs = new ArrayList<ValueSource>(2);
    for (int i = 0; i < 2; i++) {
      SchemaField sub = subField(field, i);
      vs.add(sub.getType().getValueSource(sub, parser));
    }
    return new LatLonValueSource(field, vs);
  }

  @Override
  public boolean isPolyField() {
    return true;
  }

  @Override
  public void write(XMLWriter xmlWriter, String name, Fieldable f) throws IOException {
    xmlWriter.writeStr(name, f.stringValue());
  }

  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException {
    writer.writeStr(name, f.stringValue(), false);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Sorting not supported on SpatialTileField " + field.getName());
  }



  //It never makes sense to create a single field, so make it impossible to happen

  @Override
  public Field createField(SchemaField field, String externalVal, float boost) {
    throw new UnsupportedOperationException("SpatialTileField uses multiple fields.  field=" + field.getName());
  }

}

class LatLonValueSource extends VectorValueSource {
  private final SchemaField sf;

  public LatLonValueSource(SchemaField sf, List<ValueSource> sources) {
    super(sources);
    this.sf = sf;
  }

  @Override
  public String name() {
    return "latlon";
  }

  @Override
  public String description() {
    return name() + "(" + sf.getName() + ")";
  }
}

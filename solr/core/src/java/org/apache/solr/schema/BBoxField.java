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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.spatial.bbox.BBoxOverlapRatioValueSource;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.util.ShapeAreaValueSource;
import org.apache.solr.common.SolrException;
import org.apache.solr.legacy.BBoxStrategy;
import org.apache.solr.legacy.LegacyFieldType;
import org.apache.solr.search.QParser;
import org.locationtech.spatial4j.shape.Rectangle;

public class BBoxField extends AbstractSpatialFieldType<BBoxStrategy> implements SchemaAware {
  private static final String PARAM_QUERY_TARGET_PROPORTION = "queryTargetProportion";
  private static final String PARAM_MIN_SIDE_LENGTH = "minSideLength";

  //score modes:
  private static final String OVERLAP_RATIO = "overlapRatio";
  private static final String AREA = "area";
  private static final String AREA2D = "area2D";

  private String numberTypeName;//required
  private String booleanTypeName = "boolean";
  private boolean storeSubFields = false;

  private IndexSchema schema;

  public BBoxField() {
    super(new HashSet<>(Arrays.asList(OVERLAP_RATIO, AREA, AREA2D)));
  }

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    String v = args.remove("numberType");
    if (v == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The field type: " + typeName
          + " must specify the numberType attribute.");
    }
    numberTypeName = v;

    v = args.remove("booleanType");
    if (v != null) {
      booleanTypeName = v;
    }
    
    v = args.remove("storeSubFields");
    if (v != null) {
      storeSubFields = Boolean.valueOf(v);
    }
  }

  @Override
  public void inform(IndexSchema schema) {
    this.schema = schema;
    FieldType numberType = schema.getFieldTypeByName(numberTypeName);
    FieldType booleanType = schema.getFieldTypeByName(booleanTypeName);

    if (numberType == null) {
      throw new RuntimeException("Cannot find number fieldType: " + numberTypeName);
    }
    if (booleanType == null) {
      throw new RuntimeException("Cannot find boolean fieldType: " + booleanTypeName);
    }
    if (!(booleanType instanceof BoolField)) {
      throw new RuntimeException("Must be a BoolField: " + booleanType);
    }
    if (numberType.getNumberType() != NumberType.DOUBLE) {
      throw new RuntimeException("Must be Double number type: " + numberType);
    }

    //note: this only works for explicit fields, not dynamic fields
    List<SchemaField> fields = new ArrayList<>(schema.getFields().values());//copy, because we modify during iteration
    for (SchemaField sf : fields) {
      if (sf.getType() == this) {
        String name = sf.getName();
        registerSubFields(schema, name, numberType, booleanType);
      }
    }
  }

  private void registerSubFields(IndexSchema schema, String name, FieldType numberType, FieldType booleanType) {
    register(schema, name + BBoxStrategy.SUFFIX_MINX, numberType);
    register(schema, name + BBoxStrategy.SUFFIX_MAXX, numberType);
    register(schema, name + BBoxStrategy.SUFFIX_MINY, numberType);
    register(schema, name + BBoxStrategy.SUFFIX_MAXY, numberType);
    register(schema, name + BBoxStrategy.SUFFIX_XDL, booleanType);
  }

  // note: Registering the field is probably optional; it makes it show up in the schema browser and may have other
  //  benefits.
  private void register(IndexSchema schema, String name, FieldType fieldType) {
    int props = fieldType.properties;
    if(storeSubFields) {
      props |= STORED;
    }
    else {
      props &= ~STORED;
    }
    SchemaField sf = new SchemaField(name, fieldType, props, null);
    schema.getFields().put(sf.getName(), sf);
  }

  @Override
  protected BBoxStrategy newSpatialStrategy(String fieldName) {
    //if it's a dynamic field, we register the sub-fields now.
    FieldType numberType = schema.getFieldTypeByName(numberTypeName);
    FieldType booleanType = schema.getFieldTypeByName(booleanTypeName);
    if (schema.isDynamicField(fieldName)) {
      registerSubFields(schema, fieldName, numberType, booleanType);
    }

    //Solr's FieldType ought to expose Lucene FieldType. Instead as a hack we create a Field with a dummy value.
    final SchemaField solrNumField = new SchemaField("_", numberType);//dummy temp
    org.apache.lucene.document.FieldType luceneType =
        (org.apache.lucene.document.FieldType) solrNumField.createField(0.0).fieldType();
    if ( ! (luceneType instanceof LegacyFieldType)) {
      luceneType = new org.apache.lucene.document.FieldType(luceneType);
    }
    luceneType.setStored(storeSubFields);
    
    //and annoyingly this Field isn't going to have a docValues format because Solr uses a separate Field for that
    if (solrNumField.hasDocValues()) {
      if (luceneType instanceof LegacyFieldType) {
        luceneType = new LegacyFieldType((LegacyFieldType)luceneType);
      } else {
        luceneType = new org.apache.lucene.document.FieldType(luceneType);
      }
      luceneType.setDocValuesType(DocValuesType.NUMERIC);
    }
    return new BBoxStrategy(ctx, fieldName, luceneType);
  }

  @Override
  protected DoubleValuesSource getValueSourceFromSpatialArgs(QParser parser, SchemaField field, SpatialArgs spatialArgs, String scoreParam, BBoxStrategy strategy) {
    if (scoreParam == null) {
      return null;
    }

    switch (scoreParam) {
      //TODO move these to superclass after LUCENE-5804 ?
      case OVERLAP_RATIO:
        double queryTargetProportion = 0.25;//Suggested default; weights towards target area

        String v = parser.getParam(PARAM_QUERY_TARGET_PROPORTION);
        if (v != null)
          queryTargetProportion = Double.parseDouble(v);

        double minSideLength = 0.0;
        v = parser.getParam(PARAM_MIN_SIDE_LENGTH);
        if (v != null)
          minSideLength = Double.parseDouble(v);

        return new BBoxOverlapRatioValueSource(
            strategy.makeShapeValueSource(), ctx.isGeo(),
            (Rectangle) spatialArgs.getShape(),
            queryTargetProportion, minSideLength);

      case AREA:
        return new ShapeAreaValueSource(strategy.makeShapeValueSource(), ctx, ctx.isGeo(),
            distanceUnits.multiplierFromDegreesToThisUnit() * distanceUnits.multiplierFromDegreesToThisUnit());

      case AREA2D:
        return new ShapeAreaValueSource(strategy.makeShapeValueSource(), ctx, false,
            distanceUnits.multiplierFromDegreesToThisUnit() * distanceUnits.multiplierFromDegreesToThisUnit());

      default:
        return super.getValueSourceFromSpatialArgs(parser, field, spatialArgs, scoreParam, strategy);
    }
  }
}


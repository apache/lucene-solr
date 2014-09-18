package org.apache.solr.schema;

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

import com.spatial4j.core.shape.Rectangle;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.spatial.bbox.BBoxOverlapRatioValueSource;
import org.apache.lucene.spatial.bbox.BBoxStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.util.ShapeAreaValueSource;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.QParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BBoxField extends AbstractSpatialFieldType<BBoxStrategy> implements SchemaAware {
  private static final String PARAM_QUERY_TARGET_PROPORTION = "queryTargetProportion";
  private static final String PARAM_MIN_SIDE_LENGTH = "minSideLength";
  private String numberFieldName;//required
  private String booleanFieldName = "boolean";

  private IndexSchema schema;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    String v = args.remove("numberType");
    if (v == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The field type: " + typeName
          + " must specify the numberType attribute.");
    }
    numberFieldName = v;

    v = args.remove("booleanType");
    if (v != null) {
      booleanFieldName = v;
    }
  }

  @Override
  public void inform(IndexSchema schema) {
    this.schema = schema;
    FieldType numberType = schema.getFieldTypeByName(numberFieldName);
    FieldType booleanType = schema.getFieldTypeByName(booleanFieldName);

    if (numberType == null) {
      throw new RuntimeException("Cannot find number fieldType: " + numberFieldName);
    }
    if (booleanType == null) {
      throw new RuntimeException("Cannot find boolean fieldType: " + booleanFieldName);
    }
    if (!(booleanType instanceof BoolField)) {
      throw new RuntimeException("Must be a BoolField: " + booleanType);
    }
    if (!(numberType instanceof TrieDoubleField)) { // TODO support TrieField (any trie) once BBoxStrategy does
      throw new RuntimeException("Must be TrieDoubleField: " + numberType);
    }

    List<SchemaField> fields = new ArrayList<>(schema.getFields().values());//copy, because we modify during iteration
    for (SchemaField sf : fields) {
      if (sf.getType() == this) {
        String name = sf.getName();
        register(schema, name + BBoxStrategy.SUFFIX_MINX, numberType);
        register(schema, name + BBoxStrategy.SUFFIX_MAXX, numberType);
        register(schema, name + BBoxStrategy.SUFFIX_MINY, numberType);
        register(schema, name + BBoxStrategy.SUFFIX_MAXY, numberType);
        register(schema, name + BBoxStrategy.SUFFIX_XDL, booleanType);
      }
    }
  }

  private void register(IndexSchema schema, String name, FieldType fieldType) {
    SchemaField sf = new SchemaField(name, fieldType);
    schema.getFields().put(sf.getName(), sf);
  }

  @Override
  protected BBoxStrategy newSpatialStrategy(String s) {
    BBoxStrategy strategy = new BBoxStrategy(ctx, s);
    //Solr's FieldType ought to expose Lucene FieldType. Instead as a hack we create a Field with a dummy value.
    SchemaField field = schema.getField(strategy.getFieldName() + BBoxStrategy.SUFFIX_MINX);
    org.apache.lucene.document.FieldType luceneType =
        (org.apache.lucene.document.FieldType) field.createField(0.0, 1.0f).fieldType();
    //and annoyingly this field isn't going to have a docValues format because Solr uses a separate Field for that
    if (field.hasDocValues()) {
      luceneType = new org.apache.lucene.document.FieldType(luceneType);
      luceneType.setDocValueType(FieldInfo.DocValuesType.NUMERIC);
    }
    strategy.setFieldType(luceneType);
    return strategy;
  }

  @Override
  protected ValueSource getValueSourceFromSpatialArgs(QParser parser, SchemaField field, SpatialArgs spatialArgs, String scoreParam, BBoxStrategy strategy) {
    switch (scoreParam) {
      //TODO move these to superclass after LUCENE-5804 ?
      case "overlapRatio":
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

      case "area":
        return new ShapeAreaValueSource(strategy.makeShapeValueSource(), ctx, ctx.isGeo());

      case "area2D":
        return new ShapeAreaValueSource(strategy.makeShapeValueSource(), ctx, false);

      default:
        return super.getValueSourceFromSpatialArgs(parser, field, spatialArgs, scoreParam, strategy);
    }
  }
}


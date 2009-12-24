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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.index.Term;
import org.apache.solr.search.QParser;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.MapSolrParams;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * A CoordinateFieldType is the base class for {@link org.apache.solr.schema.FieldType}s that have semantics
 * related to items in a coordinate system.
 * <br/>
 * Implementations depend on a delegating work to a sub {@link org.apache.solr.schema.FieldType}, specified by
 * either the {@link #SUB_FIELD_SUFFIX} or the {@link #SUB_FIELD_TYPE} (the latter is used if both are defined.
 * <br/>
 * Example:
 * <pre>&lt;fieldType name="xy" class="solr.PointType" dimension="2" subFieldType="double"/&gt;
 * </pre>
 * In theory, classes deriving from this should be able to do things like represent a point, a polygon, a line, etc.
 * <br/>
 * NOTE: There can only be one sub Field Type.
 *
 */
public abstract class CoordinateFieldType extends FieldType implements SchemaAware  {
  /**
   * The dimension of the coordinate system
   */
  protected int dimension;
  protected FieldType subType;
  public static final String SUB_FIELD_SUFFIX = "subFieldSuffix";
  public static final String SUB_FIELD_TYPE = "subFieldType";
  private String suffix;//need to keep this around between init and inform, since dynamic fields aren't created until before inform
  protected int dynFieldProps;

  public int getDimension() {
    return dimension;
  }

  public FieldType getSubType() {
    return subType;
  }

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {

    //it's not a first class citizen for the IndexSchema
    SolrParams p = new MapSolrParams(args);
    String subFT = p.get(SUB_FIELD_TYPE);
    String subSuffix = p.get(SUB_FIELD_SUFFIX);
    if (subFT != null) {
      args.remove(SUB_FIELD_TYPE);
      subType = schema.getFieldTypeByName(subFT.trim());
    } else if (subSuffix != null) {
      args.remove(SUB_FIELD_SUFFIX);
      suffix = subSuffix;
    }else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The field type: " + typeName
              + " must specify the " +
      SUB_FIELD_TYPE + " attribute or the " + SUB_FIELD_SUFFIX + " attribute.");
    }

    super.init(schema, args);
  }

  public void inform(IndexSchema schema) {
    //Can't do this until here b/c the Dynamic Fields are not initialized until here.
    if (suffix != null){
      SchemaField sf = schema.getField(suffix);
      subType = sf.getType();//this means it is already registered
      dynFieldProps = sf.getProperties(); 
    }
    else if (subType != null) {
      SchemaField proto = registerPolyFieldDynamicPrototype(schema, subType);
      dynFieldProps = proto.getProperties();
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The field type: " + typeName
              + " must specify the " +
      SUB_FIELD_TYPE + " attribute or the " + SUB_FIELD_SUFFIX + " attribute.");
    }
  }

  /**
   * Helper method for creating a dynamic field SchemaField prototype.  Returns a {@link org.apache.solr.schema.SchemaField} with
   * the {@link org.apache.solr.schema.FieldType} given and a name of "*" + {@link org.apache.solr.schema.FieldType#POLY_FIELD_SEPARATOR} + {@link org.apache.solr.schema.FieldType#typeName}
   * and props of indexed=true, stored=false.
   * @param schema the IndexSchema
   * @param type The {@link org.apache.solr.schema.FieldType} of the prototype.
   * @return The {@link org.apache.solr.schema.SchemaField}
   */

  static SchemaField registerPolyFieldDynamicPrototype(IndexSchema schema, FieldType type){
    String name = "*" + FieldType.POLY_FIELD_SEPARATOR + type.typeName;
    Map<String, String> props = new HashMap<String, String>();
    //Just set these, delegate everything else to the field type
    props.put("indexed", "true");
    props.put("stored", "false");
    int p = SchemaField.calcProps(name, type, props);
    SchemaField proto = SchemaField.create(name,
            type, p, null);
    schema.registerDynamicField(proto);
    return proto;
  }


  /**
   * Throws UnsupportedOperationException()
   */
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    throw new UnsupportedOperationException();
  }

}

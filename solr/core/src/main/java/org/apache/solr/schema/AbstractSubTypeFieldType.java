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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.search.QParser;
import org.apache.lucene.search.Query;

import java.util.HashMap;
import java.util.Map;


/**
 * An abstract base class for FieldTypes that delegate work to another {@link org.apache.solr.schema.FieldType}.
 * The sub type can be obtained by either specifying the subFieldType attribute or the subFieldSuffix.  In the former
 * case, a new dynamic field will be injected into the schema automatically with the name of {@link #POLY_FIELD_SEPARATOR}.
 * In the latter case, it will use an existing dynamic field definition to get the type.  See the example schema and the
 * use of the {@link org.apache.solr.schema.PointType} for more details.
 *
 **/
public abstract class AbstractSubTypeFieldType extends FieldType implements SchemaAware {
  protected FieldType subType;
  public static final String SUB_FIELD_SUFFIX = "subFieldSuffix";
  public static final String SUB_FIELD_TYPE = "subFieldType";
  protected String suffix;
  protected int dynFieldProps;
  protected String[] suffixes;
  protected String subFieldType = null;
  protected String subSuffix = null;
  protected IndexSchema schema;   // needed for retrieving SchemaFields

  public FieldType getSubType() {
    return subType;
  }

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    this.schema = schema;
    //it's not a first class citizen for the IndexSchema
    SolrParams p = new MapSolrParams(args);
    subFieldType = p.get(SUB_FIELD_TYPE);
    subSuffix = p.get(SUB_FIELD_SUFFIX);
    if (subFieldType != null) {
      args.remove(SUB_FIELD_TYPE);
      subType = schema.getFieldTypeByName(subFieldType.trim());
      suffix = POLY_FIELD_SEPARATOR + subType.typeName;
    } else if (subSuffix != null) {
      args.remove(SUB_FIELD_SUFFIX);
      suffix = subSuffix;
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The field type: " + typeName
              + " must specify the " +
              SUB_FIELD_TYPE + " attribute or the " + SUB_FIELD_SUFFIX + " attribute.");
    }
  }

  /**
   * Helper method for creating a dynamic field SchemaField prototype.  Returns a {@link SchemaField} with
   * the {@link FieldType} given and a name of "*" + {@link FieldType#POLY_FIELD_SEPARATOR} + {@link FieldType#typeName}
   * and props of indexed=true, stored=false.
   *
   * @param schema the IndexSchema
   * @param subType   The {@link FieldType} of the prototype.
   * @param polyField   The poly {@link FieldType}.
   * @return The {@link SchemaField}
   */

  static SchemaField registerPolyFieldDynamicPrototype(IndexSchema schema, FieldType subType, FieldType polyField) {
    String name = "*" + FieldType.POLY_FIELD_SEPARATOR + subType.typeName;
    Map<String, String> props = new HashMap<>();
    //Just set these, delegate everything else to the field type
    props.put("indexed", "true");
    props.put("stored", "false");
    props.put("multiValued", "false");
    // if polyField enables dv, add them to the subtypes
    if (polyField.hasProperty(DOC_VALUES)) {
      props.put("docValues", "true");
    }
    int p = SchemaField.calcProps(name, subType, props);
    SchemaField proto = SchemaField.create(name,
        subType, p, null);
    schema.registerDynamicFields(proto);
    return proto;
  }

  /**
   * Registers the polyfield dynamic prototype for this field type: : "*___(field type name)" 
   * 
   * {@inheritDoc}
   *  
   * @param schema {@inheritDoc}
   *
   */
  @Override
  public void inform(IndexSchema schema) {
    this.schema = schema;
    //Can't do this until here b/c the Dynamic Fields are not initialized until here.
    if (subType != null) {
      SchemaField proto = registerPolyFieldDynamicPrototype(schema, subType, this);
      dynFieldProps = proto.getProperties();
    }
  }

  /**
   * Throws UnsupportedOperationException()
   */
  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    throw new UnsupportedOperationException();
  }

  protected void createSuffixCache(int size) {
    suffixes = new String[size];
    for (int i=0; i<size; i++) {
      suffixes[i] = "_" + i + suffix;
    }
  }

  protected SchemaField subField(SchemaField base, int i, IndexSchema schema) {
    return schema.getField(base.getName() + suffixes[i]);
  }
}

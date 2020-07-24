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

import java.util.HashMap;
import java.util.Map;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntSet;

public class PolyFieldHelper {
  private final FieldType parent;
  private final String suffixArgname;
  private final String typeNameArgname;
  private final String purposeSuffix;
  protected final Map<String, String> defaultFieldProps;
  protected final Map<String, String> defaultFieldTypeArgs;
  protected final int inheritProps;

  private Boolean hasExplicitType;
  private FieldType explicitType;
  private FieldType subType;
  private IndexSchema schema;
  private boolean suffixInitialized;
  private String suffix;
  private String suffixDynamic;
  private String typeName;

  PolyFieldHelper(FieldType parent, String suffixArgname, String typeArgname, String purposeSuffix,
      Map<String, String> defaultFieldProps, Map<String, String> defaultFieldTypeArgs, int inheritProps) {
    this.parent = parent;
    this.suffixArgname = suffixArgname;
    this.typeNameArgname = typeArgname;
    this.purposeSuffix = purposeSuffix;
    this.defaultFieldProps = defaultFieldProps;
    this.defaultFieldTypeArgs = defaultFieldTypeArgs;
    this.inheritProps = inheritProps;
  }

  public boolean hasExplicitType() {
    return hasExplicitType;
  }

  public FieldType getExplicitType() {
    return explicitType;
  }

  private IntObjectMap<String> dynamicFieldCache;
  private IntSet subfieldPropsRegistry;

  public SchemaField getSubfield(SchemaField parent) {
    if (suffix != null) {
      return schema.getField(parent.getName().concat(suffix));
    } else {
      final int p = parent.properties;
      String propsSuffix = dynamicFieldCache.get(p);
      if (propsSuffix == null) {
        final int subfieldProps = calcProps(suffixDynamic, subType, defaultFieldProps, inheritProps, p);
        propsSuffix = suffixDynamic.concat(Integer.toHexString(subfieldProps));
        dynamicFieldCache.put(p, propsSuffix);
        if (subfieldPropsRegistry.add(subfieldProps)) {
          SchemaField proto = SchemaField.create("*".concat(propsSuffix), subType, subfieldProps, null);
          schema.registerDynamicFields(proto);
        }
      }
      return schema.getField(parent.getName().concat(propsSuffix));
    }
  }

  private static int calcProps(String name, FieldType ft, Map<String, String> props, int inheritProps, int parentProps) {
    int p = SchemaField.calcProps(name, ft, props);
    p = (p & ~inheritProps) | (parentProps & inheritProps);
    return p;
  }

  @Override
  public String toString() {
    return "{"+suffixArgname+"=\""+suffix+"\", "+typeNameArgname+"=\""+typeName+"\"}";
  }

  public void init(IndexSchema schema, Map<String,String> args) {
    suffix = args.remove(suffixArgname);
    typeName = args.remove(typeNameArgname);
    if (suffix != null && typeName != null) {
      throw new IllegalArgumentException("args "+typeNameArgname+" and "+suffixArgname
          +" are mutually exclusive (fieldType=\""+parent.getTypeName()+"\"");
    }
    hasExplicitType = suffix != null || typeName != null;
  }

  public void inform(IndexSchema schema) {
    this.schema = schema;
    if (!suffixInitialized) { // only initialize suffix once, so inform(...) is idempotent
      suffixInitialized = true;
      if (suffix != null) {
        // explicit suffix implicitly determines whether field will be compatible with multiValued or single-valued; but we
        // record both here, because it doesn't matter, and to register initialization.
        suffix = purposeSuffix + TextField.POLY_FIELD_SEPARATOR + suffix;
        SchemaField storedDvProto = schema.getField(suffix);
        explicitType = subType = storedDvProto.getType();
        if (parent.isMultiValued() != storedDvProto.multiValued()) {
          throw new IllegalStateException("TODO: flesh out this message: "+suffix);
        }
      } else {
        dynamicFieldCache = new IntObjectHashMap<>();
        subfieldPropsRegistry = new IntHashSet();
        if (typeName != null) {
          explicitType = subType = schema.getFieldTypeByName(typeName);
          suffixDynamic = purposeSuffix + TextField.POLY_FIELD_SEPARATOR + typeName + "_";
        } else {
          String syntheticDefaultTypeName = parent.getTypeName() + purposeSuffix;
          subType = getDefaultFieldType();
          subType.setTypeName(syntheticDefaultTypeName);
          Map<String, String> props = new HashMap<>(defaultFieldProps.size() + defaultFieldTypeArgs.size() + 1);
          props.putAll(defaultFieldTypeArgs);
          props.putAll(defaultFieldProps);
          subType.setArgs(schema, props);
          subType.init(schema, props);
          schema.getFieldTypes().put(syntheticDefaultTypeName, subType); // safe modification when called from inform(...) method
          suffixDynamic = TextField.POLY_FIELD_SEPARATOR + syntheticDefaultTypeName + "_";
        }
      }
    }
  }

  /**
   * May be overridden to return a different default FieldType
   */
  protected FieldType getDefaultFieldType() {
    return new StrField();
  }
}

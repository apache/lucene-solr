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
package org.apache.solr.handler.dataimport.config;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.handler.dataimport.DataImportHandlerException;
import org.apache.solr.handler.dataimport.DataImporter;
import org.apache.solr.schema.SchemaField;
import org.w3c.dom.Element;

public class Entity {
  private final String name;
  private final String pk;
  private final String pkMappingFromSchema;
  private final String dataSourceName;
  private final String processorName;
  private final Entity parentEntity;
  private final boolean docRoot;
  private final boolean child;
  private final List<Entity> children;
  private final List<EntityField> fields;
  private final Map<String,Set<EntityField>> colNameVsField;
  private final Map<String,String> allAttributes;
  private final List<Map<String,String>> allFieldAttributes;
  private final DIHConfiguration config;
  
  public Entity(boolean docRootFound, Element element, DataImporter di, DIHConfiguration config, Entity parent) {
    this.parentEntity = parent;
    this.config = config;
    
    String modName = ConfigParseUtil.getStringAttribute(element, ConfigNameConstants.NAME, null);
    if (modName == null) {
      throw new DataImportHandlerException(SEVERE, "Entity must have a name.");
    }
    if (modName.indexOf(".") != -1) {
      throw new DataImportHandlerException(SEVERE,
          "Entity name must not have period (.): '" + modName);
    }
    if (ConfigNameConstants.RESERVED_WORDS.contains(modName)) {
      throw new DataImportHandlerException(SEVERE, "Entity name : '" + modName
          + "' is a reserved keyword. Reserved words are: " + ConfigNameConstants.RESERVED_WORDS);
    }
    this.name = modName;
    this.pk = ConfigParseUtil.getStringAttribute(element, "pk", null);
    this.processorName = ConfigParseUtil.getStringAttribute(element, ConfigNameConstants.PROCESSOR,null);
    this.dataSourceName = ConfigParseUtil.getStringAttribute(element, DataImporter.DATA_SRC, null);
    
    String rawDocRootValue = ConfigParseUtil.getStringAttribute(element, ConfigNameConstants.ROOT_ENTITY, null);
    if (!docRootFound && !"false".equals(rawDocRootValue)) {
      // if in this chain no document root is found()
      docRoot = true;
    } else {
      docRoot = false;
    }
    
    String childValue = ConfigParseUtil.getStringAttribute(element, ConfigNameConstants.CHILD, null);
    child = "true".equals(childValue);
    
    Map<String,String> modAttributes = ConfigParseUtil
        .getAllAttributes(element);
    modAttributes.put(ConfigNameConstants.DATA_SRC, this.dataSourceName);
    this.allAttributes = Collections.unmodifiableMap(modAttributes);
    
    List<Element> n = ConfigParseUtil.getChildNodes(element, "field");
    List<EntityField> modFields = new ArrayList<>(n.size());
    Map<String,Set<EntityField>> modColNameVsField = new HashMap<>();
    List<Map<String,String>> modAllFieldAttributes = new ArrayList<>();
    for (Element elem : n) {
      EntityField.Builder fieldBuilder = new EntityField.Builder(elem);
      if (config.getSchema() != null) {
        if (fieldBuilder.getNameOrColumn() != null
            && fieldBuilder.getNameOrColumn().contains("${")) {
          fieldBuilder.dynamicName = true;
        } else {
          SchemaField schemaField = config.getSchemaField
              (fieldBuilder.getNameOrColumn());
          if (schemaField != null) {
            fieldBuilder.name = schemaField.getName();
            fieldBuilder.multiValued = schemaField.multiValued();
            fieldBuilder.allAttributes.put(DataImporter.MULTI_VALUED, Boolean
                .toString(schemaField.multiValued()));
            fieldBuilder.allAttributes.put(DataImporter.TYPE, schemaField
                .getType().getTypeName());
            fieldBuilder.allAttributes.put("indexed", Boolean
                .toString(schemaField.indexed()));
            fieldBuilder.allAttributes.put("stored", Boolean
                .toString(schemaField.stored()));
            fieldBuilder.allAttributes.put("defaultValue", schemaField
                .getDefaultValue());
          } else {
            fieldBuilder.toWrite = false;
          }
        }
      }
      Set<EntityField> fieldSet = modColNameVsField.get(fieldBuilder.column);
      if (fieldSet == null) {
        fieldSet = new HashSet<>();
        modColNameVsField.put(fieldBuilder.column, fieldSet);
      }
      fieldBuilder.allAttributes.put("boost", Float
          .toString(fieldBuilder.boost));
      fieldBuilder.allAttributes.put("toWrite", Boolean
          .toString(fieldBuilder.toWrite));
      modAllFieldAttributes.add(fieldBuilder.allAttributes);
      fieldBuilder.entity = this;
      EntityField field = new EntityField(fieldBuilder);
      fieldSet.add(field);
      modFields.add(field);
    }
    Map<String,Set<EntityField>> modColNameVsField1 = new HashMap<>();
    for (Map.Entry<String,Set<EntityField>> entry : modColNameVsField
        .entrySet()) {
      if (entry.getValue().size() > 0) {
        modColNameVsField1.put(entry.getKey(), Collections
            .unmodifiableSet(entry.getValue()));
      }
    }
    this.colNameVsField = Collections.unmodifiableMap(modColNameVsField1);
    this.fields = Collections.unmodifiableList(modFields);
    this.allFieldAttributes = Collections
        .unmodifiableList(modAllFieldAttributes);
    
    String modPkMappingFromSchema = null;
    if (config.getSchema() != null) {
      SchemaField uniqueKey = config.getSchema().getUniqueKeyField();
      if (uniqueKey != null) {
        modPkMappingFromSchema = uniqueKey.getName();
        // if no fields are mentioned . solr uniqueKey is same as dih 'pk'
        for (EntityField field : fields) {
          if (field.getName().equals(modPkMappingFromSchema)) {
            modPkMappingFromSchema = field.getColumn();
            // get the corresponding column mapping for the solr uniqueKey
            // But if there are multiple columns mapping to the solr uniqueKey,
            // it will fail
            // so , in one off cases we may need pk
            break;
          }
        }
      }
    }
    pkMappingFromSchema = modPkMappingFromSchema;
    n = ConfigParseUtil.getChildNodes(element, "entity");
    List<Entity> modEntities = new ArrayList<>();
    for (Element elem : n) {
      modEntities.add(new Entity((docRootFound || this.docRoot), elem, di, config, this));
    }
    this.children = Collections.unmodifiableList(modEntities);
  }
  
  public String getPk() {
    return pk == null ? pkMappingFromSchema : pk;
  }
  
  public String getSchemaPk() {
    return pkMappingFromSchema != null ? pkMappingFromSchema : pk;
  }
  
  public String getName() {
    return name;
  }
  
  public String getPkMappingFromSchema() {
    return pkMappingFromSchema;
  }
  
  public String getDataSourceName() {
    return dataSourceName;
  }
  
  public String getProcessorName() {
    return processorName;
  }
  
  public Entity getParentEntity() {
    return parentEntity;
  }
  
  public boolean isDocRoot() {
    return docRoot;
  }
  
  public List<Entity> getChildren() {
    return children;
  }
  
  public List<EntityField> getFields() {
    return fields;
  }
  
  public Map<String,Set<EntityField>> getColNameVsField() {
    return colNameVsField;
  }
  
  public Map<String,String> getAllAttributes() {
    return allAttributes;
  }
  
  public List<Map<String,String>> getAllFieldsList() {
    return allFieldAttributes;
  }

  public boolean isChild() {
    return child;
  }
}

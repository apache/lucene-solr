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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.handler.dataimport.DataImporter;
import org.apache.solr.handler.dataimport.DocBuilder;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

/**
 * <p>
 * Mapping for data-config.xml
 * </p>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
public class DIHConfiguration {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // TODO - remove from here and add it to entity
  private final String deleteQuery;
  
  private final List<Entity> entities;
  private final String onImportStart;
  private final String onImportEnd;
  private final String onError;
  private final List<Map<String, String>> functions;
  private final Script script;
  private final Map<String, Map<String,String>> dataSources;
  private final PropertyWriter propertyWriter;
  private final IndexSchema schema;
  private final Map<String,SchemaField> lowerNameVsSchemaField;
  
  public DIHConfiguration(Element element, DataImporter di,
      List<Map<String,String>> functions, Script script,
      Map<String,Map<String,String>> dataSources, PropertyWriter pw) {
    schema = di.getSchema();
    lowerNameVsSchemaField = null == schema ? Collections.<String,SchemaField>emptyMap() : loadSchemaFieldMap();
    this.deleteQuery = ConfigParseUtil.getStringAttribute(element, "deleteQuery", null);
    this.onImportStart = ConfigParseUtil.getStringAttribute(element, "onImportStart", null);
    this.onImportEnd = ConfigParseUtil.getStringAttribute(element, "onImportEnd", null);
    this.onError = ConfigParseUtil.getStringAttribute(element, "onError", null);
    List<Entity> modEntities = new ArrayList<>();
    List<Element> l = ConfigParseUtil.getChildNodes(element, "entity");
    boolean docRootFound = false;
    for (Element e : l) {
      Entity entity = new Entity(docRootFound, e, di, this, null);
      Map<String, EntityField> fields = gatherAllFields(di, entity);
      verifyWithSchema(fields);    
      modEntities.add(entity);
    }
    this.entities = Collections.unmodifiableList(modEntities);
    if(functions==null) {
      functions = Collections.emptyList();
    }
    List<Map<String, String>> modFunc = new ArrayList<>(functions.size());
    for(Map<String, String> f : functions) {
      modFunc.add(Collections.unmodifiableMap(f));
    }
    this.functions = Collections.unmodifiableList(modFunc);
    this.script = script;
    this.dataSources = Collections.unmodifiableMap(dataSources);
    this.propertyWriter = pw;
  }

  private void verifyWithSchema(Map<String,EntityField> fields) {
    Map<String,SchemaField> schemaFields = null;
    if (schema == null) {
      schemaFields = Collections.emptyMap();
    } else {
      schemaFields = schema.getFields();
    }
    for (Map.Entry<String,SchemaField> entry : schemaFields.entrySet()) {
      SchemaField sf = entry.getValue();
      if (!fields.containsKey(sf.getName())) {
        if (sf.isRequired()) {
          if (log.isInfoEnabled()) {
            log.info("{} is a required field in SolrSchema . But not found in DataConfig", sf.getName());
          }
        }
      }
    }
    for (Map.Entry<String,EntityField> entry : fields.entrySet()) {
      EntityField fld = entry.getValue();
      SchemaField field = getSchemaField(fld.getName());
      if (field == null && !isSpecialCommand(fld.getName())) {
        if (log.isInfoEnabled()) {
          log.info("The field :{} present in DataConfig does not have a counterpart in Solr Schema", fld.getName());
        }
      }
    }
  }

  private Map<String,EntityField> gatherAllFields(DataImporter di, Entity e) {
    Map<String,EntityField> fields = new HashMap<>();
    if (e.getFields() != null) {
      for (EntityField f : e.getFields()) {
        fields.put(f.getName(), f);
      }
    }
    for (Entity e1 : e.getChildren()) {
      fields.putAll(gatherAllFields(di, e1));
    }
    return fields;
  }

  private Map<String,SchemaField> loadSchemaFieldMap() {
    Map<String, SchemaField> modLnvsf = new HashMap<>();
    for (Map.Entry<String, SchemaField> entry : schema.getFields().entrySet()) {
      modLnvsf.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
    }
    return Collections.unmodifiableMap(modLnvsf);
  }

  public SchemaField getSchemaField(String caseInsensitiveName) {
    SchemaField schemaField = null;
    if(schema!=null) {
      schemaField = schema.getFieldOrNull(caseInsensitiveName);
    }
    if (schemaField == null) {
      schemaField = lowerNameVsSchemaField.get(caseInsensitiveName.toLowerCase(Locale.ROOT));
    }
    return schemaField;
  }


  public String getDeleteQuery() {
    return deleteQuery;
  }
  public List<Entity> getEntities() {
    return entities;
  }
  public String getOnImportStart() {
    return onImportStart;
  }
  public String getOnImportEnd() {
    return onImportEnd;
  }
  public String getOnError() {
    return onError;
  }
  public List<Map<String,String>> getFunctions() {
    return functions;
  }
  public Map<String,Map<String,String>> getDataSources() {
    return dataSources;
  }
  public Script getScript() {
    return script;
  }
  public PropertyWriter getPropertyWriter() {
    return propertyWriter;
  }

  public IndexSchema getSchema() {
    return schema;
  }

  public static boolean isSpecialCommand(String fld) {
    return DocBuilder.DELETE_DOC_BY_ID.equals(fld) ||
        DocBuilder.DELETE_DOC_BY_QUERY.equals(fld) ||
        DocBuilder.DOC_BOOST.equals(fld) ||
        DocBuilder.SKIP_DOC.equals(fld) ||
        DocBuilder.SKIP_ROW.equals(fld);

  }
}
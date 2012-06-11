package org.apache.solr.handler.dataimport.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.solr.handler.dataimport.DataImporter;
import org.w3c.dom.Element;

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

/**
 * <p>
 * Mapping for data-config.xml
 * </p>
 * <p/>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
public class DIHConfiguration {
  // TODO - remove from here and add it to entity
  private final String deleteQuery;
  private final List<Entity> entities;
  private final String onImportStart;
  private final String onImportEnd;
  private final List<Map<String, String>> functions;
  private final Script script;
  private final Map<String, Properties> dataSources;
  public DIHConfiguration(Element element, DataImporter di, List<Map<String, String>> functions, Script script, Map<String, Properties> dataSources) {
    this.deleteQuery = ConfigParseUtil.getStringAttribute(element, "deleteQuery", null);
    this.onImportStart = ConfigParseUtil.getStringAttribute(element, "onImportStart", null);
    this.onImportEnd = ConfigParseUtil.getStringAttribute(element, "onImportEnd", null);
    List<Entity> modEntities = new ArrayList<Entity>();
    List<Element> l = ConfigParseUtil.getChildNodes(element, "entity");
    boolean docRootFound = false;
    for (Element e : l) {
      Entity entity = new Entity(docRootFound, e, di, null);
      Map<String, EntityField> fields = ConfigParseUtil.gatherAllFields(di, entity);
      ConfigParseUtil.verifyWithSchema(di, fields);    
      modEntities.add(entity);
    }
    this.entities = Collections.unmodifiableList(modEntities);
    if(functions==null) {
      functions = Collections.emptyList();
    }
    List<Map<String, String>> modFunc = new ArrayList<Map<String, String>>(functions.size());
    for(Map<String, String> f : functions) {
      modFunc.add(Collections.unmodifiableMap(f));
    }
    this.functions = Collections.unmodifiableList(modFunc);
    this.script = script;
    this.dataSources = Collections.unmodifiableMap(dataSources);
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
  public List<Map<String,String>> getFunctions() {
    return functions;
  }
  public Map<String,Properties> getDataSources() {
    return dataSources;
  }
  public Script getScript() {
    return script;
  }
}
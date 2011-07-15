package org.apache.solr.handler.dataimport;
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


import org.apache.solr.core.SolrCore;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * An implementation for the Context
 * </p>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
public class ContextImpl extends Context {
  protected DataConfig.Entity entity;

  private ContextImpl parent;

  private VariableResolverImpl resolver;

  private DataSource ds;

  private String currProcess;

  private Map<String, Object> requestParams;

  private DataImporter dataImporter;

  private Map<String, Object> entitySession, globalSession;

  DocBuilder.DocWrapper doc;

  DocBuilder docBuilder;


  public ContextImpl(DataConfig.Entity entity, VariableResolverImpl resolver,
                     DataSource ds, String currProcess,
                     Map<String, Object> global, ContextImpl parentContext, DocBuilder docBuilder) {
    this.entity = entity;
    this.docBuilder = docBuilder;
    this.resolver = resolver;
    this.ds = ds;
    this.currProcess = currProcess;
    if (docBuilder != null) {
      this.requestParams = docBuilder.requestParameters.requestParams;
      dataImporter = docBuilder.dataImporter;
    }
    globalSession = global;
    parent = parentContext;
  }

  @Override
  public String getEntityAttribute(String name) {
    return entity == null ? null : entity.allAttributes.get(name);
  }

  @Override
  public String getResolvedEntityAttribute(String name) {
    return entity == null ? null : resolver.replaceTokens(entity.allAttributes.get(name));
  }

  @Override
  public List<Map<String, String>> getAllEntityFields() {
    return entity == null ? Collections.EMPTY_LIST : entity.allFieldsList;
  }

  @Override
  public VariableResolver getVariableResolver() {
    return resolver;
  }

  @Override
  public DataSource getDataSource() {
    if (ds != null) return ds;
    if(entity == null) return  null;
    if (entity.dataSrc == null) {
      entity.dataSrc = dataImporter.getDataSourceInstance(entity, entity.dataSource, this);
    }
    if (entity.dataSrc != null && docBuilder != null && docBuilder.verboseDebug &&
             Context.FULL_DUMP.equals(currentProcess())) {
      //debug is not yet implemented properly for deltas
      entity.dataSrc = docBuilder.writer.getDebugLogger().wrapDs(entity.dataSrc);
    }
    return entity.dataSrc;
  }

  @Override
  public DataSource getDataSource(String name) {
    return dataImporter.getDataSourceInstance(entity, name, this);
  }

  @Override
  public boolean isRootEntity() {
    return entity.isDocRoot;
  }

  @Override
  public String currentProcess() {
    return currProcess;
  }

  @Override
  public Map<String, Object> getRequestParameters() {
    return requestParams;
  }

  @Override
  public EntityProcessor getEntityProcessor() {
    return entity == null ? null : entity.processor;
  }

  @Override
  public void setSessionAttribute(String name, Object val, String scope) {
    if(name == null) return;
    if (Context.SCOPE_ENTITY.equals(scope)) {
      if (entitySession == null)
        entitySession = new ConcurrentHashMap<String, Object>();

      putVal(name, val,entitySession);
    } else if (Context.SCOPE_GLOBAL.equals(scope)) {
      if (globalSession != null) {
        putVal(name, val,globalSession);
      }
    } else if (Context.SCOPE_DOC.equals(scope)) {
      DocBuilder.DocWrapper doc = getDocument();
      if (doc != null)
        doc.setSessionAttribute(name, val);
    } else if (SCOPE_SOLR_CORE.equals(scope)){
      if(dataImporter != null) {
        putVal(name, val,dataImporter.getCoreScopeSession());
      }
    }
  }

  private void putVal(String name, Object val, Map map) {
    if(val == null) map.remove(name);
    else entitySession.put(name, val);
  }

  @Override
  public Object getSessionAttribute(String name, String scope) {
    if (Context.SCOPE_ENTITY.equals(scope)) {
      if (entitySession == null)
        return null;
      return entitySession.get(name);
    } else if (Context.SCOPE_GLOBAL.equals(scope)) {
      if (globalSession != null) {
        return globalSession.get(name);
      }
    } else if (Context.SCOPE_DOC.equals(scope)) {
      DocBuilder.DocWrapper doc = getDocument();      
      return doc == null ? null: doc.getSessionAttribute(name);
    } else if (SCOPE_SOLR_CORE.equals(scope)){
       return dataImporter == null ? null : dataImporter.getCoreScopeSession().get(name);
    }
    return null;
  }

  @Override
  public Context getParentContext() {
    return parent;
  }

  private DocBuilder.DocWrapper getDocument() {
    ContextImpl c = this;
    while (true) {
      if (c.doc != null)
        return c.doc;
      if (c.parent != null)
        c = c.parent;
      else
        return null;
    }
  }

  public void setDoc(DocBuilder.DocWrapper docWrapper) {
    this.doc = docWrapper;
  }


  @Override
  public SolrCore getSolrCore() {
    return dataImporter == null ? null : dataImporter.getCore();
  }


  @Override
  public Map<String, Object> getStats() {
    return docBuilder != null ? docBuilder.importStatistics.getStatsSnapshot() : Collections.<String, Object>emptyMap();
  }

  @Override
  public String getScript() {
    if(dataImporter != null) {
      DataConfig.Script script = dataImporter.getConfig().script;
      return script == null ? null : script.text;
    }
    return null;
  }

  @Override
  public String getScriptLanguage() {
    if (dataImporter != null) {
      DataConfig.Script script = dataImporter.getConfig().script;
      return script == null ? null : script.language;
    }
    return null;
  }

  @Override
  public void deleteDoc(String id) {
    if(docBuilder != null){
      docBuilder.writer.deleteDoc(id);
    }
  }

  @Override
  public void deleteDocByQuery(String query) {
    if(docBuilder != null){
      docBuilder.writer.deleteByQuery(query);
    } 
  }

  DocBuilder getDocBuilder(){
    return docBuilder;
  }
  @Override
  public Object resolve(String var) {
    return resolver.resolve(var);
  }

  @Override
  public String replaceTokens(String template) {
    return resolver.replaceTokens(template);
  }
}

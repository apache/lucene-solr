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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * An implementation for the Context
 * </p>
 * <b>This API is experimental and subject to change</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class ContextImpl extends Context {
  private DataConfig.Entity entity;

  private ContextImpl parent;

  private VariableResolverImpl resolver;

  private DataSource ds;

  private int currProcess;

  private Map<String, Object> requestParams;

  private DataImporter dataImporter;

  private Map<String, Object> entitySession, globalSession, docSession;

  DocBuilder docBuilder;

  public ContextImpl(DataConfig.Entity entity, VariableResolverImpl resolver,
                     DataSource ds, int currProcess,
                     Map<String, Object> global, ContextImpl parentContext, DocBuilder docBuilder) {
    this.entity = entity;
    this.resolver = resolver;
    this.ds = ds;
    this.currProcess = currProcess;
    this.docBuilder = docBuilder;
    if (docBuilder != null) {
      this.requestParams = docBuilder.requestParameters.requestParams;
      dataImporter = docBuilder.dataImporter;
    }
    globalSession = global;
    parent = parentContext;
  }

  public String getEntityAttribute(String name) {
    return entity == null ? null : entity.allAttributes.get(name);
  }

  public List<Map<String, String>> getAllEntityFields() {
    return entity == null ? Collections.EMPTY_LIST : entity.allFieldsList;
  }

  public VariableResolver getVariableResolver() {
    return resolver;
  }

  public DataSource getDataSource() {
    if (ds != null) return ds;
    if (entity.dataSrc == null) {
      entity.dataSrc = dataImporter.getDataSourceInstance(entity, entity.dataSource, this);
    }
    if (entity.dataSrc != null && docBuilder != null && docBuilder.verboseDebug &&
            currProcess == Context.FULL_DUMP) {
      //debug is not yet implemented properly for deltas
      return DebugLogger.wrapDs(entity.dataSrc);
    }
    return entity.dataSrc;
  }

  public DataSource getDataSource(String name) {
    return dataImporter.getDataSourceInstance(entity, name, this);
  }

  public boolean isRootEntity() {
    return entity.isDocRoot;
  }

  public int currentProcess() {
    return currProcess;
  }

  public Map<String, Object> getRequestParameters() {
    return requestParams;
  }

  public EntityProcessor getEntityProcessor() {
    return entity == null ? null : entity.processor;
  }

  public void setSessionAttribute(String name, Object val, String scope) {
    if (Context.SCOPE_ENTITY.equals(scope)) {
      if (entitySession == null)
        entitySession = new HashMap<String, Object>();
      entitySession.put(name, val);
    } else if (Context.SCOPE_GLOBAL.equals(scope)) {
      if (globalSession != null) {
        globalSession.put(name, val);
      }
    } else if (Context.SCOPE_DOC.equals(scope)) {
      Map<String, Object> docsession = getDocSession();
      if (docsession != null)
        docsession.put(name, val);
    } else if (SCOPE_SOLR_CORE.equals(scope)){
      if(dataImporter != null) dataImporter.getCoreScopeSession().put(name, val);
    }
  }

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
      Map<String, Object> docsession = getDocSession();
      if (docsession != null)
        return docsession.get(name);
    } else if (SCOPE_SOLR_CORE.equals(scope)){
       return dataImporter == null ? null : dataImporter.getCoreScopeSession().get(name);
    }
    return null;
  }

  public Context getParentContext() {
    return parent;
  }

  public Map<String, Object> getDocSession() {
    ContextImpl c = this;
    while (true) {
      if (c.docSession != null)
        return c.docSession;
      if (c.parent != null)
        c = c.parent;
      else
        return null;
    }
  }

  public void setDocSession(Map<String, Object> docSession) {
    this.docSession = docSession;
  }


  public SolrCore getSolrCore() {
    return dataImporter == null ? null : dataImporter.getCore();
  }


  public Map<String, Object> getStats() {
    return docBuilder != null ? docBuilder.importStatistics.getStatsSnapshot() : Collections.<String, Object>emptyMap();
  }

  public String getScript() {
    if(dataImporter != null) {
      DataConfig.Script script = dataImporter.getConfig().script;
      return script == null ? null : script.text;
    }
    return null;
  }

  public String getScriptLanguage() {
    if (dataImporter != null) {
      DataConfig.Script script = dataImporter.getConfig().script;
      return script == null ? null : script.language;
    }
    return null;
  }

  public void deleteDoc(String id) {
    if(docBuilder != null){
      docBuilder.writer.deleteDoc(id);
    }
  }

  public void deleteDocByQuery(String query) {
    if(docBuilder != null){
      docBuilder.writer.deleteByQuery(query);
    } 
  }
}

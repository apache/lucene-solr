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
package org.apache.solr.handler.dataimport;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.dataimport.config.Script;

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
 * @since solr 1.3
 */
public class ContextImpl extends Context {
  protected EntityProcessorWrapper epw;

  private ContextImpl parent;

  private VariableResolver resolver;

  @SuppressWarnings({"rawtypes"})
  private DataSource ds;

  private String currProcess;

  private Map<String, Object> requestParams;

  private DataImporter dataImporter;

  private Map<String, Object> entitySession, globalSession;

  private Exception lastException = null;

  DocBuilder.DocWrapper doc;

  DocBuilder docBuilder;



  public ContextImpl(EntityProcessorWrapper epw, VariableResolver resolver,
                     @SuppressWarnings({"rawtypes"})DataSource ds, String currProcess,
                     Map<String, Object> global, ContextImpl parentContext, DocBuilder docBuilder) {
    this.epw = epw;
    this.docBuilder = docBuilder;
    this.resolver = resolver;
    this.ds = ds;
    this.currProcess = currProcess;
    if (docBuilder != null) {
      this.requestParams = docBuilder.getReqParams().getRawParams();
      dataImporter = docBuilder.dataImporter;
    }
    globalSession = global;
    parent = parentContext;
  }

  @Override
  public String getEntityAttribute(String name) {
    return epw==null || epw.getEntity() == null ? null : epw.getEntity().getAllAttributes().get(name);
  }

  @Override
  public String getResolvedEntityAttribute(String name) {
    return epw==null || epw.getEntity() == null ? null : resolver.replaceTokens(epw.getEntity().getAllAttributes().get(name));
  }

  @Override
  public List<Map<String, String>> getAllEntityFields() {
    return epw==null || epw.getEntity() == null ? Collections.emptyList() : epw.getEntity().getAllFieldsList();
  }

  @Override
  public VariableResolver getVariableResolver() {
    return resolver;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public DataSource getDataSource() {
    if (ds != null) return ds;
    if(epw==null) { return null; }
    if (epw!=null && epw.getDatasource() == null) {
      epw.setDatasource(dataImporter.getDataSourceInstance(epw.getEntity(), epw.getEntity().getDataSourceName(), this));
    }
    if (epw!=null && epw.getDatasource() != null && docBuilder != null && docBuilder.verboseDebug &&
             Context.FULL_DUMP.equals(currentProcess())) {
      //debug is not yet implemented properly for deltas
      epw.setDatasource(docBuilder.getDebugLogger().wrapDs(epw.getDatasource()));
    }
    return epw.getDatasource();
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public DataSource getDataSource(String name) {
    return dataImporter.getDataSourceInstance(epw==null ? null : epw.getEntity(), name, this);
  }

  @Override
  public boolean isRootEntity() {
    return epw==null ? false : epw.getEntity().isDocRoot();
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
    return epw;
  }

  @Override
  public void setSessionAttribute(String name, Object val, String scope) {
    if(name == null) {
      return;
    }
    if (Context.SCOPE_ENTITY.equals(scope)) {
      if (entitySession == null) {
        entitySession = new HashMap<>();
      }
      entitySession.put(name, val);
    } else if (Context.SCOPE_GLOBAL.equals(scope)) {
      if (globalSession != null) {
        globalSession.put(name, val);
      }
    } else if (Context.SCOPE_DOC.equals(scope)) {
      DocBuilder.DocWrapper doc = getDocument();
      if (doc != null) {
        doc.setSessionAttribute(name, val);
      }
    } else if (SCOPE_SOLR_CORE.equals(scope)){
      if(dataImporter != null) {
        dataImporter.putToCoreScopeSession(name, val);
      }
    }
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
       return dataImporter == null ? null : dataImporter.getFromCoreScopeSession(name);
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

  void setDoc(DocBuilder.DocWrapper docWrapper) {
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
    if (dataImporter != null) {
      Script script = dataImporter.getConfig().getScript();
      return script == null ? null : script.getText();
    }
    return null;
  }
  
  @Override
  public String getScriptLanguage() {
    if (dataImporter != null) {
      Script script = dataImporter.getConfig().getScript();
      return script == null ? null : script.getLanguage();
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

  public Exception getLastException() { return lastException; }

  public void setLastException(Exception lastException) {this.lastException = lastException; }
}

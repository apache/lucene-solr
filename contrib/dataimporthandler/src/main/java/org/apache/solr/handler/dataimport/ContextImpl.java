package org.apache.solr.handler.dataimport;

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

  public ContextImpl(DataConfig.Entity entity, VariableResolverImpl resolver,
                     DataSource ds, int currProcess, Map<String, Object> requestParams,
                     Map<String, Object> global, ContextImpl p, DataImporter di) {
    this.entity = entity;
    this.resolver = resolver;
    this.ds = ds;
    this.currProcess = currProcess;
    this.requestParams = requestParams;
    globalSession = global;
    parent = p;
    dataImporter = di;
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
    return ds;
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
    return dataImporter.getCore();
  }
}

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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.dataimport.config.ConfigNameConstants;
import org.apache.solr.handler.dataimport.config.Entity;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.*;
import static org.apache.solr.handler.dataimport.EntityProcessorBase.*;
import static org.apache.solr.handler.dataimport.EntityProcessorBase.SKIP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A Wrapper over {@link EntityProcessor} instance which performs transforms and handles multi-row outputs correctly.
 *
 * @since solr 1.4
 */
public class EntityProcessorWrapper extends EntityProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private EntityProcessor delegate;
  private Entity entity;
  @SuppressWarnings({"rawtypes"})
  private DataSource datasource;
  private List<EntityProcessorWrapper> children = new ArrayList<>();
  private DocBuilder docBuilder;
  private boolean initialized;
  private String onError;
  private Context context;
  private VariableResolver resolver;
  private String entityName;

  protected List<Transformer> transformers;

  protected List<Map<String, Object>> rowcache;
  
  public EntityProcessorWrapper(EntityProcessor delegate, Entity entity, DocBuilder docBuilder) {
    this.delegate = delegate;
    this.entity = entity;
    this.docBuilder = docBuilder;
  }

  @Override
  public void init(Context context) {
    rowcache = null;
    this.context = context;
    resolver = context.getVariableResolver();
    if (entityName == null) {
      onError = resolver.replaceTokens(context.getEntityAttribute(ON_ERROR));
      if (onError == null) onError = ABORT;
      entityName = context.getEntityAttribute(ConfigNameConstants.NAME);
    }
    delegate.init(context);

  }

  @SuppressWarnings({"unchecked"})
  void loadTransformers() {
    String transClasses = context.getEntityAttribute(TRANSFORMER);

    if (transClasses == null) {
      transformers = Collections.emptyList();
      return;
    }

    String[] transArr = transClasses.split(",");
    transformers = new ArrayList<Transformer>() {
      @Override
      public boolean add(Transformer transformer) {
        if (docBuilder != null && docBuilder.verboseDebug) {
          transformer = docBuilder.getDebugLogger().wrapTransformer(transformer);
        }
        return super.add(transformer);
      }
    };
    for (String aTransArr : transArr) {
      String trans = aTransArr.trim();
      if (trans.startsWith("script:")) {
        // The script transformer is a potential vulnerability, esp. when the script is
        // provided from an untrusted source. Check and don't proceed if source is untrusted.
        checkIfTrusted(trans);
        String functionName = trans.substring("script:".length());
        ScriptTransformer scriptTransformer = new ScriptTransformer();
        scriptTransformer.setFunctionName(functionName);
        transformers.add(scriptTransformer);
        continue;
      }
      try {
        @SuppressWarnings({"rawtypes"})
        Class clazz = DocBuilder.loadClass(trans, context.getSolrCore());
        if (Transformer.class.isAssignableFrom(clazz)) {
          transformers.add((Transformer) clazz.newInstance());
        } else {
          Method meth = clazz.getMethod(TRANSFORM_ROW, Map.class);
          transformers.add(new ReflectionTransformer(meth, clazz, trans));
        }
      } catch (NoSuchMethodException nsme){
         String msg = "Transformer :"
                    + trans
                    + "does not implement Transformer interface or does not have a transformRow(Map<String.Object> m)method";
            log.error(msg);
            wrapAndThrow(SEVERE, nsme,msg);        
      } catch (Exception e) {
        log.error("Unable to load Transformer: {}", aTransArr, e);
        wrapAndThrow(SEVERE, e,"Unable to load Transformer: " + trans);
      }
    }

  }

  private void checkIfTrusted(String trans) {
    if (docBuilder != null) {
      SolrCore core = docBuilder.dataImporter.getCore();
      boolean trusted = (core != null)? core.getCoreDescriptor().isConfigSetTrusted(): true;
      if (!trusted) {
        Exception ex = new SolrException(ErrorCode.UNAUTHORIZED, "The configset for this collection was uploaded "
            + "without any authentication in place,"
            + " and this transformer is not available for collections with untrusted configsets. To use this transformer,"
            + " re-upload the configset after enabling authentication and authorization.");
        String msg = "Transformer: "
            + trans
            + ". " + ex.getMessage();
        log.error(msg);
        wrapAndThrow(SEVERE, ex, msg);
      }
    }
  }

  @SuppressWarnings("unchecked")
  static class ReflectionTransformer extends Transformer {
    final Method meth;

    @SuppressWarnings({"rawtypes"})
    final Class clazz;

    final String trans;

    final Object o;

    public ReflectionTransformer(Method meth, @SuppressWarnings({"rawtypes"})Class clazz, String trans)
            throws Exception {
      this.meth = meth;
      this.clazz = clazz;
      this.trans = trans;
      o = clazz.newInstance();
    }

    @Override
    public Object transformRow(Map<String, Object> aRow, Context context) {
      try {
        return meth.invoke(o, aRow);
      } catch (Exception e) {
        log.warn("method invocation failed on transformer : {}", trans, e);
        throw new DataImportHandlerException(WARN, e);
      }
    }
  }

  protected Map<String, Object> getFromRowCache() {
    Map<String, Object> r = rowcache.remove(0);
    if (rowcache.isEmpty())
      rowcache = null;
    return r;
  }

  @SuppressWarnings("unchecked")
  protected Map<String, Object> applyTransformer(Map<String, Object> row) {
    if(row == null) return null;
    if (transformers == null)
      loadTransformers();
    if (transformers == Collections.EMPTY_LIST)
      return row;
    Map<String, Object> transformedRow = row;
    List<Map<String, Object>> rows = null;
    boolean stopTransform = checkStopTransform(row);
    VariableResolver resolver = context.getVariableResolver();
    for (Transformer t : transformers) {
      if (stopTransform) break;
      try {
        if (rows != null) {
          List<Map<String, Object>> tmpRows = new ArrayList<>();
          for (Map<String, Object> map : rows) {
            resolver.addNamespace(entityName, map);
            Object o = t.transformRow(map, context);
            if (o == null)
              continue;
            if (o instanceof Map) {
              @SuppressWarnings({"rawtypes"})
              Map oMap = (Map) o;
              stopTransform = checkStopTransform(oMap);
              tmpRows.add((Map) o);
            } else if (o instanceof List) {
              tmpRows.addAll((List) o);
            } else {
              log.error("Transformer must return Map<String, Object> or a List<Map<String, Object>>");
            }
          }
          rows = tmpRows;
        } else {
          resolver.addNamespace(entityName, transformedRow);
          Object o = t.transformRow(transformedRow, context);
          if (o == null)
            return null;
          if (o instanceof Map) {
            @SuppressWarnings({"rawtypes"})
            Map oMap = (Map) o;
            stopTransform = checkStopTransform(oMap);
            transformedRow = (Map) o;
          } else if (o instanceof List) {
            rows = (List) o;
          } else {
            log.error("Transformer must return Map<String, Object> or a List<Map<String, Object>>");
          }
        }
      } catch (Exception e) {
        log.warn("transformer threw error", e);
        if (ABORT.equals(onError)) {
          wrapAndThrow(SEVERE, e);
        } else if (SKIP.equals(onError)) {
          wrapAndThrow(DataImportHandlerException.SKIP, e);
        }
        // onError = continue
      }
    }
    if (rows == null) {
      return transformedRow;
    } else {
      rowcache = rows;
      return getFromRowCache();
    }

  }

  private boolean checkStopTransform(@SuppressWarnings({"rawtypes"})Map oMap) {
    return oMap.get("$stopTransform") != null
            && Boolean.parseBoolean(oMap.get("$stopTransform").toString());
  }

  @Override
  public Map<String, Object> nextRow() {
    if (rowcache != null) {
      return getFromRowCache();
    }
    while (true) {
      Map<String, Object> arow = null;
      try {
        arow = delegate.nextRow();
      } catch (Exception e) {
        if(ABORT.equals(onError)){
          wrapAndThrow(SEVERE, e);
        } else {
          //SKIP is not really possible. If this calls the nextRow() again the Entityprocessor would be in an inconisttent state           
          SolrException.log(log, "Exception in entity : "+ entityName, e);
          return null;
        }
      }
      if (arow == null) {
        return null;
      } else {
        arow = applyTransformer(arow);
        if (arow != null) {
          delegate.postTransform(arow);
          return arow;
        }
      }
    }
  }

  @Override
  public Map<String, Object> nextModifiedRowKey() {
    Map<String, Object> row = delegate.nextModifiedRowKey();
    row = applyTransformer(row);
    rowcache = null;
    return row;
  }

  @Override
  public Map<String, Object> nextDeletedRowKey() {
    Map<String, Object> row = delegate.nextDeletedRowKey();
    row = applyTransformer(row);
    rowcache = null;
    return row;
  }

  @Override
  public Map<String, Object> nextModifiedParentRowKey() {
    return delegate.nextModifiedParentRowKey();
  }

  @Override
  public void destroy() {
    delegate.destroy();
  }

  public VariableResolver getVariableResolver() {
    return context.getVariableResolver();
  }

  public Context getContext() {
    return context;
  }

  @Override
  public void close() {
    delegate.close();
  }

  public Entity getEntity() {
    return entity;
  }

  public List<EntityProcessorWrapper> getChildren() {
    return children;
  }

  @SuppressWarnings({"rawtypes"})
  public DataSource getDatasource() {
    return datasource;
  }

  public void setDatasource(@SuppressWarnings({"rawtypes"})DataSource datasource) {
    this.datasource = datasource;
  }

  public boolean isInitialized() {
    return initialized;
  }

  public void setInitialized(boolean initialized) {
    this.initialized = initialized;
  }
}

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
package org.apache.solr.handler.dataimport;

import java.lang.reflect.Method;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * Base class for all implementations of EntityProcessor
 * </p>
 * <p/>
 * <p>
 * Most implementations of EntityProcessor extend this base class which provides
 * common functionality.
 * </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class EntityProcessorBase extends EntityProcessor {
  private static final Logger LOG = Logger.getLogger(EntityProcessorBase.class
          .getName());

  protected String entityName;

  protected Context context;

  protected VariableResolverImpl resolver;

  protected Iterator<Map<String, Object>> rowIterator;

  protected List<Transformer> transformers;

  protected List<Map<String, Object>> rowcache;

  protected String query;

  @SuppressWarnings("unchecked")
  private Map session;

  public void init(Context context) {
    rowIterator = null;
    rowcache = null;
    this.context = context;
    entityName = context.getEntityAttribute("name");
    resolver = (VariableResolverImpl) context.getVariableResolver();
    query = null;
    session = null;

  }

  @SuppressWarnings("unchecked")
  void loadTransformers() {
    String transClasses = context.getEntityAttribute(TRANSFORMER);

    if (transClasses == null) {
      transformers = Collections.EMPTY_LIST;
      return;
    }

    String[] transArr = transClasses.split(",");
    transformers = new ArrayList<Transformer>() {
      public boolean add(Transformer transformer) {
        return super.add(DebugLogger.wrapTransformer(transformer));
      }
    };
    for (String aTransArr : transArr) {
      String trans = aTransArr.trim();
      if (trans.startsWith("script:")) {
        String functionName = trans.substring("script:".length());
        ScriptTransformer scriptTransformer = new ScriptTransformer();
        scriptTransformer.setFunctionName(functionName);
        transformers.add(scriptTransformer);
        continue;
      }
      try {
        Class clazz = DocBuilder.loadClass(trans);
        if (clazz.newInstance() instanceof Transformer) {
          transformers.add((Transformer) clazz.newInstance());
        } else {
          final Method meth = clazz.getMethod(TRANSFORM_ROW, Map.class);
          if (meth == null) {
            String msg = "Transformer :"
                    + trans
                    + "does not implement Transformer interface or does not have a transformRow(Map m)method";
            LOG.log(Level.SEVERE, msg);
            throw new DataImportHandlerException(
                    DataImportHandlerException.SEVERE, msg);
          }
          transformers.add(new ReflectionTransformer(meth, clazz, trans));
        }
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Unable to load Transformer: " + aTransArr, e);
        throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                e);
      }
    }

  }

  @SuppressWarnings("unchecked")
  static class ReflectionTransformer extends Transformer {
    final Method meth;

    final Class clazz;

    final String trans;

    final Object o;

    public ReflectionTransformer(Method meth, Class clazz, String trans)
            throws Exception {
      this.meth = meth;
      this.clazz = clazz;
      this.trans = trans;
      o = clazz.newInstance();
    }

    public Object transformRow(Map<String, Object> aRow, Context context) {
      try {
        return meth.invoke(o, aRow);
      } catch (Exception e) {
        LOG.log(Level.WARNING, "method invocation failed on transformer : "
                + trans, e);
        throw new DataImportHandlerException(DataImportHandlerException.WARN, e);
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
    if (transformers == null)
      loadTransformers();
    if (transformers == Collections.EMPTY_LIST)
      return row;
    Map<String, Object> transformedRow = row;
    List<Map<String, Object>> rows = null;
    for (Transformer t : transformers) {
      try {
        if (rows != null) {
          List<Map<String, Object>> tmpRows = new ArrayList<Map<String, Object>>();
          for (Map<String, Object> map : rows) {
            Object o = t.transformRow(map, context);
            if (o == null)
              continue;
            if (o instanceof Map) {
              Map oMap = (Map) o;
              checkSkipDoc(oMap, t);
              tmpRows.add((Map) o);
            } else if (o instanceof List) {
              tmpRows.addAll((List) o);
            } else {
              LOG
                      .log(Level.SEVERE,
                              "Transformer must return Map<String, Object> or a List<Map<String, Object>>");
            }
          }
          rows = tmpRows;
        } else {
          Object o = t.transformRow(transformedRow, context);
          if (o == null)
            return null;
          if (o instanceof Map) {
            Map oMap = (Map) o;
            checkSkipDoc(oMap, t);
            transformedRow = (Map) o;
          } else if (o instanceof List) {
            rows = (List) o;
          } else {
            LOG
                    .log(Level.SEVERE,
                            "Transformer must return Map<String, Object> or a List<Map<String, Object>>");
          }
        }

      } catch (DataImportHandlerException e) {
        throw e;
      } catch (Exception e) {
        LOG.log(Level.WARNING, "transformer threw error", e);
        throw new DataImportHandlerException(DataImportHandlerException.WARN, e);
      }
    }
    if (rows == null) {
      return transformedRow;
    } else {
      rowcache = rows;
      return getFromRowCache();
    }

  }

  private void checkSkipDoc(Map oMap, Transformer t) {
    if (oMap.get(SKIP_DOC) != null
            && Boolean.parseBoolean(oMap.get(SKIP_DOC).toString()))
      throw new DataImportHandlerException(DataImportHandlerException.SKIP,
              "Document skipped by: " + DebugLogger.getTransformerName(t));
  }

  protected Map<String, Object> getNext() {
    try {
      if (rowIterator == null)
        return null;
      if (rowIterator.hasNext())
        return rowIterator.next();
      rowIterator = null;
      query = null;
      return null;
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "getNext() failed for query '" + query + "'", e);
      rowIterator = null;
      query = null;
      throw new DataImportHandlerException(DataImportHandlerException.WARN, e);
    }
  }

  public Map<String, Object> nextModifiedRowKey() {
    return null;
  }

  public Map<String, Object> nextDeletedRowKey() {
    return null;
  }

  public Map<String, Object> nextModifiedParentRowKey() {
    return null;
  }

  public void setSessionAttribute(Object key, Object val) {
    if (session == null) {
      session = new HashMap();
    }
    session.put(key, val);
  }

  public Object getSessionAttribute(Object key) {
    if (session == null)
      return null;
    return session.get(key);
  }

  /**
   * For a simple implementation, this is the only method that the sub-class
   * should implement. This is intended to stream rows one-by-one. Return null
   * to signal end of rows
   *
   * @return a row where the key is the name of the field and value can be any
   *         Object or a Collection of objects. Return null to signal end of
   *         rows
   */
  public Map<String, Object> nextRow() {
    return null;// do not do anything
  }


  public void destroy() {
    /*no op*/
  }

  /**
   * Clears the internal session maintained by this EntityProcessor
   */
  public void clearSession() {
    if (session != null)
      session.clear();
  }

  /**
   * Only used by cache implementations
   */
  protected String cachePk;

  /**
   * Only used by cache implementations
   */
  protected String cacheVariableName;

  /**
   * Only used by cache implementations
   */
  protected Map<String, List<Map<String, Object>>> simpleCache;

  /**
   * Only used by cache implementations
   */
  protected Map<String, Map<Object, List<Map<String, Object>>>> cacheWithWhereClause;

  protected List<Map<String, Object>> dataSourceRowCache;

  /**
   * Only used by cache implementations
   */
  protected void cacheInit() {
    if (simpleCache != null || cacheWithWhereClause != null)
      return;
    String where = context.getEntityAttribute("where");
    if (where == null) {
      simpleCache = new HashMap<String, List<Map<String, Object>>>();
    } else {
      String[] splits = where.split("=");
      cachePk = splits[0];
      cacheVariableName = splits[1].trim();
      cacheWithWhereClause = new HashMap<String, Map<Object, List<Map<String, Object>>>>();
    }
  }

  /**
   * If the where clause is present the cache is sql Vs Map of key Vs List of
   * Rows. Only used by cache implementations.
   *
   * @param query
   * @return
   */
  protected Map<String, Object> getIdCacheData(String query) {
    Map<Object, List<Map<String, Object>>> rowIdVsRows = cacheWithWhereClause
            .get(query);
    List<Map<String, Object>> rows = null;
    Object key = resolver.resolve(cacheVariableName);
    if (rowIdVsRows != null) {
      rows = rowIdVsRows.get(key);
      if (rows == null)
        return null;
      dataSourceRowCache = new ArrayList<Map<String, Object>>(rows);
      return getFromRowCacheTransformed();
    } else {
      rows = getAllNonCachedRows();
      if (rows.isEmpty()) {
        return null;
      } else {
        rowIdVsRows = new HashMap<Object, List<Map<String, Object>>>();
        for (Map<String, Object> row : rows) {
          Object k = row.get(cachePk);
          if (rowIdVsRows.get(k) == null)
            rowIdVsRows.put(k, new ArrayList<Map<String, Object>>());
          rowIdVsRows.get(k).add(row);
        }
        cacheWithWhereClause.put(query, rowIdVsRows);
        if (!rowIdVsRows.containsKey(key))
          return null;
        dataSourceRowCache = new ArrayList<Map<String, Object>>(rowIdVsRows.get(key));
        if (dataSourceRowCache.isEmpty()) {
          dataSourceRowCache = null;
          return null;
        }
        return getFromRowCacheTransformed();
      }
    }
  }

  /**
   * Get all the rows from the the datasource for the given query. Only used by
   * cache implementations.
   * <p/>
   * This <b>must</b> be implemented by sub-classes which intend to provide a
   * cached implementation
   *
   * @return
   */
  protected List<Map<String, Object>> getAllNonCachedRows() {
    return Collections.EMPTY_LIST;
  }

  /**
   * If where clause is not present the cache is a Map of query vs List of Rows.
   * Only used by cache implementations.
   *
   * @return
   */
  protected Map<String, Object> getSimplCacheData(String query) {
    List<Map<String, Object>> rows = simpleCache.get(query);
    if (rows != null) {
      dataSourceRowCache = new ArrayList<Map<String, Object>>(rows);
      return getFromRowCacheTransformed();
    } else {
      rows = getAllNonCachedRows();
      if (rows.isEmpty()) {
        return null;
      } else {
        dataSourceRowCache = new ArrayList<Map<String, Object>>(rows);
        simpleCache.put(query, rows);
        return getFromRowCacheTransformed();
      }
    }
  }

  protected Map<String, Object> getFromRowCacheTransformed() {
    Map<String, Object> r = dataSourceRowCache.remove(0);
    if (dataSourceRowCache.isEmpty())
      dataSourceRowCache = null;
    return r == null ? null : applyTransformer(r);
  }

  public static final String TRANSFORMER = "transformer";

  public static final String TRANSFORM_ROW = "transformRow";

  public static final String SKIP_DOC = "$skipDoc";
}

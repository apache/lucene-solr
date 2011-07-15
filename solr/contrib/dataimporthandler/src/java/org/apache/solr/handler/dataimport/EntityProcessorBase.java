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

import org.apache.solr.common.SolrException;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * <p> Base class for all implementations of {@link EntityProcessor} </p> <p/> <p> Most implementations of {@link EntityProcessor}
 * extend this base class which provides common functionality. </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
public class EntityProcessorBase extends EntityProcessor {
  private static final Logger log = LoggerFactory.getLogger(EntityProcessorBase.class);

  protected boolean isFirstInit = true;

  protected String entityName;

  protected Context context;

  protected Iterator<Map<String, Object>> rowIterator;

  protected List<Transformer> transformers;

  protected String query;

  protected String onError = ABORT;


  @Override
  public void init(Context context) {
    rowIterator = null;
    this.context = context;
    if (isFirstInit) {
      firstInit(context);
    }
    query = null;
  }

  /**first time init call. do one-time operations here
   */
  protected void firstInit(Context context) {
    entityName = context.getEntityAttribute("name");
    String s = context.getEntityAttribute(ON_ERROR);
    if (s != null) onError = s;
    isFirstInit = false;
  }


  protected Map<String, Object> getNext() {
    try {
      if (rowIterator == null)
        return null;
      if (rowIterator.hasNext())
        return rowIterator.next();
      query = null;
      rowIterator = null;
      return null;
    } catch (Exception e) {
      SolrException.log(log, "getNext() failed for query '" + query + "'", e);
      query = null;
      rowIterator = null;
      wrapAndThrow(DataImportHandlerException.WARN, e);
      return null;
    }
  }

  @Override
  public Map<String, Object> nextModifiedRowKey() {
    return null;
  }

  @Override
  public Map<String, Object> nextDeletedRowKey() {
    return null;
  }

  @Override
  public Map<String, Object> nextModifiedParentRowKey() {
    return null;
  }

  /**
   * For a simple implementation, this is the only method that the sub-class should implement. This is intended to
   * stream rows one-by-one. Return null to signal end of rows
   *
   * @return a row where the key is the name of the field and value can be any Object or a Collection of objects. Return
   *         null to signal end of rows
   */
  @Override
  public Map<String, Object> nextRow() {
    return null;// do not do anything
  }


  @Override
  public void destroy() {
    /*no op*/
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

    String cacheKey = context.getEntityAttribute(CACHE_KEY);
    String lookupKey = context.getEntityAttribute(CACHE_LOOKUP);
    if(cacheKey != null && lookupKey == null){
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "'cacheKey' is specified for the entity "+ entityName+" but 'cacheLookup' is missing" );

    }
    if (where == null && cacheKey == null) {
      simpleCache = new HashMap<String, List<Map<String, Object>>>();
    } else {
      if (where != null) {
        String[] splits = where.split("=");
        cachePk = splits[0];
        cacheVariableName = splits[1].trim();
      } else {
        cachePk = cacheKey;
        cacheVariableName = lookupKey;
      }
      cacheWithWhereClause = new HashMap<String, Map<Object, List<Map<String, Object>>>>();
    }
  }

  /**
   * If the where clause is present the cache is sql Vs Map of key Vs List of Rows. Only used by cache implementations.
   *
   * @param query the query string for which cached data is to be returned
   *
   * @return the cached row corresponding to the given query after all variables have been resolved
   */
  protected Map<String, Object> getIdCacheData(String query) {
    Map<Object, List<Map<String, Object>>> rowIdVsRows = cacheWithWhereClause
            .get(query);
    List<Map<String, Object>> rows = null;
    Object key = context.resolve(cacheVariableName);
    if (key == null) {
      throw new DataImportHandlerException(DataImportHandlerException.WARN,
              "The cache lookup value : " + cacheVariableName + " is resolved to be null in the entity :" +
                      context.getEntityAttribute("name"));

    }
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
          if (k == null) {
            throw new DataImportHandlerException(DataImportHandlerException.WARN,
                    "No value available for the cache key : " + cachePk + " in the entity : " +
                            context.getEntityAttribute("name"));
          }
          if (!k.getClass().equals(key.getClass())) {
            throw new DataImportHandlerException(DataImportHandlerException.WARN,
                    "The key in the cache type : " + k.getClass().getName() +
                            "is not same as the lookup value type " + key.getClass().getName() + " in the entity " +
                            context.getEntityAttribute("name"));
          }
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
   * <p> Get all the rows from the the datasource for the given query. Only used by cache implementations. </p> This
   * <b>must</b> be implemented by sub-classes which intend to provide a cached implementation
   *
   * @return the list of all rows fetched from the datasource.
   */
  protected List<Map<String, Object>> getAllNonCachedRows() {
    return Collections.EMPTY_LIST;
  }

  /**
   * If where clause is not present the cache is a Map of query vs List of Rows. Only used by cache implementations.
   *
   * @param query string for which cached row is to be returned
   *
   * @return the cached row corresponding to the given query
   */
  protected Map<String, Object> getSimpleCacheData(String query) {
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
    return r;
  }

  public static final String TRANSFORMER = "transformer";

  public static final String TRANSFORM_ROW = "transformRow";

  public static final String ON_ERROR = "onError";

  public static final String ABORT = "abort";

  public static final String CONTINUE = "continue";

  public static final String SKIP = "skip";

  public static final String SKIP_DOC = "$skipDoc";

  public static final String CACHE_KEY = "cacheKey";
  
  public static final String CACHE_LOOKUP = "cacheLookup";

}

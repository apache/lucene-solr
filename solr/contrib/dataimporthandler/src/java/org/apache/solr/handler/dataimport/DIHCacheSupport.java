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

import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DIHCacheSupport {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String cacheForeignKey;
  private String cacheImplName;
  private Map<String,DIHCache> queryVsCache = new HashMap<>();
  private Map<String,Iterator<Map<String,Object>>> queryVsCacheIterator;
  private Iterator<Map<String,Object>> dataSourceRowCache;
  private boolean cacheDoKeyLookup;
  
  public DIHCacheSupport(Context context, String cacheImplName) {
    this.cacheImplName = cacheImplName;
    
    Relation r = new Relation(context);
    cacheDoKeyLookup = r.doKeyLookup;
    String cacheKey = r.primaryKey;
    cacheForeignKey = r.foreignKey;
    
    context.setSessionAttribute(DIHCacheSupport.CACHE_PRIMARY_KEY, cacheKey,
        Context.SCOPE_ENTITY);
    context.setSessionAttribute(DIHCacheSupport.CACHE_FOREIGN_KEY, cacheForeignKey,
        Context.SCOPE_ENTITY);
    context.setSessionAttribute(DIHCacheSupport.CACHE_DELETE_PRIOR_DATA,
        "true", Context.SCOPE_ENTITY);
    context.setSessionAttribute(DIHCacheSupport.CACHE_READ_ONLY, "false",
        Context.SCOPE_ENTITY);
  }
  
  static class Relation{
    protected final boolean doKeyLookup;
    protected final String foreignKey;
    protected final String primaryKey;
    
    public Relation(Context context) {
      String where = context.getEntityAttribute("where");
      String cacheKey = context.getEntityAttribute(DIHCacheSupport.CACHE_PRIMARY_KEY);
      String lookupKey = context.getEntityAttribute(DIHCacheSupport.CACHE_FOREIGN_KEY);
      if (cacheKey != null && lookupKey == null) {
        throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
            "'cacheKey' is specified for the entity "
                + context.getEntityAttribute("name")
                + " but 'cacheLookup' is missing");
        
      }
      if (where == null && cacheKey == null) {
        doKeyLookup = false;
        primaryKey = null;
        foreignKey = null;
      } else {
        if (where != null) {
          String[] splits = where.split("=");
          primaryKey = splits[0];
          foreignKey = splits[1].trim();
        } else {
          primaryKey = cacheKey;
          foreignKey = lookupKey;
        }
        doKeyLookup = true;
      }
    }

    @Override
    public String toString() {
      return "Relation "
          + primaryKey + "="+foreignKey  ;
    }
    
    
  }
  
  private DIHCache instantiateCache(Context context) {
    DIHCache cache = null;
    try {
      @SuppressWarnings("unchecked")
      Class<DIHCache> cacheClass = DocBuilder.loadClass(cacheImplName, context
          .getSolrCore());
      Constructor<DIHCache> constr = cacheClass.getConstructor();
      cache = constr.newInstance();
      cache.open(context);
    } catch (Exception e) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
          "Unable to load Cache implementation:" + cacheImplName, e);
    }
    return cache;
  }
  
  public void initNewParent(Context context) {
    dataSourceRowCache = null;
    queryVsCacheIterator = new HashMap<>();
    for (Map.Entry<String,DIHCache> entry : queryVsCache.entrySet()) {
      queryVsCacheIterator.put(entry.getKey(), entry.getValue().iterator());
    }
  }
  
  public void destroyAll() {
    if (queryVsCache != null) {
      for (DIHCache cache : queryVsCache.values()) {
        cache.destroy();
      }
    }
    queryVsCache = null;
    dataSourceRowCache = null;
    cacheForeignKey = null;
  }
  
  /**
   * <p>
   * Get all the rows from the datasource for the given query and cache them
   * </p>
   */
  public void populateCache(String query,
      Iterator<Map<String,Object>> rowIterator) {
    Map<String,Object> aRow = null;
    DIHCache cache = queryVsCache.get(query);
    while ((aRow = getNextFromCache(query, rowIterator)) != null) {
      cache.add(aRow);
    }
  }
  
  private Map<String,Object> getNextFromCache(String query,
      Iterator<Map<String,Object>> rowIterator) {
    try {
      if (rowIterator == null) return null;
      if (rowIterator.hasNext()) return rowIterator.next();
      return null;
    } catch (Exception e) {
      SolrException.log(log, "getNextFromCache() failed for query '" + query
          + "'", e);
      wrapAndThrow(DataImportHandlerException.WARN, e);
      return null;
    }
  }
  
  public Map<String,Object> getCacheData(Context context, String query,
      Iterator<Map<String,Object>> rowIterator) {
    if (cacheDoKeyLookup) {
      return getIdCacheData(context, query, rowIterator);
    } else {
      return getSimpleCacheData(context, query, rowIterator);
    }
  }
  
  /**
   * If the where clause is present the cache is sql Vs Map of key Vs List of
   * Rows.
   * 
   * @param query
   *          the query string for which cached data is to be returned
   * 
   * @return the cached row corresponding to the given query after all variables
   *         have been resolved
   */
  protected Map<String,Object> getIdCacheData(Context context, String query,
      Iterator<Map<String,Object>> rowIterator) {
    Object key = context.resolve(cacheForeignKey);
    if (key == null) {
      throw new DataImportHandlerException(DataImportHandlerException.WARN,
          "The cache lookup value : " + cacheForeignKey
              + " is resolved to be null in the entity :"
              + context.getEntityAttribute("name"));
      
    }
    if (dataSourceRowCache == null) {
      DIHCache cache = queryVsCache.get(query);
      
      if (cache == null) {        
        cache = instantiateCache(context);        
        queryVsCache.put(query, cache);        
        populateCache(query, rowIterator);        
      }
      dataSourceRowCache = cache.iterator(key);
    }    
    return getFromRowCacheTransformed();
  }
  
  /**
   * If where clause is not present the cache is a Map of query vs List of Rows.
   * 
   * @param query
   *          string for which cached row is to be returned
   * 
   * @return the cached row corresponding to the given query
   */
  protected Map<String,Object> getSimpleCacheData(Context context,
      String query, Iterator<Map<String,Object>> rowIterator) {
    if (dataSourceRowCache == null) {      
      DIHCache cache = queryVsCache.get(query);      
      if (cache == null) {        
        cache = instantiateCache(context);        
        queryVsCache.put(query, cache);        
        populateCache(query, rowIterator);        
        queryVsCacheIterator.put(query, cache.iterator());        
      }      
      Iterator<Map<String,Object>> cacheIter = queryVsCacheIterator.get(query);      
      dataSourceRowCache = cacheIter;
    }
    
    return getFromRowCacheTransformed();
  }
  
  protected Map<String,Object> getFromRowCacheTransformed() {
    if (dataSourceRowCache == null || !dataSourceRowCache.hasNext()) {
      dataSourceRowCache = null;
      return null;
    }
    Map<String,Object> r = dataSourceRowCache.next();
    return r;
  }
  
  /**
   * <p>
   * Specify the class for the cache implementation
   * </p>
   */
  public static final String CACHE_IMPL = "cacheImpl";

  /**
   * <p>
   * If the cache supports persistent data, set to "true" to delete any prior
   * persisted data before running the entity.
   * </p>
   */
  
  public static final String CACHE_DELETE_PRIOR_DATA = "cacheDeletePriorData";
  /**
   * <p>
   * Specify the Foreign Key from the parent entity to join on. Use if the cache
   * is on a child entity.
   * </p>
   */
  public static final String CACHE_FOREIGN_KEY = "cacheLookup";

  /**
   * <p>
   * Specify the Primary Key field from this Entity to map the input records
   * with
   * </p>
   */
  public static final String CACHE_PRIMARY_KEY = "cacheKey";
  /**
   * <p>
   * If true, a pre-existing cache is re-opened for read-only access.
   * </p>
   */
  public static final String CACHE_READ_ONLY = "cacheReadOnly";



  
}

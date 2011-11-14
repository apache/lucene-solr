package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DIHCacheSupport {
  private static final Logger log = LoggerFactory
      .getLogger(DIHCacheSupport.class);
  private String cacheVariableName;
  private String cacheImplName;
  private Map<String,DIHCache> queryVsCache = new HashMap<String,DIHCache>();
  private Map<String,Iterator<Map<String,Object>>> queryVsCacheIterator;
  private Iterator<Map<String,Object>> dataSourceRowCache;
  private boolean cacheDoKeyLookup;
  
  public DIHCacheSupport(Context context, String cacheImplName) {
    this.cacheImplName = cacheImplName;
    
    String where = context.getEntityAttribute("where");
    String cacheKey = context
        .getEntityAttribute(DIHCacheSupport.CACHE_PRIMARY_KEY);
    String lookupKey = context
        .getEntityAttribute(DIHCacheSupport.CACHE_FOREIGN_KEY);
    if (cacheKey != null && lookupKey == null) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
          "'cacheKey' is specified for the entity "
              + context.getEntityAttribute("name")
              + " but 'cacheLookup' is missing");
      
    }
    if (where == null && cacheKey == null) {
      cacheDoKeyLookup = false;
    } else {
      if (where != null) {
        String[] splits = where.split("=");
        cacheVariableName = splits[1].trim();
      } else {
        cacheVariableName = lookupKey;
      }
      cacheDoKeyLookup = true;
    }
    context.setSessionAttribute(DIHCacheSupport.CACHE_PRIMARY_KEY, cacheKey,
        Context.SCOPE_ENTITY);
    context.setSessionAttribute(DIHCacheSupport.CACHE_FOREIGN_KEY, lookupKey,
        Context.SCOPE_ENTITY);
    context.setSessionAttribute(DIHCacheSupport.CACHE_DELETE_PRIOR_DATA,
        "true", Context.SCOPE_ENTITY);
    context.setSessionAttribute(DIHCacheSupport.CACHE_READ_ONLY, "false",
        Context.SCOPE_ENTITY);
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
    queryVsCacheIterator = new HashMap<String,Iterator<Map<String,Object>>>();
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
    cacheVariableName = null;
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
    Object key = context.resolve(cacheVariableName);
    if (key == null) {
      throw new DataImportHandlerException(DataImportHandlerException.WARN,
          "The cache lookup value : " + cacheVariableName
              + " is resolved to be null in the entity :"
              + context.getEntityAttribute("name"));
      
    }
    DIHCache cache = queryVsCache.get(query);
    if (cache == null) {
      cache = instantiateCache(context);
      queryVsCache.put(query, cache);
      populateCache(query, rowIterator);
    }
    if (dataSourceRowCache == null) {
      dataSourceRowCache = cache.iterator(key);
    }
    if (dataSourceRowCache == null) {
      return null;
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
    DIHCache cache = queryVsCache.get(query);
    if (cache == null) {
      cache = instantiateCache(context);
      queryVsCache.put(query, cache);
      populateCache(query, rowIterator);
      queryVsCacheIterator.put(query, cache.iterator());
    }
    if (dataSourceRowCache == null || !dataSourceRowCache.hasNext()) {
      dataSourceRowCache = null;
      Iterator<Map<String,Object>> cacheIter = queryVsCacheIterator.get(query);
      if (cacheIter.hasNext()) {
        List<Map<String,Object>> dsrcl = new ArrayList<Map<String,Object>>(1);
        dsrcl.add(cacheIter.next());
        dataSourceRowCache = dsrcl.iterator();
      }
    }
    if (dataSourceRowCache == null) {
      return null;
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
  public static final String CACHE_PRIMARY_KEY = "cachePk";
  /**
   * <p>
   * If true, a pre-existing cache is re-opened for read-only access.
   * </p>
   */
  public static final String CACHE_READ_ONLY = "cacheReadOnly";



  
}

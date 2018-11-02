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

import static org.apache.solr.handler.dataimport.DataImportHandlerException.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;

/**
 * <p> Base class for all implementations of {@link EntityProcessor} </p> <p> Most implementations of {@link EntityProcessor}
 * extend this base class which provides common functionality. </p>
 * <p>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
public class EntityProcessorBase extends EntityProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected boolean isFirstInit = true;

  protected String entityName;

  protected Context context;

  protected Iterator<Map<String, Object>> rowIterator;

  protected String query;  
  
  protected String onError = ABORT;  
  
  protected DIHCacheSupport cacheSupport = null;
  
  private Zipper zipper;


  @Override
  public void init(Context context) {
    this.context = context;
    if (isFirstInit) {
      firstInit(context);
    }
    if(zipper!=null){
      zipper.onNewParent(context);
    }else{
      if(cacheSupport!=null) {
        cacheSupport.initNewParent(context);
      }   
    }
  }

  /**
   * first time init call. do one-time operations here
   * it's necessary to call it from the overridden method,
   * otherwise it throws NPE on accessing zipper from nextRow()
   */
  protected void firstInit(Context context) {
    entityName = context.getEntityAttribute("name");
    String s = context.getEntityAttribute(ON_ERROR);
    if (s != null) onError = s;
    
    zipper = Zipper.createOrNull(context);
    
    if(zipper==null){
      initCache(context);
    }
    isFirstInit = false;
  }

    protected void initCache(Context context) {
        String cacheImplName = context
            .getResolvedEntityAttribute(DIHCacheSupport.CACHE_IMPL);

        if (cacheImplName != null ) {
          cacheSupport = new DIHCacheSupport(context, cacheImplName);
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
  
  protected Map<String, Object> getNext() {
    if(zipper!=null){
      return zipper.supplyNextChild(rowIterator);
    }else{
      if(cacheSupport==null) {
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
      } else  {
        return cacheSupport.getCacheData(context, query, rowIterator);
      }  
    }
  }


  @Override
  public void destroy() {
    query = null;
    if(cacheSupport!=null){
      cacheSupport.destroyAll();
    }
    cacheSupport = null;
  }

  

  public static final String TRANSFORMER = "transformer";

  public static final String TRANSFORM_ROW = "transformRow";

  public static final String ON_ERROR = "onError";

  public static final String ABORT = "abort";

  public static final String CONTINUE = "continue";

  public static final String SKIP = "skip";
}

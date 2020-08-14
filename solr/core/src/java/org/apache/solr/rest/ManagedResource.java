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
package org.apache.solr.rest;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Supports Solr components that have external data that 
 * needs to be managed using the REST API.
 */
public abstract class ManagedResource {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  /**
   * Marker interface to indicate a ManagedResource implementation class also supports
   * managing child resources at path: /&lt;resource&gt;/{child}
   */
  public static interface ChildResourceSupport {}
 
  public static final String INIT_ARGS_JSON_FIELD = "initArgs";
  public static final String MANAGED_JSON_LIST_FIELD = "managedList";
  public static final String MANAGED_JSON_MAP_FIELD = "managedMap";
  public static final String INITIALIZED_ON_JSON_FIELD = "initializedOn";
  public static final String UPDATED_SINCE_INIT_JSON_FIELD = "updatedSinceInit";
      
  private final String resourceId;
  protected final SolrResourceLoader solrResourceLoader;
  protected final ManagedResourceStorage storage;
  protected NamedList<Object> managedInitArgs;
  protected Date initializedOn;
  protected Date lastUpdateSinceInitialization;

  /**
   * Initializes this managed resource, including setting up JSON-based storage using
   * the provided storageIO implementation, such as ZK.
   */
  protected ManagedResource(String resourceId, SolrResourceLoader loader, StorageIO storageIO)
      throws SolrException {

    this.resourceId = resourceId;
    this.solrResourceLoader = loader;    
    this.storage = createStorage(storageIO, loader);
  }
  
  /**
   * Called once during core initialization to get the managed
   * data loaded from storage and notify observers.
   */
  public void loadManagedDataAndNotify(Collection<ManagedResourceObserver> observers) 
      throws SolrException {

    // load managed data from storage
    reloadFromStorage();
    
    // important!!! only affect the Solr component once during core initialization
    // also, as most analysis components will alter the initArgs it is processes them
    // we need to clone the managed initArgs
    notifyObserversDuringInit(managedInitArgs, observers);
    
    // some basic date tracking around when the data was initialized and updated
    initializedOn = new Date();
    lastUpdateSinceInitialization = null;    
  }

  /**
   * Notifies all registered observers that the ManagedResource is initialized.
   * This event only occurs once when the core is loaded. Thus, you need to
   * reload the core to get updates applied to the analysis components that
   * depend on the ManagedResource data.
   */
  protected void notifyObserversDuringInit(NamedList<?> args, Collection<ManagedResourceObserver> observers)
      throws SolrException {

    if (observers == null || observers.isEmpty())
      return;
    
    for (ManagedResourceObserver observer : observers) {
      // clone the args for each observer as some components
      // remove args as they process them, e.g. AbstractAnalysisFactory
      NamedList<?> clonedArgs = args.clone();
      observer.onManagedResourceInitialized(clonedArgs,this);
    }
    log.info("Notified {} observers of {}", observers.size(), getResourceId());
  }  
  
  /**
   * Potential extension point allowing concrete implementations to supply their own storage
   * implementation. The default implementation uses JSON as the storage format and delegates
   * the loading and saving of JSON bytes to the supplied StorageIO class. 
   */
  protected ManagedResourceStorage createStorage(StorageIO storageIO, SolrResourceLoader loader) 
      throws SolrException {
    return new ManagedResourceStorage.JsonStorage(storageIO, loader); 
  }

  /**
   * Returns the resource loader used by this resource.
   */
  public SolrResourceLoader getResourceLoader() {
    return solrResourceLoader;
  }
  
  /**
   * Gets the resource ID for this managed resource.
   */
  public String getResourceId() {
    return resourceId;
  }
  
  /**
   * Gets the ServerResource class to register this endpoint with the Rest API router;
   * in most cases, the default RestManager.ManagedEndpoint class is sufficient but
   * ManagedResource implementations can override this method if a different ServerResource
   * class is needed. 
   */
  public Class<? extends BaseSolrResource> getServerResourceClass() {
    return RestManager.ManagedEndpoint.class;
  }

  /**
   * Called from {@link #doPut(BaseSolrResource,Representation,Object)}
   * to update this resource's init args using the given updatedArgs
   */
  @SuppressWarnings("unchecked")
  protected boolean updateInitArgs(NamedList<?> updatedArgs) {
    if (updatedArgs == null || updatedArgs.size() == 0) {
      return false;
    }
    boolean madeChanges = false;
    if (!managedInitArgs.equals(updatedArgs)) {
      managedInitArgs = (NamedList<Object>)updatedArgs.clone();
      madeChanges = true;
    }
    return madeChanges;
  }
    
  /**
   * Invoked when this object determines it needs to reload the stored data.
   */
  @SuppressWarnings("unchecked")
  protected synchronized void reloadFromStorage() throws SolrException {
    String resourceId = getResourceId();
    Object data = null;
    try {
      data = storage.load(resourceId);
    } catch (FileNotFoundException fnf) {
      log.warn("No stored data found for {}", resourceId);
    } catch (IOException ioExc) {
      throw new SolrException(ErrorCode.SERVER_ERROR, 
          "Failed to load stored data for "+resourceId+" due to: "+ioExc, ioExc);
    }

    Object managedData = processStoredData(data);

    if (managedInitArgs == null)
      managedInitArgs = new NamedList<>();

    onManagedDataLoadedFromStorage(managedInitArgs, managedData);
  }

  /**
   * Processes the stored data.
   */
  protected Object processStoredData(Object data) throws SolrException {
    Object managedData = null;
    if (data != null) {
      if (!(data instanceof Map)) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Stored data for "+resourceId+" is not a valid JSON object!");
      }

      Map<String,Object> jsonMap = (Map<String,Object>)data;
      Map<String,Object> initArgsMap = (Map<String,Object>)jsonMap.get(INIT_ARGS_JSON_FIELD);
      managedInitArgs = new NamedList<>(initArgsMap);
      log.info("Loaded initArgs {} for {}", managedInitArgs, resourceId);

      if (jsonMap.containsKey(MANAGED_JSON_LIST_FIELD)) {
        Object jsonList = jsonMap.get(MANAGED_JSON_LIST_FIELD);
        if (!(jsonList instanceof List)) {
          String errMsg =
              String.format(Locale.ROOT,
                  "Expected JSON array as value for %s but client sent a %s instead!",
                  MANAGED_JSON_LIST_FIELD, jsonList.getClass().getName());
          throw new SolrException(ErrorCode.SERVER_ERROR, errMsg);
        }

        managedData = jsonList;
      } else if (jsonMap.containsKey(MANAGED_JSON_MAP_FIELD)) {
        Object jsonObj = jsonMap.get(MANAGED_JSON_MAP_FIELD);
        if (!(jsonObj instanceof Map)) {
          String errMsg =
              String.format(Locale.ROOT,
                  "Expected JSON map as value for %s but client sent a %s instead!",
                  MANAGED_JSON_MAP_FIELD, jsonObj.getClass().getName());
          throw new SolrException(ErrorCode.SERVER_ERROR, errMsg);
        }

        managedData = jsonObj;
      }
    }
    return managedData;
  }
  
  /**
   * Method called after data has been loaded from storage to give the concrete
   * implementation a chance to post-process the data.
   */
  protected abstract void onManagedDataLoadedFromStorage(NamedList<?> managedInitArgs, Object managedData)
      throws SolrException;
  
  /**
   * Persists managed data to the configured storage IO as a JSON object. 
   */
  public synchronized void storeManagedData(Object managedData) {
    
    Map<String,Object> toStore = buildMapToStore(managedData);    
    String resourceId = getResourceId();
    try {
      storage.store(resourceId, toStore);
      // keep track that the managed data has been updated
      lastUpdateSinceInitialization = new Date();
    } catch (Throwable storeErr) {
      
      // store failed, so try to reset the state of this object by reloading
      // from storage and then failing the store request, but only do that
      // if we've successfully initialized before
      if (initializedOn != null) {
        try {
          reloadFromStorage();
        } catch (Exception reloadExc) {
          // note: the data we're managing now remains in a dubious state
          // however the text analysis component remains unaffected 
          // (at least until core reload)
          log.error("Failed to load data from storage due to: "+reloadExc);
        }
      }
      
      String errMsg = String.format(Locale.ROOT,
          "Failed to store data for %s due to: %s",
          resourceId, storeErr.toString());
      log.error(errMsg, storeErr);
      throw new ResourceException(Status.SERVER_ERROR_INTERNAL, errMsg, storeErr);
    }
  }

  /**
   * Returns this resource's initialization timestamp.
   */
  public String getInitializedOn() {
    return initializedOn == null ? null : initializedOn.toInstant().toString();
  }

  /**
   * Returns the timestamp of the most recent update,
   * or null if this resource has not been updated since initialization.
   */
  public String getUpdatedSinceInitialization() {
    return lastUpdateSinceInitialization == null ? null : lastUpdateSinceInitialization.toInstant().toString();
  }

  /**
   * Returns true if this resource has been changed since initialization.
   */
  public boolean hasChangesSinceInitialization() {
    return (lastUpdateSinceInitialization != null);
  }
  
  /**
   * Builds the JSON object to be stored, containing initArgs and managed data fields. 
   */
  protected Map<String,Object> buildMapToStore(Object managedData) {
    Map<String,Object> toStore = new LinkedHashMap<>(4, 1.0f);
    toStore.put(INIT_ARGS_JSON_FIELD, convertNamedListToMap(managedInitArgs));
    
    // report important dates when data was init'd / updated
    String initializedOnStr = getInitializedOn();
    if (initializedOnStr != null) {
      toStore.put(INITIALIZED_ON_JSON_FIELD, initializedOnStr);
    }
    
    // if the managed data has been updated since initialization (ie. it's dirty)
    // return that in the response as well ... which gives a good hint that the
    // client needs to re-load the collection / core to apply the updates
    if (hasChangesSinceInitialization()) {
      toStore.put(UPDATED_SINCE_INIT_JSON_FIELD, getUpdatedSinceInitialization());
    }
    
    if (managedData != null) {
      if (managedData instanceof List || managedData instanceof Set) {
        toStore.put(MANAGED_JSON_LIST_FIELD, managedData);            
      } else if (managedData instanceof Map) {
        toStore.put(MANAGED_JSON_MAP_FIELD, managedData);      
      } else {
        throw new IllegalArgumentException(
            "Invalid managed data type "+managedData.getClass().getName()+
            "! Only List, Set, or Map objects are supported by this ManagedResource!");
      }      
    }
    
    return toStore;
  }

  /**
   * Converts a NamedList&lt;?&gt; into an ordered Map for returning as JSON.
   */
  protected Map<String,Object> convertNamedListToMap(NamedList<?> args) {
    Map<String,Object> argsMap = new LinkedHashMap<>();
    if (args != null) {
      for (Map.Entry<String,?> entry : args) {
        argsMap.put(entry.getKey(), entry.getValue());
      }
    }
    return argsMap;
  }
  
  /**
   * Just calls {@link #doPut(BaseSolrResource,Representation,Object)};
   * override to change the behavior of POST handling.
   */
  public void doPost(BaseSolrResource endpoint, Representation entity, Object json) {
    doPut(endpoint, entity, json);
  }
  
  /**
   * Applies changes to initArgs or managed data.
   */
  @SuppressWarnings("unchecked")
  public synchronized void doPut(BaseSolrResource endpoint, Representation entity, Object json) {
    
    log.info("Processing update to {}: {} is a "+json.getClass().getName(), getResourceId(), json);
    
    boolean updatedInitArgs = false;
    Object managedData = null;
    if (json instanceof Map) {
      // hmmmm ... not sure how flexible we want to be here?
      Map<String,Object> jsonMap = (Map<String,Object>)json;      
      if (jsonMap.containsKey(INIT_ARGS_JSON_FIELD) || 
          jsonMap.containsKey(MANAGED_JSON_LIST_FIELD) || 
          jsonMap.containsKey(MANAGED_JSON_MAP_FIELD))
      {
        Map<String,Object> initArgsMap = (Map<String,Object>)jsonMap.get(INIT_ARGS_JSON_FIELD);
        updatedInitArgs = updateInitArgs(new NamedList<>(initArgsMap));
        
        if (jsonMap.containsKey(MANAGED_JSON_LIST_FIELD)) {
          managedData = jsonMap.get(MANAGED_JSON_LIST_FIELD);          
        } else if (jsonMap.containsKey(MANAGED_JSON_MAP_FIELD)) {
          managedData = jsonMap.get(MANAGED_JSON_MAP_FIELD);                    
        }
      } else {
        managedData = jsonMap;        
      }      
    } else if (json instanceof List) {
      managedData = json;
    } else {
      throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, 
          "Unsupported update format "+json.getClass().getName());
    }
        
    Object updated = null;
    if (managedData != null) {
      updated = applyUpdatesToManagedData(managedData);
    }
    
    if (updatedInitArgs || updated != null) {
      storeManagedData(updated);
    }
    
    // PUT just returns success status code with an empty body
  }

  /**
   * Called by the RestManager framework after this resource has been deleted
   * to allow this resource to close and clean-up any resources used by this.
   * 
   * @throws IOException if an error occurs in the underlying storage when
   * trying to delete
   */
  public void onResourceDeleted() throws IOException {
    storage.delete(resourceId);
  }

  /**
   * Called during PUT/POST processing to apply updates to the managed data passed from the client.
   */
  protected abstract Object applyUpdatesToManagedData(Object updates);

  /**
   * Called by {@link RestManager.ManagedEndpoint#delete()}
   * to delete a named part (the given childId) of the
   * resource at the given endpoint
   */
  public abstract void doDeleteChild(BaseSolrResource endpoint, String childId);

  /**
   * Called by {@link RestManager.ManagedEndpoint#get()}
   * to retrieve a named part (the given childId) of the
   * resource at the given endpoint
   */
  public abstract void doGet(BaseSolrResource endpoint, String childId);
}

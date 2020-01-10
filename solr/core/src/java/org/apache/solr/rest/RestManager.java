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
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.restlet.Request;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.routing.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.util.Utils.fromJSONString;

/**
 * Supports runtime mapping of REST API endpoints to ManagedResource 
 * implementations; endpoints can be registered at either the /schema
 * or /config base paths, depending on which base path is more appropriate
 * for the type of managed resource.
 */
public class RestManager {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final String SCHEMA_BASE_PATH = "/schema";
  public static final String MANAGED_ENDPOINT = "/managed";
  
  // used for validating resourceIds provided during registration
  private static final Pattern resourceIdRegex = Pattern.compile("(/config|/schema)(/.*)");

  private static final boolean DECODE = true;

  /**
   * Used internally to keep track of registrations during core initialization
   */
  private static class ManagedResourceRegistration {
    String resourceId;
    Class<? extends ManagedResource> implClass;
    Set<ManagedResourceObserver> observers = new LinkedHashSet<>();

    private ManagedResourceRegistration(String resourceId,
                                        Class<? extends ManagedResource> implClass, 
                                        ManagedResourceObserver observer)
    {
      this.resourceId = resourceId;
      this.implClass = implClass;
      
      if (observer != null) {
        this.observers.add(observer);
      }
    }  

    /** Returns resourceId, class, and number of observers of this registered resource */
    public Map<String,String> getInfo() {
      Map<String,String> info = new HashMap<>();
      info.put("resourceId", resourceId);
      info.put("class", implClass.getName());
      info.put("numObservers", String.valueOf(observers.size()));
      return info;
    }    
  }
  
  /**
   * Per-core registry of ManagedResources found during core initialization.
   * 
   * Registering of managed resources can happen before the RestManager is
   * fully initialized. To avoid timing issues, resources register themselves
   * and then the RestManager initializes all ManagedResources before the core
   * is activated.  
   */
  public static class Registry {
    
    private Map<String,ManagedResourceRegistration> registered = new TreeMap<>();
    
    // maybe null until there is a restManager
    private RestManager initializedRestManager = null;

    // REST API endpoints that need to be protected against dynamic endpoint creation
    private final Set<String> reservedEndpoints = new HashSet<>();
    private final Pattern reservedEndpointsPattern;

    public Registry() {
      reservedEndpoints.add(SCHEMA_BASE_PATH + MANAGED_ENDPOINT);

      for (String reservedEndpoint : SolrSchemaRestApi.getReservedEndpoints()) {
        reservedEndpoints.add(reservedEndpoint);
      }

      reservedEndpointsPattern = getReservedEndpointsPattern();
    }

    /**
     * Returns the set of non-registrable endpoints.
     */
    public Set<String> getReservedEndpoints() {
      return Collections.unmodifiableSet(reservedEndpoints);
    }

    /**
     * Returns a Pattern, to be used with Matcher.matches(), that will recognize
     * prefixes or full matches against reserved endpoints that need to be protected
     * against dynamic endpoint registration.  group(1) will contain the match
     * regardless of whether it's a full match or a prefix.
     */
    private Pattern getReservedEndpointsPattern() {
      // Match any of the reserved endpoints exactly, or followed by a slash and more stuff
      StringBuilder builder = new StringBuilder();
      builder.append("(");
      boolean notFirst = false;
      for (String reservedEndpoint : reservedEndpoints) {
        if (notFirst) {
          builder.append("|");
        } else {
          notFirst = true;
        }
        builder.append(reservedEndpoint);
      }
      builder.append(")(?:|/.*)");
      return Pattern.compile(builder.toString());
    }


    /**
     * Get a view of the currently registered resources. 
     */
    public Collection<ManagedResourceRegistration> getRegistered() {
      return Collections.unmodifiableCollection(registered.values());
    }
    
    /**
     * Register the need to use a ManagedResource; this method is typically called
     * by a Solr component during core initialization to register itself as an 
     * observer of a specific type of ManagedResource. As many Solr components may
     * share the same ManagedResource, this method only serves to associate the
     * observer with an endpoint and implementation class. The actual construction
     * of the ManagedResource and loading of data from storage occurs later once
     * the RestManager is fully initialized.
     * @param resourceId - An endpoint in the Rest API to manage the resource; must
     * start with /config and /schema.
     * @param implClass - Class that implements ManagedResource.
     * @param observer - Solr component that needs to know when the data being managed
     * by the ManagedResource is loaded, such as a TokenFilter.
     */
    public synchronized void registerManagedResource(String resourceId, 
        Class<? extends ManagedResource> implClass, ManagedResourceObserver observer) {
      
      if (resourceId == null)
        throw new IllegalArgumentException(
            "Must provide a non-null resourceId to register a ManagedResource!");

      Matcher resourceIdValidator = resourceIdRegex.matcher(resourceId);
      if (!resourceIdValidator.matches()) {
        String errMsg = String.format(Locale.ROOT,
            "Invalid resourceId '%s'; must start with  %s.",
            resourceId,  SCHEMA_BASE_PATH);
        throw new SolrException(ErrorCode.SERVER_ERROR, errMsg);        
      }
         
      // protect reserved REST API endpoints from being used by another
      Matcher reservedEndpointsMatcher = reservedEndpointsPattern.matcher(resourceId);
      if (reservedEndpointsMatcher.matches()) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            reservedEndpointsMatcher.group(1)
            + " is a reserved endpoint used by the Solr REST API!");
      }

      // IMPORTANT: this code should assume there is no RestManager at this point
      
      // it's ok to re-register the same class for an existing path
      ManagedResourceRegistration reg = registered.get(resourceId);
      if (reg != null) {
        if (!reg.implClass.equals(implClass)) {
          String errMsg = String.format(Locale.ROOT,
              "REST API path %s already registered to instances of %s",
              resourceId, reg.implClass.getName());
          throw new SolrException(ErrorCode.SERVER_ERROR, errMsg);          
        } 
        
        if (observer != null) {
          reg.observers.add(observer);
          log.info("Added observer of type {} to existing ManagedResource {}", 
              observer.getClass().getName(), resourceId);
        }
      } else {
        registered.put(resourceId, 
            new ManagedResourceRegistration(resourceId, implClass, observer));
        log.info("Registered ManagedResource impl {} for path {}", 
            implClass.getName(), resourceId);
      }
      
      // there may be a RestManager, in which case, we want to add this new ManagedResource immediately
      if (initializedRestManager != null && initializedRestManager.getManagedResourceOrNull(resourceId) == null) {
        initializedRestManager.addRegisteredResource(registered.get(resourceId));
      }
    }    
  }  

  /**
   * Locates the RestManager using ThreadLocal SolrRequestInfo.
   */
  public static RestManager getRestManager(SolrRequestInfo solrRequestInfo) {
    if (solrRequestInfo == null)
      throw new ResourceException(Status.SERVER_ERROR_INTERNAL, 
          "No SolrRequestInfo in this Thread!");

    SolrQueryRequest req = solrRequestInfo.getReq();
    RestManager restManager = 
        (req != null) ? req.getCore().getRestManager() : null;
    
    if (restManager == null)
      throw new ResourceException(Status.SERVER_ERROR_INTERNAL, 
          "No RestManager found!");
    
    return restManager;
  }
  
  /**
   * The Restlet router needs a lightweight extension of ServerResource to delegate a request
   * to. ManagedResource implementations are heavy-weight objects that live for the duration of
   * a SolrCore, so this class acts as the proxy between Restlet and a ManagedResource when
   * doing request processing.
   *
   */
  public static class ManagedEndpoint extends BaseSolrResource
      implements GETable, PUTable, POSTable, DELETEable
  {
    /**
     * Determines the ManagedResource resourceId from the Restlet request.
     */
    public static String resolveResourceId(Request restletReq)  {
      String resourceId = restletReq.getResourceRef().
          getRelativeRef(restletReq.getRootRef().getParentRef()).getPath(DECODE);
      
      // all resources are registered with the leading slash
      if (!resourceId.startsWith("/"))
        resourceId = "/"+resourceId;


      return resourceId;
    }
    
    protected ManagedResource managedResource;
    protected String childId;    
    
    /**
     * Initialize objects needed to handle a request to the REST API. Specifically,
     * we lookup the RestManager using the ThreadLocal SolrRequestInfo and then
     * dynamically locate the ManagedResource associated with the request URI.
     */
    @Override
    public void doInit() throws ResourceException {
      super.doInit();      
      
      // get the relative path to the requested resource, which is
      // needed to locate ManagedResource impls at runtime
      String resourceId = resolveResourceId(getRequest());

      // supports a request for a registered resource or its child
      RestManager restManager = 
          RestManager.getRestManager(SolrRequestInfo.getRequestInfo());
      
      managedResource = restManager.getManagedResourceOrNull(resourceId);      
      if (managedResource == null) {
        // see if we have a registered endpoint one-level up ...
        int lastSlashAt = resourceId.lastIndexOf('/');
        if (lastSlashAt != -1) {
          String parentResourceId = resourceId.substring(0,lastSlashAt);          
          log.info("Resource not found for {}, looking for parent: {}",
              resourceId, parentResourceId);          
          managedResource = restManager.getManagedResourceOrNull(parentResourceId);
          if (managedResource != null) {
            // verify this resource supports child resources
            if (!(managedResource instanceof ManagedResource.ChildResourceSupport)) {
              String errMsg = String.format(Locale.ROOT,
                  "%s does not support child resources!", managedResource.getResourceId());
              throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, errMsg);
            }
            
            childId = resourceId.substring(lastSlashAt+1);
            log.info("Found parent resource {} for child: {}", 
                parentResourceId, childId);
          }
        }
      }    
      
      if (managedResource == null) {
        if (Method.PUT.equals(getMethod()) || Method.POST.equals(getMethod())) {
          // delegate create requests to the RestManager
          managedResource = restManager.endpoint;
        } else {        
          throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND, 
              "No REST managed resource registered for path "+resourceId);
        }
      }
      
      log.info("Found ManagedResource ["+managedResource+"] for "+resourceId);      
    }    
    
    @Override
    public Representation put(Representation entity) {
      try {
        managedResource.doPut(this, entity, parseJsonFromRequestBody(entity));
      } catch (Exception e) {
        getSolrResponse().setException(e);        
      }
      handlePostExecution(log);
      return new SolrOutputRepresentation();    
    }
    
    @Override
    public Representation post(Representation entity) {
      try {
        managedResource.doPost(this, entity, parseJsonFromRequestBody(entity));
      } catch (Exception e) {
        getSolrResponse().setException(e);        
      }
      handlePostExecution(log);
      return new SolrOutputRepresentation();    
    }    

    @Override
    public Representation delete() {
      // only delegate delete child resources to the ManagedResource
      // as deleting the actual resource is best handled by the
      // RestManager
      if (childId != null) {        
        try {
          managedResource.doDeleteChild(this, childId);
        } catch (Exception e) {
          getSolrResponse().setException(e);        
        }
      } else {
        try {
          RestManager restManager = 
              RestManager.getRestManager(SolrRequestInfo.getRequestInfo());
          restManager.deleteManagedResource(managedResource);
        } catch (Exception e) {
          getSolrResponse().setException(e);        
        }
      }
      handlePostExecution(log);
      return new SolrOutputRepresentation();    
    }    
        
    @Override
    public Representation get() { 
      try {
        managedResource.doGet(this, childId);
      } catch (Exception e) {
        getSolrResponse().setException(e);        
      }
      handlePostExecution(log);
      return new SolrOutputRepresentation();    
    }     
    
    /**
     * Parses and validates the JSON passed from the to the ManagedResource. 
     */
    protected Object parseJsonFromRequestBody(Representation entity) {
      if (entity.getMediaType() == null) {
        entity.setMediaType(MediaType.APPLICATION_JSON);
      }
      
      if (!entity.getMediaType().equals(MediaType.APPLICATION_JSON, true)) {
        String errMsg = String.format(Locale.ROOT,
            "Invalid content type %s; only %s is supported.",
            entity.getMediaType(), MediaType.APPLICATION_JSON.toString());
        log.error(errMsg);
        throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, errMsg);
      }
      
      String text = null;
      try {
        text = entity.getText();
      } catch (IOException ioExc) {
        String errMsg = "Failed to read entity text due to: "+ioExc;
        log.error(errMsg, ioExc);
        throw new ResourceException(Status.SERVER_ERROR_INTERNAL, errMsg, ioExc);
      }
      
      if (text == null || text.trim().length() == 0) {
        throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, "Empty request body!");      
      }

      Object parsedJson = null;
      try {
        parsedJson = fromJSONString(text);
      } catch (Exception ioExc) {
        String errMsg = String.format(Locale.ROOT,
            "Failed to parse request [%s] into JSON due to: %s",
            text, ioExc.toString());
        log.error(errMsg, ioExc);
        throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, errMsg, ioExc);
      }
      return parsedJson;
    }

    @Override
    protected void addDeprecatedWarning() {
      //this is not deprecated
    }
  } // end ManagedEndpoint class
  
  /**
   * The RestManager itself supports some endpoints for creating and listing managed resources.
   * Effectively, this resource provides the API endpoint for doing CRUD on the registry.
   */
  private static class RestManagerManagedResource extends ManagedResource {

    private static final String REST_MANAGER_STORAGE_ID = "/rest/managed";

    private final RestManager restManager;


    public RestManagerManagedResource(RestManager restManager) throws SolrException {
      super(REST_MANAGER_STORAGE_ID, restManager.loader, restManager.storageIO);
      this.restManager = restManager;
    }

    /**
     * Overrides the parent impl to handle FileNotFoundException better
     */
    @Override
    protected synchronized void reloadFromStorage() throws SolrException {
      String resourceId = getResourceId();
      Object data = null;
      try {
        data = storage.load(resourceId);
      } catch (FileNotFoundException fnf) {
        // this is ok - simply means there are no managed components added yet
      } catch (IOException ioExc) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Failed to load stored data for "+resourceId+" due to: "+ioExc, ioExc);
      }

      Object managedData = processStoredData(data);

      if (managedInitArgs == null)
        managedInitArgs = new NamedList<>();

      if (managedData != null)
        onManagedDataLoadedFromStorage(managedInitArgs, managedData);
    }

    /**
     * Loads and initializes any ManagedResources that have been created but
     * are not associated with any Solr components.
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void onManagedDataLoadedFromStorage(NamedList<?> managedInitArgs, Object managedData)
        throws SolrException {

      if (managedData == null) {
        // this is ok - just means no managed components have been added yet
        return;
      }
      
      List<Object> managedList = (List<Object>)managedData;
      for (Object next : managedList) {
        Map<String,String> info = (Map<String,String>)next;        
        String implClass = info.get("class");
        String resourceId = info.get("resourceId");
        Class<? extends ManagedResource> clazz = solrResourceLoader.findClass(implClass, ManagedResource.class);
        ManagedResourceRegistration existingReg = restManager.registry.registered.get(resourceId);
        if (existingReg == null) {
          restManager.registry.registerManagedResource(resourceId, clazz, null);
        } // else already registered, no need to take any action        
      }      
    }
            
    /**
     * Creates a new ManagedResource in the RestManager.
     */
    @SuppressWarnings("unchecked")
    @Override
    public synchronized void doPut(BaseSolrResource endpoint, Representation entity, Object json) {      
      if (json instanceof Map) {
        String resourceId = ManagedEndpoint.resolveResourceId(endpoint.getRequest());
        Map<String,String> info = (Map<String,String>)json;
        info.put("resourceId", resourceId);
        storeManagedData(applyUpdatesToManagedData(json));
      } else {
        throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, 
            "Expected Map to create a new ManagedResource but received a "+json.getClass().getName());
      }          
      // PUT just returns success status code with an empty body
    }

    /**
     * Registers a new {@link ManagedResource}.
     *
     * Called during PUT/POST processing to apply updates to the managed data passed from the client.
     */
    @SuppressWarnings("unchecked")
    @Override
    protected Object applyUpdatesToManagedData(Object updates) {
      Map<String,String> info = (Map<String,String>)updates;      
      // this is where we'd register a new ManagedResource
      String implClass = info.get("class");
      String resourceId = info.get("resourceId");
      log.info("Creating a new ManagedResource of type {} at path {}", 
          implClass, resourceId);
      Class<? extends ManagedResource> clazz = 
          solrResourceLoader.findClass(implClass, ManagedResource.class);
      
      // add this new resource to the RestManager
      restManager.addManagedResource(resourceId, clazz);

      // we only store ManagedResources that don't have observers as those that do
      // are already implicitly defined
      List<Map<String,String>> managedList = new ArrayList<>();
      for (ManagedResourceRegistration reg : restManager.registry.getRegistered()) {
        if (reg.observers.isEmpty()) {
          managedList.add(reg.getInfo());
        }
      }          
      return managedList;
    }

    /**
     * Deleting of child resources not supported by this implementation.
     */
    @Override
    public void doDeleteChild(BaseSolrResource endpoint, String childId) {
      throw new ResourceException(Status.SERVER_ERROR_NOT_IMPLEMENTED);
    }

    @Override
    public void doGet(BaseSolrResource endpoint, String childId) {
      
      // filter results by /schema or /config
      String path = ManagedEndpoint.resolveResourceId(endpoint.getRequest());
      Matcher resourceIdMatcher = resourceIdRegex.matcher(path);
      if (!resourceIdMatcher.matches()) {
        // extremely unlikely but didn't want to squelch it either
        throw new ResourceException(Status.SERVER_ERROR_NOT_IMPLEMENTED, path);
      }
      
      String filter = resourceIdMatcher.group(1);
            
      List<Map<String,String>> regList = new ArrayList<>();
      for (ManagedResourceRegistration reg : restManager.registry.getRegistered()) {
        if (!reg.resourceId.startsWith(filter))
          continue; // doesn't match filter
        
        if (RestManagerManagedResource.class.isAssignableFrom(reg.implClass))
          continue; // internal, no need to expose to outside
        
        regList.add(reg.getInfo());          
      }
      
      endpoint.getSolrResponse().add("managedResources", regList);      
    }    
  } // end RestManagerManagedResource
  
  protected StorageIO storageIO;
  protected Registry registry;
  protected Map<String,ManagedResource> managed = new TreeMap<>();
  protected RestManagerManagedResource endpoint;
  protected SolrResourceLoader loader;
  
  // refs to these are needed to bind new ManagedResources created using the API
  protected Router schemaRouter;
  protected Router configRouter;
  
  /**
   * Initializes the RestManager with the storageIO being optionally created outside of this implementation
   * such as to use ZooKeeper instead of the local FS. 
   */
  public void init(SolrResourceLoader loader,
                   NamedList<String> initArgs, 
                   StorageIO storageIO) 
      throws SolrException
  {
    log.debug("Initializing RestManager with initArgs: "+initArgs);

    if (storageIO == null)
      throw new IllegalArgumentException(
          "Must provide a valid StorageIO implementation to the RestManager!");
    
    this.storageIO = storageIO;
    this.loader = loader;
    
    registry = loader.getManagedResourceRegistry();
    
    // the RestManager provides metadata about managed resources via the /managed endpoint
    // and allows you to create new ManagedResources dynamically by PUT'ing to this endpoint
    endpoint = new RestManagerManagedResource(this);
    endpoint.loadManagedDataAndNotify(null); // no observers for my endpoint
    // responds to requests to /config/managed and /schema/managed
    managed.put(SCHEMA_BASE_PATH+MANAGED_ENDPOINT, endpoint);
            
    // init registered managed resources
    log.debug("Initializing {} registered ManagedResources", registry.registered.size());
    for (ManagedResourceRegistration reg : registry.registered.values()) {
      // keep track of this for lookups during request processing
      managed.put(reg.resourceId, createManagedResource(reg));
    }
    
    // this is for any new registrations that don't come through the API
    // such as from adding a new fieldType to a managed schema that uses a ManagedResource
    registry.initializedRestManager = this;
  }

  /**
   * If not already registered, registers the given {@link ManagedResource} subclass
   * at the given resourceId, creates an instance, and attaches it to the appropriate
   * Restlet router.  Returns the corresponding instance.
   */
  public synchronized ManagedResource addManagedResource(String resourceId, Class<? extends ManagedResource> clazz) {
    final ManagedResource res;
    final ManagedResourceRegistration existingReg = registry.registered.get(resourceId);
    if (existingReg == null) {
      registry.registerManagedResource(resourceId, clazz, null);
      res = addRegisteredResource(registry.registered.get(resourceId));
    } else {
      res = getManagedResource(resourceId);
    }
    return res;
  }
  
  // used internally to create and attach a ManagedResource to the Restlet router
  // the registry also uses this method directly, which is slightly hacky but necessary
  // in order to support dynamic adding of new fieldTypes using the managed-schema API
  private synchronized ManagedResource addRegisteredResource(ManagedResourceRegistration reg) {
    String resourceId = reg.resourceId;
    ManagedResource res = createManagedResource(reg);
    managed.put(resourceId, res);
    log.info("Registered new managed resource {}", resourceId);
    
    // attach this new resource to the Restlet router
    Matcher resourceIdValidator = resourceIdRegex.matcher(resourceId);
    boolean validated = resourceIdValidator.matches();
    assert validated : "managed resourceId '" + resourceId
                     + "' should already be validated by registerManagedResource()";
    String routerPath = resourceIdValidator.group(1);      
    String path = resourceIdValidator.group(2);
    Router router = SCHEMA_BASE_PATH.equals(routerPath) ? schemaRouter : configRouter;
    if (router != null) {
      attachManagedResource(res, path, router);
    }
    return res;
  }


  /**
   * Creates a ManagedResource using registration information. 
   */
  protected ManagedResource createManagedResource(ManagedResourceRegistration reg) throws SolrException {
    ManagedResource res = null;
    try {
      Constructor<? extends ManagedResource> ctor = 
          reg.implClass.getConstructor(String.class, SolrResourceLoader.class, StorageIO.class);
      res = ctor.newInstance(reg.resourceId, loader, storageIO);
      res.loadManagedDataAndNotify(reg.observers);
    } catch (Exception e) {
      String errMsg = 
          String.format(Locale.ROOT,
              "Failed to create new ManagedResource %s of type %s due to: %s",
              reg.resourceId, reg.implClass.getName(), e);      
      throw new SolrException(ErrorCode.SERVER_ERROR, errMsg, e);
    }
    return res;
  }

  /**
   * Returns the {@link ManagedResource} subclass instance corresponding
   * to the given resourceId from the registry.
   *
   * @throws ResourceException if no managed resource is registered with
   *  the given resourceId.
   */
  public ManagedResource getManagedResource(String resourceId) {
    ManagedResource res = getManagedResourceOrNull(resourceId);
    if (res == null) {
      throw new ResourceException(Status.SERVER_ERROR_INTERNAL, 
          "No ManagedResource registered for path: "+resourceId);
    }
    return res;
  }

  /**
   * Returns the {@link ManagedResource} subclass instance corresponding
   * to the given resourceId from the registry, or null if no resource
   * has been registered with the given resourceId.
   */
  public synchronized ManagedResource getManagedResourceOrNull(String resourceId) {
    return managed.get(resourceId);
  }
  
  /**
   * Deletes a managed resource if it is not being used by any Solr components. 
   */
  public synchronized void deleteManagedResource(ManagedResource res) {
    String resourceId = res.getResourceId();
    ManagedResourceRegistration existingReg = registry.registered.get(resourceId);
    int numObservers = existingReg.observers.size();
    if (numObservers > 0) {
      String errMsg = 
          String.format(Locale.ROOT,
              "Cannot delete managed resource %s as it is being used by %d Solr components",
              resourceId, numObservers);
      throw new SolrException(ErrorCode.FORBIDDEN, errMsg);
    }
    
    registry.registered.remove(resourceId);
    managed.remove(resourceId);
    try {
      res.onResourceDeleted();
    } catch (IOException e) {
      // the resource is already deleted so just log this
      log.error("Error when trying to clean-up after deleting "+resourceId, e);
    }
  }
      
  /**
   * Attach managed resource paths to the given Restlet Router. 
   * @param router - Restlet Router
   */
  public synchronized void attachManagedResources(String routerPath, Router router) {
    if (SCHEMA_BASE_PATH.equals(routerPath)) {
      this.schemaRouter = router;
    } else {
      throw new SolrException(ErrorCode.SERVER_ERROR, 
          routerPath+" not supported by the RestManager");
    }      
    
    int numAttached = 0;
    for (Map.Entry<String, ManagedResource> entry : managed.entrySet()) {
      String resourceId = entry.getKey();
      if (resourceId.startsWith(routerPath)) {
        // the way restlet works is you attach a path w/o the routerPath
        String path = resourceId.substring(routerPath.length());
        attachManagedResource(entry.getValue(), path, router);
        ++numAttached;
      }
    }
    
    log.info("Attached {} ManagedResource endpoints to Restlet router: {}", 
        numAttached, routerPath);
  }
  
  /**
   * Attaches a ManagedResource and optionally a path for child resources
   * to the given Restlet Router.
   */
  protected void attachManagedResource(ManagedResource res, String path, Router router) {
    router.attach(path, res.getServerResourceClass());
    log.info("Attached managed resource at path: {}",path);
    
    // Determine if we should also route requests for child resources
    // ManagedResource.ChildResourceSupport is a marker interface that
    // indicates the ManagedResource also manages child resources at
    // a path one level down from the main resourceId
    if (ManagedResource.ChildResourceSupport.class.isAssignableFrom(res.getClass())) {
      router.attach(path+"/{child}", res.getServerResourceClass());
    }    
  }
}

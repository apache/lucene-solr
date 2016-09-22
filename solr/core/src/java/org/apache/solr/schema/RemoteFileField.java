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
package org.apache.solr.schema;

import static org.apache.solr.update.processor.DistributedUpdateProcessor.DISTRIB_FROM;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.apache.solr.search.function.FileFloatSource;
import org.apache.solr.update.SolrCmdDistributor.Node;
import org.apache.solr.update.SolrCmdDistributor.StdNode;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class RemoteFileField extends ExternalFileField implements ResourceLoaderAware, ManagedResourceObserver {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private CloseableHttpClient httpClient = HttpClientUtil.createClient(new ModifiableSolrParams());
  private IndexSchema schema;
  

  private List<SchemaField> remoteFileFields = Collections.emptyList();
  private RemoteFileFieldManager resourceManager = null;

  private boolean fallBackToLocal = false;
  
  
  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    String fallBackToLocalStr = args.remove("fallBackToLocal");
    fallBackToLocal = fallBackToLocalStr != null ? Boolean.valueOf(fallBackToLocalStr) : false;
  }
  //This is called when schema is loaded. Use this time to cache all the SchemaFields which are wired
  //to the instance of this FieldType
  @Override
  public void inform(IndexSchema schema) {
    super.inform(schema);

    this.schema = schema;

    ImmutableList.Builder<SchemaField> remoteFileFieldsBuilder = ImmutableList.builder();
    
    for(SchemaField sf: schema.getFields().values()) {
      if(sf.getType() == this) {
        remoteFileFieldsBuilder.add(sf);
      }
    }
    
    remoteFileFields = remoteFileFieldsBuilder.build();
  }
  
  
  //Called once the managed resources have been loaded
  //Use this time to cache the reference to the resource manager
  @Override
  public void onManagedResourceInitialized(NamedList<?> args, ManagedResource res) throws SolrException {
    resourceManager = (RemoteFileFieldManager) res;
  }
  

  @Override     
  public void inform(ResourceLoader loader) throws IOException {
    SolrResourceLoader solrResourceLoader = (SolrResourceLoader)loader;
    
    // here we want to register that we need to be managed
    // at a specified path and the ManagedResource impl class
    // that should be used to manage this component
    solrResourceLoader.getManagedResourceRegistry().
      registerManagedResource("/schema/remote-files/"+typeName, RemoteFileFieldManager.class, this);
  }
  
  private synchronized void downloadAll() {
    log.info("Downloading all remote field files");
    // FIXME HACK -- Rest Managed doesn't update distrib automatically
    resourceManager.forceReload();
    for (SchemaField field: remoteFileFields) {
      try {
        downloadFile(schema, field);
      } catch (Exception e) {
        log.error( "Unable to download remote file for '" + field.getName() + "'", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to download remote file for '" + field.getName() + "'", e);
      }
    }
  }
  
  private synchronized void downloadFile(IndexSchema schema, SchemaField sf) throws Exception {
    Optional<RemoteFile> rf = resourceManager.getManagedFileURL(sf.getName());
    
    if(!rf.isPresent()) {
      log.warn("No Url specified for {}", sf.getName());
      if(fallBackToLocal) {
        return;
      }

      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No url specified for " + sf.getName());
    }
    
    Path efp = externalFieldPath(schema, sf);
    log.info("Trying to download remote file from {} to {}", rf.get().url.toString(), efp.toAbsolutePath().toString());
    
    
    //TODO fall back to local version of file on startup? -- retry?
    HttpGet req = new HttpGet(rf.get().url.toString());
    CloseableHttpResponse resp = null;
    FileOutputStream os = null;
    try {
      os = new FileOutputStream(efp.toFile());
      resp = httpClient.execute(req);
      
      if(HttpStatus.SC_OK != resp.getStatusLine().getStatusCode()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            String.format("Unable to fetch remote file from path '%s': %s", rf.get().url, resp.getStatusLine().toString()));
      }
      
      HttpEntity entity = resp.getEntity();
      Header contentEncoding = entity.getContentEncoding();
      boolean isGzip = rf.get().gzipped || (contentEncoding != null && (contentEncoding.getValue().contains("gzip") || contentEncoding.getValue().contains("deflate")));
      
      InputStream is = entity.getContent();
      
      if(isGzip) {
        is = new GZIPInputStream(is);
      }
      
      IOUtils.copy(is, os);
    } finally {
      IOUtils.closeQuietly(os);
      IOUtils.closeQuietly(resp);
    }
  }
  
  private Path externalFieldPath(IndexSchema schema, SchemaField sf) {
    return Paths.get(schema.getResourceLoader().getDataDir(), "external_" + sf.getName());
  }

  public static class DownloadRemoteFilesRequestHandler extends RequestHandlerBase implements SolrCoreAware {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private String path;
    
    
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
        throws Exception {
      
      CoreDescriptor coreDesc = req.getCore().getCoreDescriptor();
      boolean zkEnabled = coreDesc.getCoreContainer().isZooKeeperAware();
      
      //Do local first
      if( !zkEnabled || false == req.getParams().getBool(CommonParams.DISTRIB, true) ) {         
        log.info("Downloading files locally");
        downloadAll(req.getSchema());
        return;
      }
      
      log.info("Trying remote files distrib");
      //TODO null check?
      List<Node> nodes = getCollectionUrls(req, coreDesc.getCloudDescriptor().getCollectionName());

      UpdateShardHandler handler = coreDesc.getCoreContainer().getUpdateShardHandler();
      
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.DISTRIB, false);
      params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
          coreDesc.getCoreContainer().getZkController().getBaseUrl(), 
          req.getCore().getName()));
      
      // TODO make parallel
      for(Node n: nodes) {
        submit(handler, n, params);
      }

      //And then reset the external field cache and commit -- DEFAULT NO
      if(req.getParams().getBool(UpdateRequestHandler.COMMIT, false)) {
        FileFloatSource.resetCache();
        log.debug("readerCache has been reset.");
  
        UpdateRequestProcessor processor =
          req.getCore().getUpdateProcessingChain(null).createProcessor(req, rsp);
        try{
          RequestHandlerUtils.handleCommit(req, processor, req.getParams(), true);
        }
        finally{
          processor.finish();
        }
      }
      
    }

    @Override
    public String getDescription() {
      return "Download remote files request handler";
    }
    
    private void submit(UpdateShardHandler uhandler, Node node, ModifiableSolrParams params) {
      
      try (HttpSolrClient client = new HttpSolrClient(node.getUrl(), uhandler.getHttpClient())) {
        client.request(new GenericSolrRequest(METHOD.POST, path, params));
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Failed synchronous update on shard " + node + " for request to " + path, e);
      }
    }
    
    /**
     * Code stolen from @link{DistributedUpdateProcessor#getCollectionUrls}
     */
    private List<Node> getCollectionUrls(SolrQueryRequest req, String collection) {
      ClusterState clusterState = req.getCore().getCoreDescriptor()
          .getCoreContainer().getZkController().getClusterState();
      Map<String,Slice> slices = clusterState.getSlicesMap(collection);
      if (slices == null) {
        throw new ZooKeeperException(ErrorCode.BAD_REQUEST,
            "Could not find collection in zk: " + clusterState);
      }
      final List<Node> urls = new ArrayList<>(slices.size());
      for (Map.Entry<String,Slice> sliceEntry : slices.entrySet()) {
        Slice replicas = slices.get(sliceEntry.getKey());

        Map<String,Replica> shardMap = replicas.getReplicasMap();
        
        for (Entry<String,Replica> entry : shardMap.entrySet()) {
          ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(entry.getValue());
          if (clusterState.liveNodesContain(nodeProps.getNodeName())) {
            urls.add(new StdNode(nodeProps, collection, replicas.getName()));
          }
        }
      }
      if (urls.isEmpty()) {
        return null;
      }
      return urls;
    }

    @Override
    public void inform(SolrCore core) {
      // Find the registered path of the handler
      path = null;
      for (Map.Entry<String, PluginBag.PluginHolder<SolrRequestHandler>> entry : core.getRequestHandlers().getRegistry().entrySet()) {
        if (core.getRequestHandlers().isLoaded(entry.getKey()) && entry.getValue().get() == this) {
          path = entry.getKey();
          break;
        }
      }
      if (path == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "The DownloadRemoteFilesRequestHandler is not registered with the current core.");
      }
      if (!path.startsWith("/")) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "The DownloadRemoteFilesRequestHandler needs to be registered to a path.");
      }
      
      
      // This is a huge hack !! there is a race condition in being able to get the dataDir from resource loader
      // IT doesn't be come available into SolrCoreAware's are trigger
      // Unfortunately you can't implement SolrCoreAware unless a SubType of a specific class, one of them being a RequestHandler
      // If there is another way to get the data dir with the information provided to the FieldType above, this can be changed
      downloadAll(core.getLatestSchema());
    }
    
    private void downloadAll(IndexSchema schema) {
      for(SchemaField sf: schema.getFields().values()) {
        if(sf.getType() instanceof RemoteFileField) {
          ((RemoteFileField)sf.getType()).downloadAll();
        }
      }
    }
  }
  
  private static class RemoteFile {
    public final URL url;
    public final boolean gzipped;
    
    public RemoteFile(URL url, boolean gzipped) {
      this.url = url;
      this.gzipped = gzipped;
    }

    @Override
    public String toString() {
      return "{ \"url\": \""+url.toString()+", \"gzipped\": \""+gzipped+"\"}";
    }
    
  }
  
  public static class RemoteFileFieldManager extends ManagedResource implements ManagedResource.ChildResourceSupport
  {
    Map<String, RemoteFile> managedFiles = new HashMap<String, RemoteFile>();

    public RemoteFileFieldManager(String resourceId, SolrResourceLoader loader, StorageIO storageIO)
        throws SolrException {
      super(resourceId, loader, storageIO);
    }

    @Override
    protected void onManagedDataLoadedFromStorage(NamedList<?> managedInitArgs, Object managedData)
        throws SolrException {
      updateManagedData(managedData, true);
    }

    @Override
    protected Object applyUpdatesToManagedData(Object updates) {
      return updateManagedData(updates, false) ? getStoredView() : null;
    }
    
    private Map<String, Object> getStoredView() {
      Map<String, Object> view = new HashMap<String, Object>();
      
      for(Map.Entry<String,RemoteFile> e: managedFiles.entrySet()) {
        Map<String, Object> val = new HashMap<String,Object>();
        val.put("url", e.getValue().url.toString());
        val.put("gzipped", e.getValue().gzipped);
        view.put(e.getKey(), val);
      }
      
      return view;
    }
    
    @SuppressWarnings("unchecked")
    private synchronized boolean updateManagedData(Object managedData, boolean clearCurrentData) {
      boolean madeChanges = false;
      Map<String, RemoteFile> nextManagedFiles = clearCurrentData ? new HashMap<String, RemoteFile>() : managedFiles;
      
      if (managedData != null) {
        
        Map<String,Object> storedFileUrls = cast(managedData, Map.class, "the top level argument");
        
        for(Map.Entry<String,Object> e: storedFileUrls.entrySet()) {
          Map<String,Object> params = cast(e.getValue(), Map.class, e.getKey());
          
          String surl = cast(params.get("url"), String.class, e.getKey() + ".url");
          
          boolean gzipped = Boolean.valueOf(params.getOrDefault("gzipped", false).toString());
          
          try {
            URL url = new URL((String)surl);
            
            log.info("creating mapping {} -> {}", e.getKey(), url.toString());
            
            RemoteFile oldRemote = nextManagedFiles.put(e.getKey(), new RemoteFile(url, gzipped));
            madeChanges = madeChanges ? true : oldRemote == null;
          } catch (MalformedURLException err) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
                "RemoteFileFieldManager unabled to parse url for '" +e.getKey()+"' and value '"+(String)surl+"'", err);
          }
          
        }
      }
      
      managedFiles = nextManagedFiles;
      
      return madeChanges;
    }
    
    private <T> T cast(Object o, Class<T> clzz, String errDesc) {
      if(clzz.isInstance(o)) {
        return clzz.cast(o);
      }
      
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, 
          String.format("RemoteFileFieldManager was expecting a '%s' for '%s' but got '%s' (%s)" , 
              clzz.getName(), errDesc, o.getClass().getName(), o.toString()));
    }

    @Override
    public void doDeleteChild(BaseSolrResource endpoint, String childId) {
      managedFiles.remove(childId);
    }

    @Override
    public void doGet(BaseSolrResource endpoint, String childId) {
      Optional<RemoteFile> rf = getManagedFileURL(childId);
      if(rf.isPresent()) {
        endpoint.getSolrResponse().add("files", rf.get().url.toString());
      }
    }
    
    public Optional<RemoteFile> getManagedFileURL(String fieldName) {
      return Optional.ofNullable(managedFiles.get(fieldName));
    }

    public void forceReload() {
      super.reloadFromStorage();
    }
    
  }

}

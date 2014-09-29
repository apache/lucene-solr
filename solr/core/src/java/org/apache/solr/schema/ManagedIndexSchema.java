package org.apache.solr.schema;
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

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.rest.schema.FieldTypeXmlAdapter;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.FileUtils;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Solr-managed schema - non-user-editable, but can be mutable via internal and external REST API requests. */
public final class ManagedIndexSchema extends IndexSchema {

  private boolean isMutable = false;

  @Override public boolean isMutable() { return isMutable; }

  final String managedSchemaResourceName;
  
  int schemaZkVersion;
  
  final Object schemaUpdateLock;
  
  /**
   * Constructs a schema using the specified resource name and stream.
   *
   * @see org.apache.solr.core.SolrResourceLoader#openSchema
   *      By default, this follows the normal config path directory searching rules.
   * @see org.apache.solr.core.SolrResourceLoader#openResource
   */
  ManagedIndexSchema(SolrConfig solrConfig, String name, InputSource is, boolean isMutable, 
                     String managedSchemaResourceName, int schemaZkVersion, Object schemaUpdateLock) 
      throws KeeperException, InterruptedException {
    super(solrConfig, name, is);
    this.isMutable = isMutable;
    this.managedSchemaResourceName = managedSchemaResourceName;
    this.schemaZkVersion = schemaZkVersion;
    this.schemaUpdateLock = schemaUpdateLock;
  }
  
  
  /** Persist the schema to local storage or to ZooKeeper */
  boolean persistManagedSchema(boolean createOnly) {
    if (loader instanceof ZkSolrResourceLoader) {
      return persistManagedSchemaToZooKeeper(createOnly);
    }
    // Persist locally
    File managedSchemaFile = new File(loader.getConfigDir(), managedSchemaResourceName);
    OutputStreamWriter writer = null;
    try {
      File parentDir = managedSchemaFile.getParentFile();
      if ( ! parentDir.isDirectory()) {
        if ( ! parentDir.mkdirs()) {
          final String msg = "Can't create managed schema directory " + parentDir.getAbsolutePath();
          log.error(msg);
          throw new SolrException(ErrorCode.SERVER_ERROR, msg);
        }
      }
      final FileOutputStream out = new FileOutputStream(managedSchemaFile);
      writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
      persist(writer);
      log.info("Upgraded to managed schema at " + managedSchemaFile.getPath());
    } catch (IOException e) {
      final String msg = "Error persisting managed schema " + managedSchemaFile;
      log.error(msg, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg, e);
    } finally {
      IOUtils.closeQuietly(writer);
      try {
        FileUtils.sync(managedSchemaFile);
      } catch (IOException e) {
        final String msg = "Error syncing the managed schema file " + managedSchemaFile;
        log.error(msg, e);
      }
    }
    return true;
  }

  /**
   * Persists the managed schema to ZooKeeper using optimistic concurrency.
   * <p/>
   * If createOnly is true, success is when the schema is created or if it previously existed.
   * <p/>
   * If createOnly is false, success is when the schema is persisted - this will only happen
   * if schemaZkVersion matches the version in ZooKeeper.
   * 
   * @return true on success 
   */
  boolean persistManagedSchemaToZooKeeper(boolean createOnly) {
    final ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader)loader;
    final ZkController zkController = zkLoader.getZkController();
    final SolrZkClient zkClient = zkController.getZkClient();
    final String managedSchemaPath = zkLoader.getCollectionZkPath() + "/" + managedSchemaResourceName;
    boolean success = true;
    boolean schemaChangedInZk = false;
    try {
      // Persist the managed schema
      StringWriter writer = new StringWriter();
      persist(writer);

      final byte[] data = writer.toString().getBytes(StandardCharsets.UTF_8);
      if (createOnly) {
        try {
          zkClient.create(managedSchemaPath, data, CreateMode.PERSISTENT, true);
          schemaZkVersion = 0;
          log.info("Created and persisted managed schema znode at " + managedSchemaPath);
        } catch (KeeperException.NodeExistsException e) {
          // This is okay - do nothing and fall through
          log.info("Managed schema znode at " + managedSchemaPath + " already exists - no need to create it");
        }
      } else {
        try {
          // Assumption: the path exists
          Stat stat = zkClient.setData(managedSchemaPath, data, schemaZkVersion, true);
          schemaZkVersion = stat.getVersion();
          log.info("Persisted managed schema version "+schemaZkVersion+" at " + managedSchemaPath);
        } catch (KeeperException.BadVersionException e) {

          log.error("Bad version when trying to persist schema using "+schemaZkVersion+" due to: "+e);

          success = false;
          schemaChangedInZk = true;
        }
      }
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt(); // Restore the interrupted status
      }
      final String msg = "Error persisting managed schema at " + managedSchemaPath;
      log.error(msg, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg, e);
    }
    if (schemaChangedInZk) {
      String msg = "Failed to persist managed schema at " + managedSchemaPath
        + " - version mismatch";
      log.info(msg);
      throw new SchemaChangedInZkException(ErrorCode.CONFLICT, msg + ", retry.");
    }
    return success; 
  }

  /**
   * Block up to a specified maximum time until we see agreement on the schema
   * version in ZooKeeper across all replicas for a collection.
   */
  public static void waitForSchemaZkVersionAgreement(String collection, String localCoreNodeName,
                                                     int schemaZkVersion, ZkController zkController, int maxWaitSecs)
  {
    long startMs = System.currentTimeMillis();

    // get a list of active replica cores to query for the schema zk version (skipping this core of course)
    List<GetZkSchemaVersionCallable> concurrentTasks = new ArrayList<>();
    for (String coreUrl : getActiveReplicaCoreUrls(zkController, collection, localCoreNodeName))
      concurrentTasks.add(new GetZkSchemaVersionCallable(coreUrl, schemaZkVersion));
    if (concurrentTasks.isEmpty())
      return; // nothing to wait for ...


    log.info("Waiting up to "+maxWaitSecs+" secs for "+concurrentTasks.size()+
        " replicas to apply schema update version "+schemaZkVersion+" for collection "+collection);

    // use an executor service to invoke schema zk version requests in parallel with a max wait time
    int poolSize = Math.min(concurrentTasks.size(), 10);
    ExecutorService parallelExecutor =
        Executors.newFixedThreadPool(poolSize, new DefaultSolrThreadFactory("managedSchemaExecutor"));
    try {
      List<Future<Integer>> results =
          parallelExecutor.invokeAll(concurrentTasks, maxWaitSecs, TimeUnit.SECONDS);

      // determine whether all replicas have the update
      List<String> failedList = null; // lazily init'd
      for (int f=0; f < results.size(); f++) {
        int vers = -1;
        Future<Integer> next = results.get(f);
        if (next.isDone() && !next.isCancelled()) {
          // looks to have finished, but need to check the version value too
          try {
            vers = next.get();
          } catch (ExecutionException e) {
            // shouldn't happen since we checked isCancelled
          }
        }

        if (vers == -1) {
          String coreUrl = concurrentTasks.get(f).coreUrl;
          log.warn("Core "+coreUrl+" version mismatch! Expected "+schemaZkVersion+" but got "+vers);
          if (failedList == null) failedList = new ArrayList<>();
          failedList.add(coreUrl);
        }
      }

      // if any tasks haven't completed within the specified timeout, it's an error
      if (failedList != null)
        throw new SolrException(ErrorCode.SERVER_ERROR, failedList.size()+" out of "+(concurrentTasks.size() + 1)+
            " replicas failed to update their schema to version "+schemaZkVersion+" within "+
            maxWaitSecs+" seconds! Failed cores: "+failedList);

    } catch (InterruptedException ie) {
      log.warn("Core "+localCoreNodeName+" was interrupted waiting for schema version "+schemaZkVersion+
          " to propagate to "+concurrentTasks.size()+" replicas for collection "+collection);

      Thread.currentThread().interrupt();
    } finally {
      if (!parallelExecutor.isShutdown())
        parallelExecutor.shutdownNow();
    }

    long diffMs = (System.currentTimeMillis() - startMs);
    log.info("Took "+Math.round(diffMs/1000d)+" secs for "+concurrentTasks.size()+
        " replicas to apply schema update version "+schemaZkVersion+" for collection "+collection);
  }

  protected static List<String> getActiveReplicaCoreUrls(ZkController zkController, String collection, String localCoreNodeName) {
    List<String> activeReplicaCoreUrls = new ArrayList<>();
    ZkStateReader zkStateReader = zkController.getZkStateReader();
    ClusterState clusterState = zkStateReader.getClusterState();
    Set<String> liveNodes = clusterState.getLiveNodes();
    Collection<Slice> activeSlices = clusterState.getActiveSlices(collection);
    if (activeSlices != null && activeSlices.size() > 0) {
      for (Slice next : activeSlices) {
        Map<String, Replica> replicasMap = next.getReplicasMap();
        if (replicasMap != null) {
          for (Map.Entry<String, Replica> entry : replicasMap.entrySet()) {
            Replica replica = entry.getValue();
            if (!localCoreNodeName.equals(replica.getName()) &&
                ZkStateReader.ACTIVE.equals(replica.getStr(ZkStateReader.STATE_PROP)) &&
                liveNodes.contains(replica.getNodeName())) {
              ZkCoreNodeProps replicaCoreProps = new ZkCoreNodeProps(replica);
              activeReplicaCoreUrls.add(replicaCoreProps.getCoreUrl());
            }
          }
        }
      }
    }
    return activeReplicaCoreUrls;
  }

  private static class GetZkSchemaVersionCallable extends SolrRequest implements Callable<Integer> {

    private String coreUrl;
    private int expectedZkVersion;

    GetZkSchemaVersionCallable(String coreUrl, int expectedZkVersion) {
      super(METHOD.GET, "/schema/zkversion");

      this.coreUrl = coreUrl;
      this.expectedZkVersion = expectedZkVersion;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams wparams = new ModifiableSolrParams();
      wparams.set("refreshIfBelowVersion", expectedZkVersion);
      return wparams;
    }

    @Override
    public Integer call() throws Exception {
      HttpSolrServer solr = new HttpSolrServer(coreUrl);
      int remoteVersion = -1;
      try {
        // eventually, this loop will get killed by the ExecutorService's timeout
        while (remoteVersion == -1 || remoteVersion < expectedZkVersion) {
          try {
            HttpSolrServer.HttpUriRequestResponse mrr = solr.httpUriRequest(this);
            NamedList<Object> zkversionResp = mrr.future.get();
            if (zkversionResp != null)
              remoteVersion = (Integer)zkversionResp.get("zkversion");

            if (remoteVersion < expectedZkVersion) {
              // rather than waiting and re-polling, let's be proactive and tell the replica
              // to refresh its schema from ZooKeeper, if that fails, then the
              //Thread.sleep(1000); // slight delay before requesting version again
              log.error("Replica "+coreUrl+" returned schema version "+
                  remoteVersion+" and has not applied schema version "+expectedZkVersion);
            }

          } catch (Exception e) {
            if (e instanceof InterruptedException) {
              break; // stop looping
            } else {
              log.warn("Failed to get /schema/zkversion from " + coreUrl + " due to: " + e);
            }
          }
        }
      } finally {
        solr.shutdown();
      }

      return remoteVersion;
    }

    @Override
    public Collection<ContentStream> getContentStreams() throws IOException {
      return null;
    }

    @Override
    public SolrResponse process(SolrServer server) throws SolrServerException, IOException {
      return null;
    }
  }


  public class FieldExistsException extends SolrException {
    public FieldExistsException(ErrorCode code, String msg) {
      super(code, msg);
    }
  }

  public class SchemaChangedInZkException extends SolrException {
    public SchemaChangedInZkException(ErrorCode code, String msg) {
      super(code, msg);
    }
  }
  
  @Override
  public ManagedIndexSchema addField(SchemaField newField) {
    return addFields(Arrays.asList(newField));
  }

  @Override
  public ManagedIndexSchema addField(SchemaField newField, Collection<String> copyFieldNames) {
    return addFields(Arrays.asList(newField), Collections.singletonMap(newField.getName(), copyFieldNames));
  }

  @Override
  public ManagedIndexSchema addFields(Collection<SchemaField> newFields) {
    return addFields(newFields, Collections.<String, Collection<String>>emptyMap());
  }

  @Override
  public ManagedIndexSchema addFields(Collection<SchemaField> newFields, Map<String, Collection<String>> copyFieldNames) {
    ManagedIndexSchema newSchema = null;
    if (isMutable) {
      boolean success = false;
      if (copyFieldNames == null){
        copyFieldNames = Collections.emptyMap();
      }
      newSchema = shallowCopy(true);

      for (SchemaField newField : newFields) {
        if (null != newSchema.getFieldOrNull(newField.getName())) {
          String msg = "Field '" + newField.getName() + "' already exists.";
          throw new FieldExistsException(ErrorCode.BAD_REQUEST, msg);
        }
        newSchema.fields.put(newField.getName(), newField);

        if (null != newField.getDefaultValue()) {
          log.debug(newField.getName() + " contains default value: " + newField.getDefaultValue());
          newSchema.fieldsWithDefaultValue.add(newField);
        }
        if (newField.isRequired()) {
          log.debug("{} is required in this schema", newField.getName());
          newSchema.requiredFields.add(newField);
        }
        Collection<String> copyFields = copyFieldNames.get(newField.getName());
        if (copyFields != null) {
          for (String copyField : copyFields) {
            newSchema.registerCopyField(newField.getName(), copyField);
          }
        }
      }

      // Run the callbacks on SchemaAware now that everything else is done
      for (SchemaAware aware : newSchema.schemaAware) {
        aware.inform(newSchema);
      }
      newSchema.refreshAnalyzers();
      success = newSchema.persistManagedSchema(false); // don't just create - update it if it already exists
      if (success) {
        log.debug("Added field(s): {}", newFields);
      } else {
        log.error("Failed to add field(s): {}", newFields);
        newSchema = null;
      }
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return newSchema;
  }

  @Override
  public IndexSchema addDynamicField(SchemaField newDynamicField) {
    return addDynamicFields(Arrays.asList(newDynamicField));
  }

  @Override
  public IndexSchema addDynamicField(SchemaField newDynamicField, Collection<String> copyFieldNames) {
    return addDynamicFields(Arrays.asList(newDynamicField),
        Collections.singletonMap(newDynamicField.getName(), copyFieldNames));
  }

  @Override
  public ManagedIndexSchema addDynamicFields(Collection<SchemaField> newDynamicFields) {
    return addDynamicFields(newDynamicFields, Collections.<String,Collection<String>>emptyMap());
  }

  @Override
  public ManagedIndexSchema addDynamicFields(Collection<SchemaField> newDynamicFields, 
                                             Map<String,Collection<String>> copyFieldNames) {
    ManagedIndexSchema newSchema = null;
    if (isMutable) {
      boolean success = false;
      if (copyFieldNames == null){
        copyFieldNames = Collections.emptyMap();
      }
      newSchema = shallowCopy(true);

      for (SchemaField newDynamicField : newDynamicFields) {
        List<DynamicField> dFields = new ArrayList<>(Arrays.asList(newSchema.dynamicFields));
        if (isDuplicateDynField(dFields, newDynamicField)) {
          String msg = "Dynamic field '" + newDynamicField.getName() + "' already exists.";
          throw new FieldExistsException(ErrorCode.BAD_REQUEST, msg);
        }
        dFields.add(new DynamicField(newDynamicField));
        newSchema.dynamicFields = dynamicFieldListToSortedArray(dFields);
        
        Collection<String> copyFields = copyFieldNames.get(newDynamicField.getName());
        if (copyFields != null) {
          for (String copyField : copyFields) {
            newSchema.registerCopyField(newDynamicField.getName(), copyField);
          }
        }
      }

      // Run the callbacks on SchemaAware now that everything else is done
      for (SchemaAware aware : newSchema.schemaAware) {
        aware.inform(newSchema);
      }
      newSchema.refreshAnalyzers();
      success = newSchema.persistManagedSchema(false); // don't just create - update it if it already exists
      if (success) {
        log.debug("Added dynamic field(s): {}", newDynamicFields);
      } else {
        log.error("Failed to add dynamic field(s): {}", newDynamicFields);
      }
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return newSchema;
  }

  @Override
  public ManagedIndexSchema addCopyFields(Map<String, Collection<String>> copyFields) {
    ManagedIndexSchema newSchema = null;
    if (isMutable) {
      boolean success = false;
      newSchema = shallowCopy(true);
      for (Map.Entry<String, Collection<String>> entry : copyFields.entrySet()) {
        //Key is the name of the field, values are the destinations

        for (String destination : entry.getValue()) {
          newSchema.registerCopyField(entry.getKey(), destination);
        }
      }
      //TODO: move this common stuff out to shared methods
      // Run the callbacks on SchemaAware now that everything else is done
      for (SchemaAware aware : newSchema.schemaAware) {
        aware.inform(newSchema);
      }
      newSchema.refreshAnalyzers();
      success = newSchema.persistManagedSchema(false); // don't just create - update it if it already exists
      if (success) {
        log.debug("Added copy fields for {} sources", copyFields.size());
      } else {
        log.error("Failed to add copy fields for {} sources", copyFields.size());
      }
    }
    return newSchema;
  }
  
  public ManagedIndexSchema addFieldType(FieldType fieldType) {
    return addFieldTypes(Collections.singletonList(fieldType));
  }  

  public ManagedIndexSchema addFieldTypes(List<FieldType> fieldTypeList) {
    if (!isMutable) {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);    
    }

    ManagedIndexSchema newSchema = shallowCopy(true);

    // we shallow copied fieldTypes, but since we're changing them, we need to do a true
    // deep copy before adding the new field types
    HashMap<String,FieldType> clone =
        (HashMap<String,FieldType>)((HashMap<String,FieldType>)newSchema.fieldTypes).clone();
    newSchema.fieldTypes = clone;

    // do a first pass to validate the field types don't exist already
    for (FieldType fieldType : fieldTypeList) {    
      String typeName = fieldType.getTypeName();
      if (newSchema.getFieldTypeByName(typeName) != null) {
        throw new FieldExistsException(ErrorCode.BAD_REQUEST,
            "Field type '" + typeName + "' already exists!");
      }
      
      newSchema.fieldTypes.put(typeName, fieldType);
    }

    // Run the callbacks on SchemaAware now that everything else is done
    for (SchemaAware aware : newSchema.schemaAware)
      aware.inform(newSchema);
    
    // looks good for the add, notify ResoureLoaderAware objects
    for (FieldType fieldType : fieldTypeList) {      
          
      // must inform any sub-components used in the 
      // tokenizer chain if they are ResourceLoaderAware    
      if (fieldType.supportsAnalyzers()) {
        Analyzer indexAnalyzer = fieldType.getIndexAnalyzer();
        if (indexAnalyzer != null && indexAnalyzer instanceof TokenizerChain)
          informResourceLoaderAwareObjectsInChain((TokenizerChain)indexAnalyzer);
        
        Analyzer queryAnalyzer = fieldType.getQueryAnalyzer();
        // ref comparison is correct here (vs. equals) as they may be the same
        // object in which case, we don't need to inform twice ... however, it's
        // actually safe to call inform multiple times on an object anyway
        if (queryAnalyzer != null && 
            queryAnalyzer != indexAnalyzer && 
            queryAnalyzer instanceof TokenizerChain)
          informResourceLoaderAwareObjectsInChain((TokenizerChain)queryAnalyzer);

        // if fieldType is a TextField, it might have a multi-term analyzer
        if (fieldType instanceof TextField) {
          TextField textFieldType = (TextField)fieldType;
          Analyzer multiTermAnalyzer = textFieldType.getMultiTermAnalyzer();
          if (multiTermAnalyzer != null && multiTermAnalyzer != indexAnalyzer &&
              multiTermAnalyzer != queryAnalyzer && multiTermAnalyzer instanceof TokenizerChain)
            informResourceLoaderAwareObjectsInChain((TokenizerChain)multiTermAnalyzer);
        }
      }      
    }

    newSchema.refreshAnalyzers();
    
    boolean success = newSchema.persistManagedSchema(false);
    if (success) {
      if (log.isDebugEnabled()) {
        StringBuilder fieldTypeNames = new StringBuilder();
        for (int i=0; i < fieldTypeList.size(); i++) {
          if (i > 0) fieldTypeNames.append(", ");
          fieldTypeNames.append(fieldTypeList.get(i).typeName);
        }
        log.debug("Added field types: {}", fieldTypeNames.toString());
      }
    } else {
      // this is unlikely to happen as most errors are handled as exceptions in the persist code
      log.error("Failed to add field types: {}", fieldTypeList);
      throw new SolrException(ErrorCode.SERVER_ERROR, 
          "Failed to persist updated schema due to underlying storage issue; check log for more details!");
    }
    
    return newSchema;
  }  
  
  @Override
  public SchemaField newField(String fieldName, String fieldType, Map<String,?> options) {
    SchemaField sf; 
    if (isMutable) {
      try {
        if (-1 != fieldName.indexOf('*')) {
          String msg = "Can't add dynamic field '" + fieldName + "'.";
          throw new SolrException(ErrorCode.BAD_REQUEST, msg);
        }
        SchemaField existingFieldWithTheSameName = getFieldOrNull(fieldName);
        if (null != existingFieldWithTheSameName) {
          String msg = "Field '" + fieldName + "' already exists.";
          throw new SolrException(ErrorCode.BAD_REQUEST, msg);
        }
        FieldType type = getFieldTypeByName(fieldType);
        if (null == type) {
          String msg = "Field '" + fieldName + "': Field type '" + fieldType + "' not found.";
          log.error(msg);
          throw new SolrException(ErrorCode.BAD_REQUEST, msg);
        }
        sf = SchemaField.create(fieldName, type, options);
      } catch (SolrException e) {
        throw e;
      } catch (Exception e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      }
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return sf;
  }
  
  public int getSchemaZkVersion() {
    return schemaZkVersion;
  }

  @Override
  public SchemaField newDynamicField(String fieldNamePattern, String fieldType, Map<String,?> options) {
    SchemaField sf;
    if (isMutable) {
      try {
        FieldType type = getFieldTypeByName(fieldType);
        if (null == type) {
          String msg = "Dynamic field '" + fieldNamePattern + "': Field type '" + fieldType + "' not found.";
          log.error(msg);
          throw new SolrException(ErrorCode.BAD_REQUEST, msg);
        }
        sf = SchemaField.create(fieldNamePattern, type, options);
        if ( ! isValidDynamicField(Arrays.asList(dynamicFields), sf)) {
          String msg =  "Invalid dynamic field '" + fieldNamePattern + "'";
          log.error(msg);
          throw new SolrException(ErrorCode.BAD_REQUEST, msg);
        }
      } catch (SolrException e) {
        throw e;
      } catch (Exception e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      }
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return sf;
  }

  @Override
  public FieldType newFieldType(String typeName, String className, Map<String, ?> options) {
    if (!isMutable) {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }

    if (getFieldTypeByName(typeName) != null) {
      String msg = "Field type '" + typeName + "' already exists.";
      log.error(msg);
      throw new SolrException(ErrorCode.BAD_REQUEST, msg);
    }

    // build the new FieldType using the existing FieldTypePluginLoader framework
    // which expects XML, so we use a JSON to XML adapter to transform the JSON object
    // provided in the request into the XML format supported by the plugin loader
    Map<String, FieldType> newFieldTypes = new HashMap<String, FieldType>();
    List<SchemaAware> schemaAwareList = new ArrayList<SchemaAware>();
    FieldTypePluginLoader typeLoader = new FieldTypePluginLoader(this, newFieldTypes, schemaAwareList);
    typeLoader.loadSingle(loader, FieldTypeXmlAdapter.toNode(options));
    FieldType ft = newFieldTypes.get(typeName);
    if (!schemaAwareList.isEmpty())
      schemaAware.addAll(schemaAwareList);

    return ft;
  }

  /**
   * After creating a new FieldType, it may contain components that implement
   * the ResourceLoaderAware interface, which need to be informed after they
   * are loaded (as they depend on this callback to complete initialization work)
   */
  protected void informResourceLoaderAwareObjectsInChain(TokenizerChain chain) {
    CharFilterFactory[] charFilters = chain.getCharFilterFactories();
    if (charFilters != null) {
      for (CharFilterFactory next : charFilters) {
        if (next instanceof ResourceLoaderAware) {
          try {
            ((ResourceLoaderAware) next).inform(loader);
          } catch (IOException e) {
            throw new SolrException(ErrorCode.SERVER_ERROR, e);
          }
        }
      }
    }

    TokenizerFactory tokenizerFactory = chain.getTokenizerFactory();
    if (tokenizerFactory != null && tokenizerFactory instanceof ResourceLoaderAware) {
      try {
        ((ResourceLoaderAware) tokenizerFactory).inform(loader);
      } catch (IOException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    TokenFilterFactory[] filters = chain.getTokenFilterFactories();
    if (filters != null) {
      for (TokenFilterFactory next : filters) {
        if (next instanceof ResourceLoaderAware) {
          try {
            ((ResourceLoaderAware) next).inform(loader);
          } catch (IOException e) {
            throw new SolrException(ErrorCode.SERVER_ERROR, e);
          }
        }
      }
    }
  }
  

  /** 
   * Called from ZkIndexSchemaReader to merge the fields from the serialized managed schema
   * on ZooKeeper with the local managed schema.
   * 
   * @param inputSource The serialized content of the managed schema from ZooKeeper
   * @param schemaZkVersion The ZK version of the managed schema on ZooKeeper
   * @return The new merged schema
   */
  ManagedIndexSchema reloadFields(InputSource inputSource, int schemaZkVersion) {
    ManagedIndexSchema newSchema;
    try {
      newSchema = shallowCopy(false);
      Config schemaConf = new Config(loader, SCHEMA, inputSource, SLASH+SCHEMA+SLASH);
      Document document = schemaConf.getDocument();
      final XPath xpath = schemaConf.getXPath();

      // create a unified collection of field types from zk and in the local
      newSchema.mergeFieldTypesFromZk(document, xpath);

      newSchema.loadFields(document, xpath);
      // let's completely rebuild the copy fields from the schema in ZK.
      // create new copyField-related objects so we don't affect the
      // old schema
      newSchema.copyFieldsMap = new HashMap<>();
      newSchema.dynamicCopyFields = new DynamicCopy[] {};
      newSchema.copyFieldTargetCounts = new HashMap<>();
      newSchema.loadCopyFields(document, xpath);
      if (null != uniqueKeyField) {
        newSchema.requiredFields.add(uniqueKeyField);
      }
      //Run the callbacks on SchemaAware now that everything else is done
      for (SchemaAware aware : newSchema.schemaAware) {
        aware.inform(newSchema);
      }
      newSchema.refreshAnalyzers();
      newSchema.schemaZkVersion = schemaZkVersion;
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Schema Parsing Failed: " + e.getMessage(), e);
    }
    return newSchema;
  }
  
  private ManagedIndexSchema(final SolrConfig solrConfig, final SolrResourceLoader loader, boolean isMutable,
                             String managedSchemaResourceName, int schemaZkVersion, Object schemaUpdateLock) 
      throws KeeperException, InterruptedException {
    super(solrConfig, loader);
    this.isMutable = isMutable;
    this.managedSchemaResourceName = managedSchemaResourceName;
    this.schemaZkVersion = schemaZkVersion;
    this.schemaUpdateLock = schemaUpdateLock;
  }

  /**
   * Makes a shallow copy of this schema.
   * 
   * Not copied: analyzers 
   * 
   * @param includeFieldDataStructures if true, fields, fieldsWithDefaultValue, and requiredFields
   *                                   are copied; otherwise, they are not.
   * @return A shallow copy of this schema
   */
  private ManagedIndexSchema shallowCopy(boolean includeFieldDataStructures) {
    ManagedIndexSchema newSchema = null;
    try {
      newSchema = new ManagedIndexSchema
          (solrConfig, loader, isMutable, managedSchemaResourceName, schemaZkVersion, getSchemaUpdateLock());
    } catch (KeeperException e) {
      final String msg = "Error instantiating ManagedIndexSchema";
      log.error(msg, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg, e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.warn("", e);
    }

    assert newSchema != null;
    
    newSchema.name = name;
    newSchema.version = version;
    newSchema.defaultSearchFieldName = defaultSearchFieldName;
    newSchema.queryParserDefaultOperator = queryParserDefaultOperator;
    newSchema.isExplicitQueryParserDefaultOperator = isExplicitQueryParserDefaultOperator;
    newSchema.similarity = similarity;
    newSchema.similarityFactory = similarityFactory;
    newSchema.isExplicitSimilarity = isExplicitSimilarity;
    newSchema.uniqueKeyField = uniqueKeyField;
    newSchema.uniqueKeyFieldName = uniqueKeyFieldName;
    newSchema.uniqueKeyFieldType = uniqueKeyFieldType;
    
    // After the schema is persisted, resourceName is the same as managedSchemaResourceName
    newSchema.resourceName = managedSchemaResourceName;

    if (includeFieldDataStructures) {
      // These need new collections, since addFields() can add members to them
      newSchema.fields.putAll(fields);
      newSchema.fieldsWithDefaultValue.addAll(fieldsWithDefaultValue);
      newSchema.requiredFields.addAll(requiredFields);
    }

    // These don't need new collections - addFields() won't add members to them 
    newSchema.fieldTypes = fieldTypes;
    newSchema.dynamicFields = dynamicFields;
    newSchema.dynamicCopyFields = dynamicCopyFields;
    newSchema.copyFieldsMap = copyFieldsMap;
    newSchema.copyFieldTargetCounts = copyFieldTargetCounts;
    newSchema.schemaAware = schemaAware;

    return newSchema;
  }

  @Override
  public Object getSchemaUpdateLock() {
    return schemaUpdateLock;
  }

  /**
   * Loads FieldType objects defined in the schema.xml document.
   *
   * @param document Schema XML document where field types are defined.
   * @param xpath Used for evaluating xpath expressions to find field types defined in the schema.xml.
   * @throws javax.xml.xpath.XPathExpressionException if an error occurs when finding field type elements in the document.
   */
  protected synchronized void mergeFieldTypesFromZk(Document document, XPath xpath)
      throws XPathExpressionException
  {
    Map<String, FieldType> newFieldTypes = new HashMap<String, FieldType>();
    FieldTypePluginLoader typeLoader = new FieldTypePluginLoader(this, newFieldTypes, schemaAware);
    String expression = getFieldTypeXPathExpressions();
    NodeList nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);
    typeLoader.load(loader, nodes);
    for (String newTypeName : newFieldTypes.keySet())
      fieldTypes.put(newTypeName, newFieldTypes.get(newTypeName));
  }
}

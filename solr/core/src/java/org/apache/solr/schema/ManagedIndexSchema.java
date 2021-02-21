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

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.Version;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.rest.schema.FieldTypeXmlAdapter;
import org.apache.solr.util.FileUtils;
import org.apache.solr.util.RTimer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/** Solr-managed schema - non-user-editable, but can be mutable via internal and external REST API requests. */
public final class ManagedIndexSchema extends IndexSchema {

  public static final DynamicCopy[] EMPTY_DYNAMIC_COPY_FIELDS = new DynamicCopy[0];
  private final boolean isMutable;
  private final String collection;

  private final ManagedIndexSchemaFactory managedIndexSchemaFactory;

  @Override public boolean isMutable() { return isMutable; }

  volatile String managedSchemaResourceName;

  volatile int schemaZkVersion;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Constructs a schema using the specified resource name and stream.
   *
   * By default, this follows the normal config path directory searching rules.
   * @see org.apache.solr.core.SolrResourceLoader#openResource
   */
  ManagedIndexSchema(ManagedIndexSchemaFactory managedIndexSchemaFactory, String collection, SolrConfig solrConfig, String name, InputSource is, boolean isMutable, String managedSchemaResourceName, int schemaZkVersion) {
    super(name, is, solrConfig.luceneMatchVersion, solrConfig.getResourceLoader(), solrConfig.getSubstituteProperties());
    this.managedIndexSchemaFactory = managedIndexSchemaFactory;
    this.isMutable = isMutable;
    this.collection = collection;
    this.managedSchemaResourceName = managedSchemaResourceName;
    this.schemaZkVersion = schemaZkVersion;
  }


  public IndexSchemaFactory getSchemaFactory() {
    return managedIndexSchemaFactory;
  }
  
  /**
   * Persist the schema to local storage or to ZooKeeper
   * @param createOnly set to false to allow update of existing schema
   */
  public boolean persistManagedSchema(boolean createOnly) {

    if (loader instanceof ZkSolrResourceLoader) {
      return persistManagedSchemaToZooKeeper(createOnly);
    }
    // Persist locally
    File managedSchemaFile = new File(loader.getConfigDir(), managedSchemaResourceName);
    OutputStreamWriter writer = null;
    try {
      File parentDir = managedSchemaFile.getParentFile();
      if (!parentDir.isDirectory()) {
        if (!parentDir.mkdirs()) {
          final String msg = "Can't create managed schema directory " + parentDir.getAbsolutePath();
          log.error(msg);
          throw new SolrException(ErrorCode.SERVER_ERROR, msg);
        }
      }
      final FileOutputStream out = new FileOutputStream(managedSchemaFile);
      writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
      persist(writer);
      if (log.isInfoEnabled()) {
        log.info("Upgraded to managed schema at {}", managedSchemaFile.getPath());
      }
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
    final ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader) loader;
    final SolrZkClient zkClient = zkLoader.getZkClient();

    final String managedSchemaPath = zkLoader.getConfigSetZkPath() + "/" + managedSchemaResourceName;
    boolean success = true;
    boolean schemaChangedInZk = false;
    int ver = -1;
    try {

      // Persist the managed schema
      StringWriter writer = new StringWriter();
      persist(writer);

      final byte[] data = writer.toString().getBytes(StandardCharsets.UTF_8);
      if (createOnly) {
        try {
          zkClient.create(managedSchemaPath, data, CreateMode.PERSISTENT, true);
          log.info("Created and persisted managed schema znode at {}", managedSchemaPath);
          schemaZkVersion = 0;
        } catch (KeeperException.NodeExistsException e) {
          // This is okay - do nothing and fall through
        }
      } else {
        try {
          // Assumption: the path exists

          ver = getSchemaZkVersion();

          Stat managedSchemaStat = zkClient.setData(managedSchemaPath, data, ver, true);
          log.info("Persisted managed schema version {} at {}", managedSchemaStat.getVersion(), managedSchemaPath);
          schemaZkVersion = managedSchemaStat.getVersion();
        } catch (KeeperException.BadVersionException e) {
          // try again with latest schemaZkVersion value
          Stat stat = zkClient.exists(managedSchemaPath, null, true);
          int found = -1;
          if (stat != null) {
            found = stat.getVersion();
          }
          if (log.isDebugEnabled()) {
            log.debug("Bad version when trying to persist schema using {} found {} schema {}", ver, found, this);
          }

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
      String msg = "Failed to persist managed schema at " + managedSchemaPath + " - version mismatch";
      if (log.isDebugEnabled()) {
        log.debug(msg);
      }
      throw new SchemaChangedInZkException(ErrorCode.CONFLICT, msg + ", retry.");
    }

    return success;
  }

  public ManagedIndexSchemaFactory getManagedIndexSchemaFactory() {
    return managedIndexSchemaFactory;
  }

  /**
   * Block up to a specified maximum time until we see agreement on the schema
   * version in ZooKeeper across all replicas for a collection.
   */
  public static void waitForSchemaZkVersionAgreement(String collection, String coreName, int schemaZkVersion, ZkController zkController, int maxWaitSecs, ConnectionManager.IsClosed isClosed) {
    if (zkController.getCoreContainer().isShutDown()) {
      throw new AlreadyClosedException();
    }
    RTimer timer = new RTimer();

    // get a list of active replica cores to query for the schema zk version (skipping this core of course)
    List<GetZkSchemaVersionCallable> concurrentTasks = new ArrayList<>();
    for (String coreUrl : getActiveReplicaCoreUrls(zkController, collection, coreName))
      // MRM TODO: - make a general http2 client that is not also for updates, for now we use recovery client
      concurrentTasks.add(new GetZkSchemaVersionCallable(coreUrl, schemaZkVersion, zkController.getCoreContainer().getUpdateShardHandler().getRecoveryOnlyClient(), isClosed));
    if (concurrentTasks.isEmpty()) return; // nothing to wait for ...

    if (log.isInfoEnabled()) {
      log.info("Waiting up to {} secs for {} replicas to apply schema update version {} for collection {}", maxWaitSecs, concurrentTasks.size(), schemaZkVersion, collection);
    }

    // use an executor service to invoke schema zk version requests in parallel with a max wait time

    List<Future<Integer>> results = new ArrayList<>(concurrentTasks.size());
    for (GetZkSchemaVersionCallable call : concurrentTasks) {
      results.add(ParWork.getRootSharedExecutor().submit(call));
    }

    // determine whether all replicas have the update
    List<String> failedList = null; // lazily init'd
    for (int f = 0; f < results.size(); f++) {
      int vers = -1;
      Future<Integer> next = results.get(f);
      // looks to have finished, but need to check the version value too

      if (zkController.getCoreContainer().isShutDown()) {
        for (int j = 0; j < results.size(); j++) {
          Future<Integer> fut = results.get(j);
          fut.cancel(false);
        }
        return;
      }

      if (!next.isCancelled()) {
        try {
          vers = next.get(maxWaitSecs, TimeUnit.SECONDS);
        } catch (Exception e) {
          log.warn("", e);
          // shouldn't happen since we checked isCancelled
        }
      }


      if (vers == -1) {
        String coreUrl = concurrentTasks.get(f).coreUrl;
        log.warn("Core {} version mismatch! Expected {} but got {}", coreUrl, schemaZkVersion, vers);
        if (failedList == null) failedList = new ArrayList<>();
        failedList.add(coreUrl);
      }
    }

    // if any tasks haven't completed within the specified timeout, it's an error
    if (failedList != null) throw new SolrException(ErrorCode.SERVER_ERROR,
        failedList.size() + " out of " + (concurrentTasks.size() + 1) + " replicas failed to update their schema to version " + schemaZkVersion + " within " + maxWaitSecs + " seconds! Failed cores: "
            + failedList);

    if (log.isInfoEnabled()) {
      log.info("Took {}ms for {} replicas to apply schema update version {} for collection {}", timer.getTime(), concurrentTasks.size(), schemaZkVersion, collection);
    }
  }

  protected static List<String> getActiveReplicaCoreUrls(ZkController zkController, String collection, String coreName) {
    List<String> activeReplicaCoreUrls = new ArrayList<>();
    ZkStateReader zkStateReader = zkController.getZkStateReader();
    Set<String> liveNodes = zkStateReader.getLiveNodes();
    final DocCollection docCollection = zkStateReader.getClusterState().getCollectionOrNull(collection);
    if (docCollection != null && docCollection.getActiveSlices().size() > 0) {
      Collection<Slice> activeSlices = docCollection.getActiveSlices();
      for (Slice next : activeSlices) {
        Map<String, Replica> replicasMap = next.getReplicasMap();
        if (replicasMap != null) {
          for (Map.Entry<String, Replica> entry : replicasMap.entrySet()) {
            Replica replica = entry.getValue();
            if (!coreName.equals(replica.getName()) &&
                replica.getState() == Replica.State.ACTIVE &&
                liveNodes.contains(replica.getNodeName())) {
              activeReplicaCoreUrls.add(replica.getCoreUrl());
            }
          }
        }
      }
    }
    return activeReplicaCoreUrls;
  }

    public ReentrantLock getSchemaLock() {
      return managedIndexSchemaFactory.getSchemaUpdateLock();
    }

    private static class GetZkSchemaVersionCallable extends SolrRequest implements Callable<Integer> {

    private final ConnectionManager.IsClosed isClosed;
    private final Http2SolrClient solrClient;
    private String coreUrl;
    private int expectedZkVersion;

    GetZkSchemaVersionCallable(String coreUrl, int expectedZkVersion, Http2SolrClient solrClient, ConnectionManager.IsClosed isClosed) {
      super(METHOD.GET, "/schema/zkversion");
      setBasePath(coreUrl);
      this.isClosed = isClosed;
      this.coreUrl = coreUrl;
      this.expectedZkVersion = expectedZkVersion;
      this.solrClient = solrClient;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams wparams = new ModifiableSolrParams();
      wparams.set("refreshIfBelowVersion", expectedZkVersion);
      return wparams;
    }

    @Override
    public Integer call() throws Exception {
      int remoteVersion = -1;

        // eventually, this loop will get killed by the ExecutorService's timeout
        while (remoteVersion == -1 || remoteVersion < expectedZkVersion) {
          try {
            NamedList<Object> zkversionResp = solrClient.request(this);
            if (zkversionResp != null)
              remoteVersion = (Integer)zkversionResp.get("zkversion");

            if (remoteVersion < expectedZkVersion) {
              log.info("Replica {} returned schema version {} and has not applied schema version {}"
                  , coreUrl, remoteVersion, expectedZkVersion);
            }

          } catch (Exception e) {
            Throwable cause = e.getCause();
            if (stoppingException(e) || stoppingException(cause)) {
              break;
            } else if (e instanceof  KeeperException.SessionExpiredException || e instanceof RejectedExecutionException) {
              break; // stop looping
            } else {
              log.warn("Failed to get /schema/zkversion from {} due to: ", coreUrl, e);
            }
          }
        }

      return remoteVersion;
    }

    private boolean stoppingException(Throwable e) {
      if (e == null) {
        return false;
      }
      if (e instanceof KeeperException.SessionExpiredException || e instanceof RejectedExecutionException
          || e instanceof InterruptedException || e instanceof AlreadyClosedException) {
        return true;
      }
      return false;
    }

    @Override
    protected SolrResponse createResponse(SolrClient client) {
      return null;
    }

  }


  public static class FieldExistsException extends SolrException {
    public FieldExistsException(ErrorCode code, String msg) {
      super(code, msg);
    }
  }

  public static class UnknownFieldException extends SolrException {
    public UnknownFieldException(ErrorCode code, String msg) {
      super(code, msg);
    }
  }

  public static class SchemaChangedInZkException extends SolrException {
    public SchemaChangedInZkException(ErrorCode code, String msg) {
      super(code, msg);
    }
  }
  

  @Override
  public ManagedIndexSchema addFields(Collection<SchemaField> newFields,
                                      Map<String, Collection<String>> copyFieldNames,
                                      boolean persist) {
    ManagedIndexSchema newSchema;
    if (isMutable) {
      boolean success = false;
      if (copyFieldNames == null) {
        copyFieldNames = Collections.emptyMap();
      }
      newSchema = shallowCopy(true);

      ManagedIndexSchema finalNewSchema = newSchema;
      Map<String,SchemaField> fields = new HashMap<>(newSchema.fields);
      Map<String,Collection<String>> finalCopyFieldNames = copyFieldNames;

      newFields.forEach(newField -> {
        if (null != finalNewSchema.fields.get(newField.getName())) {
          String msg = "Field '" + newField.getName() + "' already exists.";
          throw new FieldExistsException(ErrorCode.BAD_REQUEST, msg);
        }
        fields.put(newField.getName(), newField);

        if (null != newField.getDefaultValue()) {
          if (log.isDebugEnabled()) {
            log.debug("{} contains default value: {}", newField.getName(), newField.getDefaultValue());
          }
          finalNewSchema.fieldsWithDefaultValue.add(newField);
        }
        if (newField.isRequired()) {
          if (log.isDebugEnabled()) {
            log.debug("{} is required in this schema", newField.getName());
          }
          finalNewSchema.requiredFields.add(newField);
        }
        Collection<String> copyFields = finalCopyFieldNames.get(newField.getName());
        if (copyFields != null) {
          for (String copyField : copyFields) {
            finalNewSchema.registerCopyField(newField.getName(), copyField);
          }
        }

      });
      finalNewSchema.fields = Collections.unmodifiableMap(fields);

      if (persist) {
        success = newSchema.persistManagedSchema(false); // don't just create - update it if it already exists
        if (success) {
          if (log.isDebugEnabled()) log.debug("Added field(s): {}", newFields);
          newSchema.postReadInform();
          newSchema.refreshAnalyzers();
        } else {
          log.error("Failed to add field(s): {}", newFields);
          throw new SchemaChangedInZkException(ErrorCode.CONFLICT, "Failed to add field(s): " + newFields + ", retry.");
        }
      } else {
        newSchema.postReadInform();
        newSchema.refreshAnalyzers();
      }
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return newSchema;
  }

  @Override
  public ManagedIndexSchema deleteFields(Collection<String> names) {
    ManagedIndexSchema newSchema;
    if (isMutable) {
      newSchema = shallowCopy(true);
      Map<String,SchemaField> fields = new HashMap<>(newSchema.fields);
      for (String name : names) {
        SchemaField field = getFieldOrNull(name); 
        if (null != field) {
          String message = "Can't delete field '" + name
              + "' because it's referred to by at least one copy field directive.";
          if (newSchema.copyFieldsMap.containsKey(name) || newSchema.isCopyFieldTarget(field)) {
            throw new SolrException(ErrorCode.BAD_REQUEST, message);
          }
          for (int i = 0 ; i < newSchema.dynamicCopyFields.length ; ++i) {
            DynamicCopy dynamicCopy = newSchema.dynamicCopyFields[i];
            if (name.equals(dynamicCopy.getRegex())) {
              throw new SolrException(ErrorCode.BAD_REQUEST, message);
            }
          }
          fields.remove(name);
          newSchema.fieldsWithDefaultValue.remove(field);
          newSchema.requiredFields.remove(field);
        } else {
          String msg = "The field '" + name + "' is not present in this schema, and so cannot be deleted.";
          throw new SolrException(ErrorCode.BAD_REQUEST, msg);
        }
      }
      newSchema.fields = fields;
      newSchema.postReadInform();
      newSchema.refreshAnalyzers();
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return newSchema;
  }

  @Override
  public ManagedIndexSchema replaceField
      (String fieldName, FieldType replacementFieldType, Map<String,?> replacementArgs) {
    ManagedIndexSchema newSchema;
    if (isMutable) {
      SchemaField oldField = fields.get(fieldName);
      if (null == oldField) {
        String msg = "The field '" + fieldName + "' is not present in this schema, and so cannot be replaced.";
        throw new SolrException(ErrorCode.BAD_REQUEST, msg);
      }
      newSchema = shallowCopy(true);
      // clone data structures before modifying them
      Map<String,SchemaField> fields = new HashMap<>(newSchema.fields);

      // Drop the old field
      fields.remove(fieldName);
      newSchema.fieldsWithDefaultValue.remove(oldField);
      newSchema.requiredFields.remove(oldField);

      // Add the replacement field
      SchemaField replacementField = SchemaField.create(fieldName, replacementFieldType, replacementArgs);
      fields.put(fieldName, replacementField);
      newSchema.fields = Collections.unmodifiableMap(fields);
      if (null != replacementField.getDefaultValue()) {
        if (log.isDebugEnabled()) {
          log.debug("{} contains default value: {}", replacementField.getName(), replacementField.getDefaultValue());
        }
        newSchema.fieldsWithDefaultValue.add(replacementField);
      }
      if (replacementField.isRequired()) {
        if (log.isDebugEnabled()) {
          log.debug("{} is required in this schema", replacementField.getName());
        }
        newSchema.requiredFields.add(replacementField);
      }

      List<CopyField> copyFieldsToRebuild = new ArrayList<>();
      newSchema.removeCopyFieldSource(fieldName, copyFieldsToRebuild);

      newSchema.copyFieldTargetCounts.remove(oldField); // zero out target count for this field

      // Remove copy fields where the target is this field; remember them to rebuild
      for (Map.Entry<String,List<CopyField>> entry : newSchema.copyFieldsMap.entrySet()) {
        List<CopyField> perSourceCopyFields = entry.getValue();
        Iterator<CopyField> checkDestCopyFieldsIter = perSourceCopyFields.iterator();
        while (checkDestCopyFieldsIter.hasNext()) {
          CopyField checkDestCopyField = checkDestCopyFieldsIter.next();
          if (fieldName.equals(checkDestCopyField.getDestination().getName())) {
            checkDestCopyFieldsIter.remove();
            copyFieldsToRebuild.add(checkDestCopyField);
          }
        }
      }
      newSchema.rebuildCopyFields(copyFieldsToRebuild);

      // Find dynamic copy fields where the source or destination is this field; remember them to rebuild
      List<DynamicCopy> dynamicCopyFieldsToRebuild = new ArrayList<>();
      List<DynamicCopy> newDynamicCopyFields = new ArrayList<>();
      for (int i = 0 ; i < newSchema.dynamicCopyFields.length ; ++i) {
        DynamicCopy dynamicCopy = newSchema.dynamicCopyFields[i];
        SchemaField destinationPrototype = dynamicCopy.getDestination().getPrototype();
        if (fieldName.equals(dynamicCopy.getRegex()) || fieldName.equals(destinationPrototype.getName())) {
          dynamicCopyFieldsToRebuild.add(dynamicCopy);
        } else {
          newDynamicCopyFields.add(dynamicCopy);
        }
      }
      // Rebuild affected dynamic copy fields
      if (dynamicCopyFieldsToRebuild.size() > 0) {
        newSchema.dynamicCopyFields = newDynamicCopyFields.toArray(new DynamicCopy[newDynamicCopyFields.size()]);
        for (DynamicCopy dynamicCopy : dynamicCopyFieldsToRebuild) {
          newSchema.registerCopyField(dynamicCopy.getRegex(), dynamicCopy.getDestFieldName(), dynamicCopy.getMaxChars());
        }
      }

      newSchema.postReadInform();
      newSchema.refreshAnalyzers();
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return newSchema;
  }
  
  @Override
  public ManagedIndexSchema addDynamicFields(Collection<SchemaField> newDynamicFields, 
                                             Map<String,Collection<String>> copyFieldNames, boolean persist) {
    ManagedIndexSchema newSchema;
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

      if (persist) {
        success = newSchema.persistManagedSchema(false); // don't just create - update it if it already exists
        if (success) {
          log.debug("Added dynamic field(s): {}", newDynamicFields);
          newSchema.postReadInform();
          newSchema.refreshAnalyzers();
        } else {
          log.error("Failed to add dynamic field(s): {}", newDynamicFields);
        }
      } else {
        newSchema.postReadInform();
        newSchema.refreshAnalyzers();
      }
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return newSchema;
  }

  @Override
  public ManagedIndexSchema deleteDynamicFields(Collection<String> fieldNamePatterns) {
    ManagedIndexSchema newSchema;
    if (isMutable) {
      newSchema = shallowCopy(true);

      newSchema.dynamicCopyFields = new DynamicCopy[dynamicCopyFields.length];
      System.arraycopy(dynamicCopyFields, 0, newSchema.dynamicCopyFields, 0, dynamicCopyFields.length);

      List<DynamicCopy> dynamicCopyFieldsToRebuild = new ArrayList<>();
      List<DynamicCopy> newDynamicCopyFields = new ArrayList<>();

      for (String fieldNamePattern : fieldNamePatterns) {
        DynamicField dynamicField = null;
        int dfPos = 0;
        for ( ; dfPos < newSchema.dynamicFields.length ; ++dfPos) {
          DynamicField df = newSchema.dynamicFields[dfPos];
          if (df.getRegex().equals(fieldNamePattern)) {
            dynamicField = df;
            break;
          }
        }
        if (null == dynamicField) {
          String msg = "The dynamic field '" + fieldNamePattern
              + "' is not present in this schema, and so cannot be deleted.";
          throw new SolrException(ErrorCode.BAD_REQUEST, msg);
        }
        for (int i = 0 ; i < newSchema.dynamicCopyFields.length ; ++i) {
          DynamicCopy dynamicCopy = newSchema.dynamicCopyFields[i];
          DynamicField destDynamicBase = dynamicCopy.getDestDynamicBase();
          DynamicField sourceDynamicBase = dynamicCopy.getSourceDynamicBase();
          if ((null != destDynamicBase && fieldNamePattern.equals(destDynamicBase.getRegex()))
              || (null != sourceDynamicBase && fieldNamePattern.equals(sourceDynamicBase.getRegex()))
              || dynamicField.matches(dynamicCopy.getRegex())
              || dynamicField.matches(dynamicCopy.getDestFieldName())) {
            dynamicCopyFieldsToRebuild.add(dynamicCopy);
            newSchema.decrementCopyFieldTargetCount(dynamicCopy.getDestination().getPrototype());
            // don't add this dynamic copy field to newDynamicCopyFields - effectively removing it
          } else {
            newDynamicCopyFields.add(dynamicCopy);
          }
        }
        if (newSchema.dynamicFields.length > 1) {
          DynamicField[] temp = new DynamicField[newSchema.dynamicFields.length - 1];
          System.arraycopy(newSchema.dynamicFields, 0, temp, 0, dfPos);
          // skip over the dynamic field to be deleted
          System.arraycopy(newSchema.dynamicFields, dfPos + 1, temp, dfPos, newSchema.dynamicFields.length - dfPos - 1);
          newSchema.dynamicFields = temp;
        } else {
          newSchema.dynamicFields = EMPTY_DYNAMIC_FIELDS;
        }
      }
      // After removing all dynamic fields, rebuild affected dynamic copy fields.
      // This may trigger an exception, if one of the deleted dynamic fields was the only matching source or target.
      if (dynamicCopyFieldsToRebuild.size() > 0) {
        newSchema.dynamicCopyFields = newDynamicCopyFields.toArray(new DynamicCopy[newDynamicCopyFields.size()]);
        for (DynamicCopy dynamicCopy : dynamicCopyFieldsToRebuild) {
          newSchema.registerCopyField(dynamicCopy.getRegex(), dynamicCopy.getDestFieldName(), dynamicCopy.getMaxChars());
        }
      }

      newSchema.postReadInform();
      newSchema.refreshAnalyzers();
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return newSchema;
  }

  @Override
  public ManagedIndexSchema replaceDynamicField
    (String fieldNamePattern, FieldType replacementFieldType, Map<String,?> replacementArgs) {
    ManagedIndexSchema newSchema;
    if (isMutable) {
      DynamicField oldDynamicField = null;
      int dfPos = 0;
      for ( ; dfPos < dynamicFields.length ; ++dfPos) {
        DynamicField dynamicField = dynamicFields[dfPos];
        if (dynamicField.getRegex().equals(fieldNamePattern)) {
          oldDynamicField = dynamicField;
          break;
        }
      }
      if (null == oldDynamicField) {
        String msg = "The dynamic field '" + fieldNamePattern 
            + "' is not present in this schema, and so cannot be replaced.";
        throw new SolrException(ErrorCode.BAD_REQUEST, msg);
      }

      newSchema = shallowCopy(true);

      // clone data structures before modifying them
      newSchema.copyFieldTargetCounts
          = (Map<SchemaField,Integer>)((HashMap<SchemaField,Integer>)copyFieldTargetCounts).clone();
      newSchema.dynamicCopyFields = new DynamicCopy[dynamicCopyFields.length];
      System.arraycopy(dynamicCopyFields, 0, newSchema.dynamicCopyFields, 0, dynamicCopyFields.length);

      // Put the replacement dynamic field in place
      SchemaField prototype = SchemaField.create(fieldNamePattern, replacementFieldType, replacementArgs);
      newSchema.dynamicFields[dfPos] = new DynamicField(prototype);

      // Find dynamic copy fields where this dynamic field is the source or target base; remember them to rebuild
      List<DynamicCopy> dynamicCopyFieldsToRebuild = new ArrayList<>();
      List<DynamicCopy> newDynamicCopyFields = new ArrayList<>();
      for (int i = 0 ; i < newSchema.dynamicCopyFields.length ; ++i) {
        DynamicCopy dynamicCopy = newSchema.dynamicCopyFields[i];
        DynamicField destDynamicBase = dynamicCopy.getDestDynamicBase();
        DynamicField sourceDynamicBase = dynamicCopy.getSourceDynamicBase();
        if (fieldNamePattern.equals(dynamicCopy.getRegex())
            || fieldNamePattern.equals(dynamicCopy.getDestFieldName())
            || (null != destDynamicBase && fieldNamePattern.equals(destDynamicBase.getRegex()))
            || (null != sourceDynamicBase && fieldNamePattern.equals(sourceDynamicBase.getRegex()))) {
          dynamicCopyFieldsToRebuild.add(dynamicCopy);
          newSchema.decrementCopyFieldTargetCount(dynamicCopy.getDestination().getPrototype());
          // don't add this dynamic copy field to newDynamicCopyFields - effectively removing it
        } else {
          newDynamicCopyFields.add(dynamicCopy);
        }
      }
      // Rebuild affected dynamic copy fields
      if (dynamicCopyFieldsToRebuild.size() > 0) {
        newSchema.dynamicCopyFields = newDynamicCopyFields.toArray(new DynamicCopy[newDynamicCopyFields.size()]);
        for (DynamicCopy dynamicCopy : dynamicCopyFieldsToRebuild) {
          newSchema.registerCopyField(dynamicCopy.getRegex(), dynamicCopy.getDestFieldName(), dynamicCopy.getMaxChars());
        }
      }

      newSchema.postReadInform();
      newSchema.refreshAnalyzers();
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return newSchema;
  }

  @Override
  public ManagedIndexSchema addCopyFields(Map<String, Collection<String>> copyFields, boolean persist) {
    ManagedIndexSchema newSchema;
    if (isMutable) {
      boolean success = false;
      newSchema = shallowCopy(true);
      for (Map.Entry<String, Collection<String>> entry : copyFields.entrySet()) {
        //Key is the name of the field, values are the destinations

        for (String destination : entry.getValue()) {
          newSchema.registerCopyField(entry.getKey(), destination);
        }
      }

      if (persist) {
        success = newSchema.persistManagedSchema(false); // don't just create - update it if it already exists
        if (success) {
          if (log.isDebugEnabled()) {
            log.debug("Added copy fields for {} sources", copyFields.size());
          }
          newSchema.postReadInform();
          newSchema.refreshAnalyzers();
        } else {
          log.error("Failed to add copy fields for {} sources", copyFields.size());
        }
      } else {
        newSchema.postReadInform();
        newSchema.refreshAnalyzers();
      }
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return newSchema;
  }

  @Override
  public ManagedIndexSchema addCopyFields(String source, Collection<String> destinations, int maxChars) {
    ManagedIndexSchema newSchema;
    if (isMutable) {
      newSchema = shallowCopy(true);
      for (String destination : destinations) {
        newSchema.registerCopyField(source, destination, maxChars);
      }
      newSchema.postReadInform();
      newSchema.refreshAnalyzers();
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return newSchema;
  }
  
  @Override
  public ManagedIndexSchema deleteCopyFields(Map<String,Collection<String>> copyFields) {
    ManagedIndexSchema newSchema;
    if (isMutable) {
      newSchema = shallowCopy(true);
      // clone data structures before modifying them
      newSchema.copyFieldsMap = cloneCopyFieldsMap(copyFieldsMap);
      newSchema.copyFieldTargetCounts
          = (Map<SchemaField,Integer>)((HashMap<SchemaField,Integer>)copyFieldTargetCounts).clone();
      newSchema.dynamicCopyFields = new DynamicCopy[dynamicCopyFields.length];
      System.arraycopy(dynamicCopyFields, 0, newSchema.dynamicCopyFields, 0, dynamicCopyFields.length);

      for (Map.Entry<String,Collection<String>> entry : copyFields.entrySet()) {
        // Key is the source, values are the destinations
        for (String destination : entry.getValue()) {
          newSchema.deleteCopyField(entry.getKey(), destination);
        }
      }
      newSchema.postReadInform();
      newSchema.refreshAnalyzers();
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return newSchema;
  }
  
  private void deleteCopyField(String source, String dest) {
    // Assumption: a copy field directive will exist only if the source & destination (dynamic) fields exist
    SchemaField destSchemaField = fields.get(dest);
    SchemaField sourceSchemaField = fields.get(source);

    final String invalidGlobMessage = "is an invalid glob: either it contains more than one asterisk,"
        + " or the asterisk occurs neither at the start nor at the end.";
    if (source.contains("*") && ! isValidFieldGlob(source)) {
      String msg = "copyField source '" + source + "' " + invalidGlobMessage;
      throw new SolrException(ErrorCode.BAD_REQUEST, msg);
    }
    if (dest.contains("*") && ! isValidFieldGlob(dest)) {
      String msg = "copyField dest '" + dest + "' " + invalidGlobMessage;
      throw new SolrException(ErrorCode.BAD_REQUEST, msg);
    }

    boolean found = false;

    if (null == destSchemaField || null == sourceSchemaField) { // Must be dynamic copy field
      for (int i = 0; i < dynamicCopyFields.length; ++i) {
        DynamicCopy dynamicCopy = dynamicCopyFields[i];
        if (source.equals(dynamicCopy.getRegex()) && dest.equals(dynamicCopy.getDestFieldName())) {
          found = true;
          SchemaField destinationPrototype = dynamicCopy.getDestination().getPrototype();
          if (copyFieldTargetCounts.containsKey(destinationPrototype)) {
            decrementCopyFieldTargetCount(destinationPrototype);
          }
          if (dynamicCopyFields.length > 1) {
            DynamicCopy[] temp = new DynamicCopy[dynamicCopyFields.length - 1];
            System.arraycopy(dynamicCopyFields, 0, temp, 0, i);
            // skip over the dynamic copy field to be deleted
            System.arraycopy(dynamicCopyFields, i + 1, temp, i, dynamicCopyFields.length - i - 1);
            dynamicCopyFields = temp;
          } else {
            dynamicCopyFields = EMPTY_DYNAMIC_COPY_FIELDS;
          }
          break;
        }
      }
    }

    if (!found) {
      // non-dynamic copy field directive.
      // Here, source field could either exists in schema or match a dynamic rule
      List<CopyField> copyFieldList = copyFieldsMap.get(source);
      if (copyFieldList != null) {
        for (Iterator<CopyField> iter = copyFieldList.iterator() ; iter.hasNext() ; ) {
          CopyField copyField = iter.next();
          if (dest.equals(copyField.getDestination().getName())) {
            found = true;
            decrementCopyFieldTargetCount(copyField.getDestination());
            iter.remove();
            if (copyFieldList.isEmpty()) {
              copyFieldsMap.remove(source);
            }
            break;
          }
        }
      }
    }
    if ( ! found) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Copy field directive not found: '" + source + "' -> '" + dest + "'");
    }
  }

  /**
   * Removes all copy fields with the given source field name, decrements the count for the copy field target,
   * and adds the removed copy fields to removedCopyFields.
   */
  private void removeCopyFieldSource(String sourceFieldName, List<CopyField> removedCopyFields) {
    List<CopyField> sourceCopyFields = copyFieldsMap.remove(sourceFieldName);
    if (null != sourceCopyFields) {
      for (CopyField sourceCopyField : sourceCopyFields) {
        decrementCopyFieldTargetCount(sourceCopyField.getDestination());
        removedCopyFields.add(sourceCopyField);
      }
    }
  }

  /**
   * Registers new copy fields with the source, destination and maxChars taken from each of the oldCopyFields.
   * 
   * Assumption: the fields in oldCopyFields still exist in the schema. 
   */
  private void rebuildCopyFields(List<CopyField> oldCopyFields) {
    if (oldCopyFields.size() > 0) {
      for (CopyField copyField : oldCopyFields) {
        SchemaField source = fields.get(copyField.getSource().getName());
        SchemaField destination = fields.get(copyField.getDestination().getName());
        registerExplicitSrcAndDestFields
            (copyField.getSource().getName(), copyField.getMaxChars(), destination, source);
      }
    }
  }

  /**
   * Decrements the count for the given destination field in copyFieldTargetCounts.
   */
  private void decrementCopyFieldTargetCount(SchemaField dest) {
    Integer count = copyFieldTargetCounts.get(dest);
    assert count != null;
    if (count <= 1) {
      copyFieldTargetCounts.remove(dest);
    } else {
      copyFieldTargetCounts.put(dest, count - 1);
    }
  }

  public ManagedIndexSchema addFieldTypes(List<FieldType> fieldTypeList, boolean persist) {
    if (!isMutable) {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }

    ManagedIndexSchema newSchema = shallowCopy(true);
    Map<String,FieldType> fieldTypes = new HashMap<>(newSchema.fieldTypes);

    // do a first pass to validate the field types don't exist already
    for (FieldType fieldType : fieldTypeList) {
      String typeName = fieldType.getTypeName();
      if (newSchema.getFieldTypeByName(typeName, fieldTypes) != null) {
        throw new FieldExistsException(ErrorCode.BAD_REQUEST,
            "Field type '" + typeName + "' already exists!");
      }

      fieldTypes.put(typeName, fieldType);
    }
    newSchema.fieldTypes = Collections.unmodifiableMap(fieldTypes);

    if (persist) {
      boolean success = newSchema.persistManagedSchema(false);
      if (success) {
        if (log.isDebugEnabled()) {
          StringBuilder fieldTypeNames = new StringBuilder();
          for (int i=0; i < fieldTypeList.size(); i++) {
            if (i > 0) fieldTypeNames.append(", ");
            fieldTypeNames.append(fieldTypeList.get(i).typeName);
          }
          log.debug("Added field types: {}", fieldTypeNames);
        }
        newSchema.postReadInform();
        newSchema.refreshAnalyzers();
      } else {
        // this is unlikely to happen as most errors are handled as exceptions in the persist code
        log.error("Failed to add field types: {}", fieldTypeList);
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Failed to persist updated schema due to underlying storage issue; check log for more details!");
      }
    } else {
      newSchema.postReadInform();
      newSchema.refreshAnalyzers();
    }

    return newSchema;
  }

  @Override
  public ManagedIndexSchema deleteFieldTypes(Collection<String> names) {
    ManagedIndexSchema newSchema;
    if (isMutable) {

      for (String name : names) {
        if ( ! fieldTypes.containsKey(name)) {
          String msg = "The field type '" + name + "' is not present in this schema, and so cannot be deleted.";
          throw new SolrException(ErrorCode.BAD_REQUEST, msg);
        }
        for (SchemaField field : fields.values()) {
          if (field.getType().getTypeName().equals(name)) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Can't delete '" + name
                + "' because it's the field type of field '" + field.getName() + "'.");
          }
        }
        for (DynamicField dynamicField : dynamicFields) {
          if (dynamicField.getPrototype().getType().getTypeName().equals(name)) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Can't delete '" + name
                + "' because it's the field type of dynamic field '" + dynamicField.getRegex() + "'.");
          }
        }
      }
      newSchema = shallowCopy(true);
      Map<String,FieldType> fieldTypes = new HashMap<>(newSchema.fieldTypes);
      for (String name : names) {
        fieldTypes.remove(name);
      }
      newSchema.fieldTypes = Collections.unmodifiableMap(fieldTypes);
      newSchema.postReadInform();
      newSchema.refreshAnalyzers();
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return newSchema;
  }
  
  private Map<String,List<CopyField>> cloneCopyFieldsMap(Map<String,List<CopyField>> original) {
    Map<String,List<CopyField>> clone = new HashMap<>(original.size());
    Iterator<Map.Entry<String,List<CopyField>>> iterator = original.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String,List<CopyField>> entry = iterator.next();
      clone.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }
    return clone;
  }

  @Override
  public ManagedIndexSchema replaceFieldType(String typeName, String replacementClassName, Map<String,Object> replacementArgs) {
    ManagedIndexSchema newSchema;
    if (isMutable) {
      if ( ! fieldTypes.containsKey(typeName)) {
        String msg = "The field type '" + typeName + "' is not present in this schema, and so cannot be replaced.";
        throw new SolrException(ErrorCode.BAD_REQUEST, msg);
      }
      newSchema = shallowCopy(true);

      Map<String,SchemaField> fields = new HashMap<>(newSchema.fields);
      Map<String,FieldType> fieldTypes = new HashMap<>(newSchema.fieldTypes);
      // clone data structures before modifying them
      newSchema.copyFieldsMap = cloneCopyFieldsMap(copyFieldsMap);
      newSchema.copyFieldTargetCounts
          = (Map<SchemaField,Integer>)((HashMap<SchemaField,Integer>)copyFieldTargetCounts).clone();
      newSchema.dynamicCopyFields = new DynamicCopy[dynamicCopyFields.length];
      System.arraycopy(dynamicCopyFields, 0, newSchema.dynamicCopyFields, 0, dynamicCopyFields.length);
      newSchema.dynamicFields = new DynamicField[dynamicFields.length];
      System.arraycopy(dynamicFields, 0, newSchema.dynamicFields, 0, dynamicFields.length);

      fieldTypes.remove(typeName);
      FieldType replacementFieldType = newSchema.newFieldType(typeName, replacementClassName, replacementArgs, fieldTypes);
      fieldTypes.put(typeName, replacementFieldType);

      // Rebuild fields of the type being replaced
      List<CopyField> copyFieldsToRebuild = new ArrayList<>();
      List<SchemaField> replacementFields = new ArrayList<>();
      Iterator<Map.Entry<String,SchemaField>> fieldsIter = newSchema.fields.entrySet().iterator();
      while (fieldsIter.hasNext()) {
        Map.Entry<String,SchemaField> entry = fieldsIter.next();
        SchemaField oldField = entry.getValue();
        if (oldField.getType().getTypeName().equals(typeName)) {
          String fieldName = oldField.getName();

          // Drop the old field
          fieldsIter.remove();
          newSchema.fieldsWithDefaultValue.remove(oldField);
          newSchema.requiredFields.remove(oldField);

          // Add the replacement field
          SchemaField replacementField = SchemaField.create(fieldName, replacementFieldType, oldField.getArgs());
          replacementFields.add(replacementField); // Save the new field to be added after iteration is finished
          if (null != replacementField.getDefaultValue()) {
            if (log.isDebugEnabled()) {
              log.debug("{} contains default value: {}", replacementField.getName(), replacementField.getDefaultValue());
            }
            newSchema.fieldsWithDefaultValue.add(replacementField);
          }
          if (replacementField.isRequired()) {
            if (log.isDebugEnabled()) {
              log.debug("{} is required in this schema", replacementField.getName());
            }
            newSchema.requiredFields.add(replacementField);
          }
          newSchema.removeCopyFieldSource(fieldName, copyFieldsToRebuild);
        }
      }
      for (SchemaField replacementField : replacementFields) {
        fields.put(replacementField.getName(), replacementField);
      }
      newSchema.fields = Collections.unmodifiableMap(fields);
      newSchema.fieldTypes = Collections.unmodifiableMap(fieldTypes);
      // Remove copy fields where the target is of the type being replaced; remember them to rebuild
      Iterator<Map.Entry<String,List<CopyField>>> copyFieldsMapIter = newSchema.copyFieldsMap.entrySet().iterator();
      while (copyFieldsMapIter.hasNext()) {
        Map.Entry<String,List<CopyField>> entry = copyFieldsMapIter.next();
        List<CopyField> perSourceCopyFields = entry.getValue();
        Iterator<CopyField> checkDestCopyFieldsIter = perSourceCopyFields.iterator();
        while (checkDestCopyFieldsIter.hasNext()) {
          CopyField checkDestCopyField = checkDestCopyFieldsIter.next();
          SchemaField destination = checkDestCopyField.getDestination();
          if (typeName.equals(destination.getType().getTypeName())) {
            checkDestCopyFieldsIter.remove();
            copyFieldsToRebuild.add(checkDestCopyField);
            newSchema.copyFieldTargetCounts.remove(destination); // zero out target count
          }
        }
        if (perSourceCopyFields.isEmpty()) {
          copyFieldsMapIter.remove();
        }
      }
      // Rebuild dynamic fields of the type being replaced
      for (int i = 0; i < newSchema.dynamicFields.length; ++i) {
        SchemaField prototype = newSchema.dynamicFields[i].getPrototype();
        if (typeName.equals(prototype.getType().getTypeName())) {
          newSchema.dynamicFields[i] = new DynamicField
              (SchemaField.create(prototype.getName(), replacementFieldType, prototype.getArgs()));
        }
      }
      // Find dynamic copy fields where the destination field's type is being replaced
      // or the source dynamic base's type is being replaced; remember them to rebuild
      List<DynamicCopy> dynamicCopyFieldsToRebuild = new ArrayList<>();
      List<DynamicCopy> newDynamicCopyFields = new ArrayList<>();
      for (int i = 0 ; i < newSchema.dynamicCopyFields.length ; ++i) {
        DynamicCopy dynamicCopy = newSchema.dynamicCopyFields[i];
        DynamicField sourceDynamicBase = dynamicCopy.getSourceDynamicBase();
        SchemaField destinationPrototype = dynamicCopy.getDestination().getPrototype();
        if (typeName.equals(destinationPrototype.getType().getTypeName())
            || (null != sourceDynamicBase && typeName.equals(sourceDynamicBase.getPrototype().getType().getTypeName()))) {
          dynamicCopyFieldsToRebuild.add(dynamicCopy);
          if (newSchema.copyFieldTargetCounts.containsKey(destinationPrototype)) {
            newSchema.decrementCopyFieldTargetCount(destinationPrototype);
          }
          // don't add this dynamic copy field to newDynamicCopyFields - effectively removing it
        } else {
          newDynamicCopyFields.add(dynamicCopy);
        }
      }
      // Rebuild affected dynamic copy fields
      if (dynamicCopyFieldsToRebuild.size() > 0) {
        newSchema.dynamicCopyFields = newDynamicCopyFields.toArray(new DynamicCopy[newDynamicCopyFields.size()]);
        for (DynamicCopy dynamicCopy : dynamicCopyFieldsToRebuild) {
          newSchema.registerCopyField(dynamicCopy.getRegex(), dynamicCopy.getDestFieldName(), dynamicCopy.getMaxChars());
        }
      }
      newSchema.rebuildCopyFields(copyFieldsToRebuild);

      newSchema.postReadInform();
      newSchema.refreshAnalyzers();
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    return newSchema;
  }
  
  @Override
  public void postReadInform() {
    super.postReadInform();

    try (ParWork work = new ParWork(this)) {
      fieldTypes.forEach((s, fieldType) -> {
        work.collect("", ()->{
          informResourceLoaderAwareObjectsForFieldType(fieldType);
        });
      });
    }
  }

  /**
   * Informs analyzers used by a fieldType.
   */
  protected void informResourceLoaderAwareObjectsForFieldType(FieldType fieldType) {
    // must inform any sub-components used in the
    // tokenizer chain if they are ResourceLoaderAware
    if (!fieldType.supportsAnalyzers())
      return;

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
  
  @Override
  public SchemaField newField(String fieldName, String fieldType, Map<String,?> options) {
    SchemaField sf; 
    if (isMutable) {
      try {
        if (-1 != fieldName.indexOf('*')) {
          String msg = "Can't add dynamic field '" + fieldName + "'.";
          throw new SolrException(ErrorCode.BAD_REQUEST, msg);
        }
        SchemaField existingFieldWithTheSameName = fields.get(fieldName);
        if (null != existingFieldWithTheSameName) {
          String msg = "Field '" + fieldName + "' already exists.";
          throw new SolrException(ErrorCode.BAD_REQUEST, msg);
        }
        FieldType type = getFieldTypeByName(fieldType, fieldTypes);
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
        FieldType type = getFieldTypeByName(fieldType, fieldTypes);
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
  public FieldType newFieldType(String typeName, String className, Map<String, ?> options, Map<String,FieldType> fieldTypes) {
    if (!isMutable) {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }

    if (getFieldTypeByName(typeName, fieldTypes) != null) {
      String msg = "Field type '" + typeName + "' already exists.";
      log.error(msg);
      throw new SolrException(ErrorCode.BAD_REQUEST, msg);
    }

    // build the new FieldType using the existing FieldTypePluginLoader framework
    // which expects XML, so we use a JSON to XML adapter to transform the JSON object
    // provided in the request into the XML format supported by the plugin loader
    Map<String,FieldType> newFieldTypes = new HashMap<>(32);
    List<SchemaAware> schemaAwareList = new ArrayList<>(32);
    FieldTypePluginLoader typeLoader = new FieldTypePluginLoader(this, newFieldTypes, schemaAwareList);
    typeLoader.loadSingle(loader, FieldTypeXmlAdapter.toNode(loader, options));
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
    for (CharFilterFactory next : charFilters) {
      if (next instanceof ResourceLoaderAware) {
        try {
          ((ResourceLoaderAware) next).inform(loader);
        } catch (IOException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
      }
    }

    TokenizerFactory tokenizerFactory = chain.getTokenizerFactory();
    if (tokenizerFactory instanceof ResourceLoaderAware) {
      try {
        ((ResourceLoaderAware) tokenizerFactory).inform(loader);
      } catch (IOException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    TokenFilterFactory[] filters = chain.getTokenFilterFactories();
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
  
  private ManagedIndexSchema(ManagedIndexSchemaFactory managedIndexSchemaFactory, String collection, Version luceneVersion, SolrResourceLoader loader, boolean isMutable,
                             String managedSchemaResourceName, int schemaZkVersion, Properties substitutableProps) {
    super(luceneVersion, loader, substitutableProps);
    this.managedIndexSchemaFactory = managedIndexSchemaFactory;
    this.isMutable = isMutable;
    this.managedSchemaResourceName = managedSchemaResourceName;
    this.schemaZkVersion = schemaZkVersion;
    this.collection = collection;
    if (log.isDebugEnabled()) {
      log.debug("Copy to new ManagedIndexSchemaFactory with version {}", schemaZkVersion);
    }
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
   ManagedIndexSchema shallowCopy(boolean includeFieldDataStructures) {
     ManagedIndexSchema newSchema = new ManagedIndexSchema
         (managedIndexSchemaFactory, collection, luceneVersion, loader, isMutable, managedSchemaResourceName, schemaZkVersion, substitutableProperties);

    newSchema.name = name;
    newSchema.version = version;
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
      newSchema.fieldsWithDefaultValue.addAll(fieldsWithDefaultValue);
      newSchema.requiredFields.addAll(requiredFields);
    }

    // These don't need new collections - addFields() won't add members to them
    newSchema.dynamicFields = dynamicFields;
    newSchema.dynamicCopyFields = dynamicCopyFields;
    newSchema.fields = fields;
    newSchema.fieldTypes = fieldTypes;
    newSchema.copyFieldsMap = copyFieldsMap;
    newSchema.copyFieldTargetCounts = copyFieldTargetCounts;
    newSchema.schemaAware = schemaAware;

    return newSchema;
  }
}

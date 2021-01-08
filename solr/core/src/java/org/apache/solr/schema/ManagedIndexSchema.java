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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.rest.schema.FieldTypeXmlAdapter;
import org.apache.solr.util.DOMConfigNode;
import org.apache.solr.util.FileUtils;
import org.apache.solr.util.RTimer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.core.SolrResourceLoader.informAware;

/** Solr-managed schema - non-user-editable, but can be mutable via internal and external REST API requests. */
public final class ManagedIndexSchema extends IndexSchema {

  private final boolean isMutable;

  @Override public boolean isMutable() { return isMutable; }

  final String managedSchemaResourceName;

  int schemaZkVersion;

  final Object schemaUpdateLock;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Constructs a schema using the specified resource name and stream.
   *
   * By default, this follows the normal config path directory searching rules.
   * @see org.apache.solr.core.SolrResourceLoader#openResource
   */
  ManagedIndexSchema(SolrConfig solrConfig, String name, ConfigSetService.ConfigResource is, boolean isMutable,
                     String managedSchemaResourceName, int schemaZkVersion, Object schemaUpdateLock) {
    super(name, is, solrConfig.luceneMatchVersion, solrConfig.getResourceLoader(), solrConfig.getSubstituteProperties());
    this.isMutable = isMutable;
    this.managedSchemaResourceName = managedSchemaResourceName;
    this.schemaZkVersion = schemaZkVersion;
    this.schemaUpdateLock = schemaUpdateLock;
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
    final ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader)loader;
    final ZkController zkController = zkLoader.getZkController();
    final SolrZkClient zkClient = zkController.getZkClient();
    final String managedSchemaPath = zkLoader.getConfigSetZkPath() + "/" + managedSchemaResourceName;
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
          log.info("Created and persisted managed schema znode at {}", managedSchemaPath);
        } catch (KeeperException.NodeExistsException e) {
          // This is okay - do nothing and fall through
          log.info("Managed schema znode at {} already exists - no need to create it", managedSchemaPath);
        }
      } else {
        try {
          // Assumption: the path exists
          Stat stat = zkClient.setData(managedSchemaPath, data, schemaZkVersion, true);
          schemaZkVersion = stat.getVersion();
          log.info("Persisted managed schema version {}  at {}", schemaZkVersion, managedSchemaPath);
        } catch (KeeperException.BadVersionException e) {

          log.error("Bad version when trying to persist schema using {} due to: ", schemaZkVersion, e);

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
    RTimer timer = new RTimer();

    // get a list of active replica cores to query for the schema zk version (skipping this core of course)
    List<GetZkSchemaVersionCallable> concurrentTasks = new ArrayList<>();
    for (String coreUrl : getActiveReplicaCoreUrls(zkController, collection, localCoreNodeName))
      concurrentTasks.add(new GetZkSchemaVersionCallable(coreUrl, schemaZkVersion));
    if (concurrentTasks.isEmpty())
      return; // nothing to wait for ...


    if (log.isInfoEnabled()) {
      log.info("Waiting up to {} secs for {} replicas to apply schema update version {} for collection {}"
          , maxWaitSecs, concurrentTasks.size(), schemaZkVersion, collection);
    }

    // use an executor service to invoke schema zk version requests in parallel with a max wait time
    int poolSize = Math.min(concurrentTasks.size(), 10);
    ExecutorService parallelExecutor =
        ExecutorUtil.newMDCAwareFixedThreadPool(poolSize, new SolrNamedThreadFactory("managedSchemaExecutor"));
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
          log.warn("Core {} version mismatch! Expected {} but got {}", coreUrl, schemaZkVersion, vers);
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
      log.warn("Core {} was interrupted waiting for schema version {} to propagate to {} replicas for collection {}"
          , localCoreNodeName, schemaZkVersion, concurrentTasks.size(), collection);
      Thread.currentThread().interrupt();
    } finally {
      if (!parallelExecutor.isShutdown())
        parallelExecutor.shutdown();
    }

    if (log.isInfoEnabled()) {
      log.info("Took {}ms for {} replicas to apply schema update version {} for collection {}",
          timer.getTime(), concurrentTasks.size(), schemaZkVersion, collection);
    }
  }

  protected static List<String> getActiveReplicaCoreUrls(ZkController zkController, String collection, String localCoreNodeName) {
    List<String> activeReplicaCoreUrls = new ArrayList<>();
    ZkStateReader zkStateReader = zkController.getZkStateReader();
    ClusterState clusterState = zkStateReader.getClusterState();
    Set<String> liveNodes = clusterState.getLiveNodes();
    final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    if (docCollection != null && docCollection.getActiveSlicesArr().length > 0) {
      final Slice[] activeSlices = docCollection.getActiveSlicesArr();
      for (Slice next : activeSlices) {
        Map<String, Replica> replicasMap = next.getReplicasMap();
        if (replicasMap != null) {
          for (Map.Entry<String, Replica> entry : replicasMap.entrySet()) {
            Replica replica = entry.getValue();
            if (!localCoreNodeName.equals(replica.getName()) &&
                replica.getState() == Replica.State.ACTIVE &&
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

  @SuppressWarnings({"rawtypes"})
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
      int remoteVersion = -1;
      try (HttpSolrClient solr = new HttpSolrClient.Builder(coreUrl).build()) {
        // eventually, this loop will get killed by the ExecutorService's timeout
        while (remoteVersion == -1 || remoteVersion < expectedZkVersion) {
          try {
            HttpSolrClient.HttpUriRequestResponse mrr = solr.httpUriRequest(this);
            NamedList<Object> zkversionResp = mrr.future.get();
            if (zkversionResp != null)
              remoteVersion = (Integer)zkversionResp.get("zkversion");

            if (remoteVersion < expectedZkVersion) {
              // rather than waiting and re-polling, let's be proactive and tell the replica
              // to refresh its schema from ZooKeeper, if that fails, then the
              //Thread.sleep(1000); // slight delay before requesting version again
              log.error("Replica {} returned schema version {} and has not applied schema version {}"
                  , coreUrl, remoteVersion, expectedZkVersion);
            }

          } catch (Exception e) {
            if (e instanceof InterruptedException) {
              break; // stop looping
            } else {
              log.warn("Failed to get /schema/zkversion from {} due to: ", coreUrl, e);
            }
          }
        }
      }
      return remoteVersion;
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
      if (copyFieldNames == null){
        copyFieldNames = Collections.emptyMap();
      }
      newSchema = shallowCopy(true);

      for (SchemaField newField : newFields) {
        if (null != newSchema.fields.get(newField.getName())) {
          String msg = "Field '" + newField.getName() + "' already exists.";
          throw new FieldExistsException(ErrorCode.BAD_REQUEST, msg);
        }
        newSchema.fields.put(newField.getName(), newField);

        if (null != newField.getDefaultValue()) {
          if (log.isDebugEnabled()) {
            log.debug("{} contains default value: {}", newField.getName(), newField.getDefaultValue());
          }
          newSchema.fieldsWithDefaultValue.add(newField);
        }
        if (newField.isRequired()) {
          if (log.isDebugEnabled()) {
            log.debug("{} is required in this schema", newField.getName());
          }
          newSchema.requiredFields.add(newField);
        }
        Collection<String> copyFields = copyFieldNames.get(newField.getName());
        if (copyFields != null) {
          for (String copyField : copyFields) {
            newSchema.registerCopyField(newField.getName(), copyField);
          }
        }
      }

      newSchema.postReadInform();

      newSchema.refreshAnalyzers();

      if(persist) {
        success = newSchema.persistManagedSchema(false); // don't just create - update it if it already exists
        if (success) {
          log.debug("Added field(s): {}", newFields);
        } else {
          log.error("Failed to add field(s): {}", newFields);
          newSchema = null;
        }
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
          newSchema.fields.remove(name);
          newSchema.fieldsWithDefaultValue.remove(field);
          newSchema.requiredFields.remove(field);
        } else {
          String msg = "The field '" + name + "' is not present in this schema, and so cannot be deleted.";
          throw new SolrException(ErrorCode.BAD_REQUEST, msg);
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
  @SuppressWarnings({"unchecked"})
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
      newSchema.copyFieldsMap = cloneCopyFieldsMap(copyFieldsMap);
      newSchema.copyFieldTargetCounts
          = (Map<SchemaField,Integer>)((HashMap<SchemaField,Integer>)copyFieldTargetCounts).clone();
      newSchema.dynamicCopyFields = new DynamicCopy[dynamicCopyFields.length];
      System.arraycopy(dynamicCopyFields, 0, newSchema.dynamicCopyFields, 0, dynamicCopyFields.length);

      // Drop the old field
      newSchema.fields.remove(fieldName);
      newSchema.fieldsWithDefaultValue.remove(oldField);
      newSchema.requiredFields.remove(oldField);

      // Add the replacement field
      SchemaField replacementField = SchemaField.create(fieldName, replacementFieldType, replacementArgs);
      newSchema.fields.put(fieldName, replacementField);
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

      newSchema.postReadInform();
      newSchema.refreshAnalyzers();
      if (persist) {
        success = newSchema.persistManagedSchema(false); // don't just create - update it if it already exists
        if (success) {
          log.debug("Added dynamic field(s): {}", newDynamicFields);
        } else {
          log.error("Failed to add dynamic field(s): {}", newDynamicFields);
        }
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
          newSchema.dynamicFields = new DynamicField[] {};
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
  @SuppressWarnings({"unchecked"})
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
      newSchema.postReadInform();
      newSchema.refreshAnalyzers();
      if(persist) {
        success = newSchema.persistManagedSchema(false); // don't just create - update it if it already exists
        if (success) {
          if (log.isDebugEnabled()) {
            log.debug("Added copy fields for {} sources", copyFields.size());
          }
        } else {
          log.error("Failed to add copy fields for {} sources", copyFields.size());
        }
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
  @SuppressWarnings({"unchecked"})
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
            dynamicCopyFields = new DynamicCopy[] {};
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
        // source or destination either could be explicit field which matches dynamic rule
        SchemaField source = getFieldOrNull(copyField.getSource().getName());
        SchemaField destination = getFieldOrNull(copyField.getDestination().getName());
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

    // we shallow copied fieldTypes, but since we're changing them, we need to do a true
    // deep copy before adding the new field types
    @SuppressWarnings({"unchecked"})
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

    newSchema.postReadInform();

    newSchema.refreshAnalyzers();

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
      } else {
        // this is unlikely to happen as most errors are handled as exceptions in the persist code
        log.error("Failed to add field types: {}", fieldTypeList);
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Failed to persist updated schema due to underlying storage issue; check log for more details!");
      }
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
      for (String name : names) {
        newSchema.fieldTypes.remove(name);
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
  @SuppressWarnings({"unchecked"})
  public ManagedIndexSchema replaceFieldType(String typeName, String replacementClassName, Map<String,Object> replacementArgs) {
    ManagedIndexSchema newSchema;
    if (isMutable) {
      if ( ! fieldTypes.containsKey(typeName)) {
        String msg = "The field type '" + typeName + "' is not present in this schema, and so cannot be replaced.";
        throw new SolrException(ErrorCode.BAD_REQUEST, msg);
      }
      newSchema = shallowCopy(true);
      // clone data structures before modifying them
      newSchema.fieldTypes = (Map<String,FieldType>)((HashMap<String,FieldType>)fieldTypes).clone();
      newSchema.copyFieldsMap = cloneCopyFieldsMap(copyFieldsMap);
      newSchema.copyFieldTargetCounts
          = (Map<SchemaField,Integer>)((HashMap<SchemaField,Integer>)copyFieldTargetCounts).clone();
      newSchema.dynamicCopyFields = new DynamicCopy[dynamicCopyFields.length];
      System.arraycopy(dynamicCopyFields, 0, newSchema.dynamicCopyFields, 0, dynamicCopyFields.length);
      newSchema.dynamicFields = new DynamicField[dynamicFields.length];
      System.arraycopy(dynamicFields, 0, newSchema.dynamicFields, 0, dynamicFields.length);

      newSchema.fieldTypes.remove(typeName);
      FieldType replacementFieldType = newSchema.newFieldType(typeName, replacementClassName, replacementArgs);
      newSchema.fieldTypes.put(typeName, replacementFieldType);

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
        newSchema.fields.put(replacementField.getName(), replacementField);
      }
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
  protected void postReadInform() {
    super.postReadInform();
    for (FieldType fieldType : fieldTypes.values()) {
      informResourceLoaderAwareObjectsForFieldType(fieldType);
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
    Map<String,FieldType> newFieldTypes = new HashMap<>();
    List<SchemaAware> schemaAwareList = new ArrayList<>();
    FieldTypePluginLoader typeLoader = new FieldTypePluginLoader(this, newFieldTypes, schemaAwareList);
    typeLoader.loadSingle(solrClassLoader, new DOMConfigNode(FieldTypeXmlAdapter.toNode(options)));
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
          informAware(loader, (ResourceLoaderAware) next);
        } catch (IOException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
      }
    }

    TokenizerFactory tokenizerFactory = chain.getTokenizerFactory();
    if (tokenizerFactory instanceof ResourceLoaderAware) {
      try {
        informAware(loader, (ResourceLoaderAware) tokenizerFactory);
      } catch (IOException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    TokenFilterFactory[] filters = chain.getTokenFilterFactories();
    for (TokenFilterFactory next : filters) {
      if (next instanceof ResourceLoaderAware) {
        try {
          informAware(loader, (ResourceLoaderAware) next);
        } catch (IOException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
      }
    }
  }

  private ManagedIndexSchema(Version luceneVersion, SolrResourceLoader loader, boolean isMutable,
                             String managedSchemaResourceName, int schemaZkVersion, Object schemaUpdateLock, Properties substitutableProps) {
    super(luceneVersion, loader, substitutableProps);
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
  ManagedIndexSchema shallowCopy(boolean includeFieldDataStructures) {
    ManagedIndexSchema newSchema = new ManagedIndexSchema
        (luceneVersion, loader, isMutable, managedSchemaResourceName, schemaZkVersion, getSchemaUpdateLock(), substitutableProperties);

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
}

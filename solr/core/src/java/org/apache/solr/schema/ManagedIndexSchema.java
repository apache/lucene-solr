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
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPath;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

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
      writer = new OutputStreamWriter(out, "UTF-8");
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
    try {
      // Persist the managed schema
      StringWriter writer = new StringWriter();
      persist(writer);

      final byte[] data = writer.toString().getBytes("UTF-8");
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
          log.info("Persisted managed schema at " + managedSchemaPath);
        } catch (KeeperException.BadVersionException e) {
          log.info("Failed to persist managed schema at " + managedSchemaPath 
                  + " - version mismatch");
          success = false;
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
    return success; 
  }

  @Override
  public ManagedIndexSchema addField(SchemaField newField) {
    return addFields(Arrays.asList(newField));
  }

  public class FieldExistsException extends SolrException {
    public FieldExistsException(ErrorCode code, String msg) {
      super(code, msg);
    }
  }
  
  @Override
  public ManagedIndexSchema addFields(Collection<SchemaField> newFields) {
    ManagedIndexSchema newSchema = null;
    if (isMutable) {
      boolean success = false;
      while ( ! success) { // optimistic concurrency
        // even though fields is volatile, we need to synchronize to avoid two addFields
        // happening concurrently (and ending up missing one of them)
        synchronized (getSchemaUpdateLock()) {
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
          }
          // Run the callbacks on SchemaAware now that everything else is done
          for (SchemaAware aware : newSchema.schemaAware) {
            aware.inform(newSchema);
          }
          newSchema.refreshAnalyzers();
          success = newSchema.persistManagedSchema(false); // don't just create - update it if it already exists
          if (success) {
            log.debug("Added field(s): {}", newFields);
          }
        }
        // release the lock between tries to allow the schema reader to update the schema & schemaZkVersion
      }
    } else {
      String msg = "This ManagedIndexSchema is not mutable.";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
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
      newSchema.loadFields(document, xpath);
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
  
  public Object getSchemaUpdateLock() {
    return schemaUpdateLock;
  }
}

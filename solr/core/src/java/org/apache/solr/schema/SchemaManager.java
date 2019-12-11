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

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.schema.FieldType.CLASS_NAME;
import static org.apache.solr.schema.IndexSchema.DESTINATION;
import static org.apache.solr.schema.IndexSchema.MAX_CHARS;
import static org.apache.solr.schema.IndexSchema.NAME;
import static org.apache.solr.schema.IndexSchema.SOURCE;
import static org.apache.solr.schema.IndexSchema.SchemaProps.Handler.COPY_FIELDS;
import static org.apache.solr.schema.IndexSchema.SchemaProps.Handler.DYNAMIC_FIELDS;
import static org.apache.solr.schema.IndexSchema.SchemaProps.Handler.FIELDS;
import static org.apache.solr.schema.IndexSchema.SchemaProps.Handler.FIELD_TYPES;
import static org.apache.solr.schema.IndexSchema.TYPE;

/**
 * A utility class to manipulate schema using the bulk mode.
 * This class takes in all the commands and processes them completely.
 * It is an all or nothing operation.
 */
public class SchemaManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  ManagedIndexSchema managedIndexSchema;

  private static final Map<String, String> level2;

  static {
    Map s = Utils.makeMap(
        FIELD_TYPES.nameLower, null,
        FIELDS.nameLower, "fl",
        DYNAMIC_FIELDS.nameLower, "fl",
        COPY_FIELDS.nameLower, null
    );

    level2 = Collections.unmodifiableMap(s);

  }

  private static Set<String> subPaths = new HashSet<>(Arrays.asList(
      "version",
      "uniquekey",
      "name",
      "similarity",
      "defaultsearchfield",
      "solrqueryparser",
      "zkversion"
  ));

  static {
    subPaths.addAll(level2.keySet());

  }

  public static Set<String> getSubPaths() {
    return subPaths;
  }
  /**
   * Take in a JSON command set and execute them. It tries to capture as many errors
   * as possible instead of failing at the first error it encounters
   * @return List of errors. If the List is empty then the operation was successful.
   */
  public List doCmdOperations(List<CommandOperation> cmds, SolrCore core, SolrParams params) throws Exception {
    List errs = CommandOperation.captureErrors(cmds);
    if (!errs.isEmpty()) return errs;

    IndexSchema schema = core.getLatestSchema();
    if (schema instanceof ManagedIndexSchema && schema.isMutable()) {
      return doOperations(cmds, core, params);
    } else {
      return singletonList(singletonMap(CommandOperation.ERR_MSGS, "schema is not editable"));
    }
  }
  public List doCmdOperations(String name,List<CommandOperation> cmds,
                              ManagedIndexSchema schema,SolrConfig config,
                              SolrParams params,SolrResourceLoader loader,ZkController controller) throws Exception {
    List errs = CommandOperation.captureErrors(cmds);
    if (!errs.isEmpty()) return errs;

    if (schema instanceof ManagedIndexSchema && schema.isMutable()) {
      return doOperations(name,cmds, schema, config, params,loader,controller);
    } else {
      return singletonList(singletonMap(CommandOperation.ERR_MSGS, "schema is not editable"));
    }
  }

  private List doOperations(String name, List<CommandOperation> operations, ManagedIndexSchema schema, SolrConfig config,
                            SolrParams params, SolrResourceLoader loader, ZkController controller)
      throws InterruptedException, IOException, KeeperException {
    //@todo check for non cloud
    //The default timeout is 10 minutes when no BaseSolrResource.UPDATE_TIMEOUT_SECS is specified
    int timeout = params.getInt(BaseSolrResource.UPDATE_TIMEOUT_SECS, 600);

    //If BaseSolrResource.UPDATE_TIMEOUT_SECS=0 or -1 then end time then we'll try for 10 mins ( default timeout )
    if (timeout < 1) {
      timeout = 600;
    }
    TimeOut timeOut = new TimeOut(timeout, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    String errorMsg = "Unable to persist managed schema. ";
    List errors ;

    synchronized (schema.getSchemaUpdateLock()) {
        managedIndexSchema = getFreshManagedSchema(schema,config,loader);
        for (CommandOperation op : operations) {
          OpType opType = OpType.get(op.name);
          if (opType != null) {
            opType.perform(op, this);
          } else {
            op.addError("No such operation : " + op.name);
          }
        }
        errors = CommandOperation.captureErrors(operations);
      if (!errors.isEmpty()) return errors;
        StringWriter sw = new StringWriter();
      try {
        managedIndexSchema.persist(sw);
        final SolrZkClient zkClient = controller.getZkClient();
        final String resourceLocation = ZkConfigManager.CONFIGS_ZKNODE + "/" + name + "/" + managedIndexSchema.getResourceName();
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        ZkController.updateResource(zkClient, resourceLocation, bytes, managedIndexSchema.getSchemaZkVersion());
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "unable to serialize schema");
        //unlikely
      }
    }
    if (errors.isEmpty() && timeOut.hasTimedOut()) {
      log.warn(errorMsg + "Timed out.");
      errors = singletonList(errorMsg + "Timed out.");
    }
    return errors;
  }

  private List doOperations(List<CommandOperation> operations, SolrCore core, SolrParams params)
      throws InterruptedException, IOException, KeeperException {
    //The default timeout is 10 minutes when no BaseSolrResource.UPDATE_TIMEOUT_SECS is specified
    int timeout = params.getInt(BaseSolrResource.UPDATE_TIMEOUT_SECS, 600);

    //If BaseSolrResource.UPDATE_TIMEOUT_SECS=0 or -1 then end time then we'll try for 10 mins ( default timeout )
    if (timeout < 1) {
      timeout = 600;
    }
    TimeOut timeOut = new TimeOut(timeout, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    String errorMsg = "Unable to persist managed schema. ";
    List errors = Collections.emptyList();
    int latestVersion = -1;

    synchronized (core.getLatestSchema().getSchemaUpdateLock()) {
      while (!timeOut.hasTimedOut()) {
        managedIndexSchema = getFreshManagedSchema(core);
        for (CommandOperation op : operations) {
          OpType opType = OpType.get(op.name);
          if (opType != null) {
            opType.perform(op, this);
          } else {
            op.addError("No such operation : " + op.name);
          }
        }
        errors = CommandOperation.captureErrors(operations);
        if (!errors.isEmpty()) break;
        SolrResourceLoader loader = core.getResourceLoader();
        if (loader instanceof ZkSolrResourceLoader) {
          ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader) loader;
          StringWriter sw = new StringWriter();
          try {
            managedIndexSchema.persist(sw);
          } catch (IOException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "unable to serialize schema");
            //unlikely
          }

          try {
            latestVersion = ZkController.persistConfigResourceToZooKeeper
                (zkLoader, managedIndexSchema.getSchemaZkVersion(), managedIndexSchema.getResourceName(),
                    sw.toString().getBytes(StandardCharsets.UTF_8), true);
            core.getCoreContainer().reload(core.getName());
            break;
          } catch (ZkController.ResourceModifiedInZkException e) {
            log.info("Schema was modified by another node. Retrying..");
          }
        } else {
          try {
            //only for non cloud stuff
            managedIndexSchema.persistManagedSchema(false);
            core.setLatestSchema(managedIndexSchema);
            core.getCoreContainer().reload(core.getName());
          } catch (SolrException e) {
            log.warn(errorMsg);
            errors = singletonList(errorMsg + e.getMessage());
          }
          break;
        }
      }
    }
    if (core.getResourceLoader() instanceof ZkSolrResourceLoader) {
      // Don't block further schema updates while waiting for a pending update to propagate to other replicas.
      // This reduces the likelihood of a (time-limited) distributed deadlock during concurrent schema updates.
      waitForOtherReplicasToUpdate(core, timeOut, latestVersion);
    }
    if (errors.isEmpty() && timeOut.hasTimedOut()) {
      log.warn(errorMsg + "Timed out.");
      errors = singletonList(errorMsg + "Timed out.");
    }
    return errors;
  }

  private void waitForOtherReplicasToUpdate(SolrCore core, TimeOut timeOut, int latestVersion) {
    CoreDescriptor cd = core.getCoreDescriptor();
    String collection = cd.getCollectionName();
    if (collection != null) {
      if (timeOut.hasTimedOut()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Not enough time left to update replicas. However, the schema is updated already.");
      }
      ManagedIndexSchema.waitForSchemaZkVersionAgreement(collection, cd.getCloudDescriptor().getCoreNodeName(),
          latestVersion, core.getCoreContainer().getZkController(), (int) timeOut.timeLeft(TimeUnit.SECONDS));
    }
  }

  public enum OpType {
    ADD_FIELD_TYPE("add-field-type") {
      @Override public boolean perform(CommandOperation op, SchemaManager mgr) {
        String name = op.getStr(NAME);
        String className = op.getStr(CLASS_NAME);
        if (op.hasError())
          return false;
        try {
          FieldType fieldType = mgr.managedIndexSchema.newFieldType(name, className, op.getDataMap());
          mgr.managedIndexSchema = mgr.managedIndexSchema.addFieldTypes(singletonList(fieldType), false);
          return true;
        } catch (Exception e) {
          op.addError(getErrorStr(e));
          return false;
        }
      }
    },
    ADD_COPY_FIELD("add-copy-field") {
      @Override public boolean perform(CommandOperation op, SchemaManager mgr) {
        String src  = op.getStr(SOURCE);
        List<String> dests = op.getStrs(DESTINATION);

        int maxChars = CopyField.UNLIMITED; // If maxChars is not specified, there is no limit on copied chars
        String maxCharsStr = op.getStr(MAX_CHARS, null);
        if (null != maxCharsStr) {
          try {
            maxChars = Integer.parseInt(maxCharsStr);
          } catch (NumberFormatException e) {
            op.addError("Exception parsing " + MAX_CHARS + " '" + maxCharsStr + "': " + getErrorStr(e));
          }
          if (maxChars < 0) {
            op.addError(MAX_CHARS + " '" + maxCharsStr + "' is negative.");
          }
        }

        if (op.hasError())
          return false;
        if ( ! op.getValuesExcluding(SOURCE, DESTINATION, MAX_CHARS).isEmpty()) {
          op.addError("Only the '" + SOURCE + "', '" + DESTINATION + "' and '" + MAX_CHARS
              + "' params are allowed with the 'add-copy-field' operation");
          return false;
        }
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.addCopyFields(src, dests, maxChars);
          return true;
        } catch (Exception e) {
          op.addError(getErrorStr(e));
          return false;
        }
      }
    },
    ADD_FIELD("add-field") {
      @Override public boolean perform(CommandOperation op, SchemaManager mgr) {
        String name = op.getStr(NAME);
        String type = op.getStr(TYPE);
        if (op.hasError())
          return false;
        try {
          SchemaField field = mgr.managedIndexSchema.newField(name, type, op.getValuesExcluding(NAME, TYPE));
          mgr.managedIndexSchema
              = mgr.managedIndexSchema.addFields(singletonList(field), Collections.emptyMap(), false);
          return true;
        } catch (Exception e) {
          op.addError(getErrorStr(e));
          return false;
        }
      }
    },
    ADD_DYNAMIC_FIELD("add-dynamic-field") {
      @Override public boolean perform(CommandOperation op, SchemaManager mgr) {
        String name = op.getStr(NAME);
        String type = op.getStr(TYPE);
        if (op.hasError())
          return false;
        try {
          SchemaField field = mgr.managedIndexSchema.newDynamicField(name, type, op.getValuesExcluding(NAME, TYPE));
          mgr.managedIndexSchema
              = mgr.managedIndexSchema.addDynamicFields(singletonList(field), Collections.emptyMap(), false);
          return true;
        } catch (Exception e) {
          op.addError(getErrorStr(e));
          return false;
        }
      }
    },
    DELETE_FIELD_TYPE("delete-field-type") {
      @Override public boolean perform(CommandOperation op, SchemaManager mgr) {
        String name = op.getStr(NAME);
        if (op.hasError())
          return false;
        if ( ! op.getValuesExcluding(NAME).isEmpty()) {
          op.addError("Only the '" + NAME + "' param is allowed with the 'delete-field-type' operation");
          return false;
        }
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.deleteFieldTypes(singleton(name));
          return true;
        } catch (Exception e) {
          op.addError(getErrorStr(e));
          return false;
        }
      }
    },
    DELETE_COPY_FIELD("delete-copy-field") {
      @Override public boolean perform(CommandOperation op, SchemaManager mgr) {
        String source = op.getStr(SOURCE);
        List<String> dests = op.getStrs(DESTINATION);
        if (op.hasError())
          return false;
        if ( ! op.getValuesExcluding(SOURCE, DESTINATION).isEmpty()) {
          op.addError("Only the '" + SOURCE + "' and '" + DESTINATION
              + "' params are allowed with the 'delete-copy-field' operation");
          return false;
        }
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.deleteCopyFields(singletonMap(source, dests));
          return true;
        } catch (Exception e) {
          op.addError(getErrorStr(e));
          return false;
        }
      }
    },
    DELETE_FIELD("delete-field") {
      @Override public boolean perform(CommandOperation op, SchemaManager mgr) {
        String name = op.getStr(NAME);
        if (op.hasError())
          return false;
        if ( ! op.getValuesExcluding(NAME).isEmpty()) {
          op.addError("Only the '" + NAME + "' param is allowed with the 'delete-field' operation");
          return false;
        }
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.deleteFields(singleton(name));
          return true;
        } catch (Exception e) {
          op.addError(getErrorStr(e));
          return false;
        }
      }
    },
    DELETE_DYNAMIC_FIELD("delete-dynamic-field") {
      @Override public boolean perform(CommandOperation op, SchemaManager mgr) {
        String name = op.getStr(NAME);
        if (op.hasError())
          return false;
        if ( ! op.getValuesExcluding(NAME).isEmpty()) {
          op.addError("Only the '" + NAME + "' param is allowed with the 'delete-dynamic-field' operation");
          return false;
        }
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.deleteDynamicFields(singleton(name));
          return true;
        } catch (Exception e) {
          op.addError(getErrorStr(e));
          return false;
        }
      }
    },
    REPLACE_FIELD_TYPE("replace-field-type") {
      @Override public boolean perform(CommandOperation op, SchemaManager mgr) {
        String name = op.getStr(NAME);
        String className = op.getStr(CLASS_NAME);
        if (op.hasError())
          return false;
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.replaceFieldType(name, className, op.getDataMap());
          return true;
        } catch (Exception e) {
          op.addError(getErrorStr(e));
          return false;
        }
      }
    },
    REPLACE_FIELD("replace-field") {
      @Override public boolean perform(CommandOperation op, SchemaManager mgr) {
        String name = op.getStr(NAME);
        String type = op.getStr(TYPE);
        if (op.hasError())
          return false;
        FieldType ft = mgr.managedIndexSchema.getFieldTypeByName(type);
        if (ft == null) {
          op.addError("No such field type '" + type + "'");
          return false;
        }
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.replaceField(name, ft, op.getValuesExcluding(NAME, TYPE));
          return true;
        } catch (Exception e) {
          op.addError(getErrorStr(e));
          return false;
        }
      }
    },
    REPLACE_DYNAMIC_FIELD("replace-dynamic-field") {
      @Override public boolean perform(CommandOperation op, SchemaManager mgr) {
        String name = op.getStr(NAME);
        String type = op.getStr(TYPE);
        if (op.hasError())
          return false;
        FieldType ft = mgr.managedIndexSchema.getFieldTypeByName(type);
        if (ft == null) {
          op.addError("No such field type '" + type + "'");
          return  false;
        }
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.replaceDynamicField(name, ft, op.getValuesExcluding(NAME, TYPE));
          return true;
        } catch (Exception e) {
          op.addError(getErrorStr(e));
          return false;
        }
      }
    };

    public abstract boolean perform(CommandOperation op, SchemaManager mgr);

    public static OpType get(String label) {
      return Nested.OP_TYPES.get(label);
    }

    private static class Nested { // Initializes contained static map before any enum ctor
      static final Map<String,OpType> OP_TYPES = new HashMap<>();
    }

    private OpType(String label) {
      Nested.OP_TYPES.put(label, this);
    }
  }

  public static String getErrorStr(Exception e) {
    StringBuilder sb = new StringBuilder();
    Throwable cause = e;
    for (int i = 0 ; i < 5 ; i++) {
      sb.append(cause.getMessage()).append("\n");
      if (cause.getCause() == null || cause.getCause() == cause) break;
      cause = cause.getCause();
    }
    return sb.toString();
  }

  public static ManagedIndexSchema getFreshManagedSchema(SolrCore core) throws IOException {
    return getFreshManagedSchema(core.getLatestSchema(), core.getSolrConfig(), core.getResourceLoader());
  }

  private static ManagedIndexSchema getFreshManagedSchema(
      IndexSchema currentSchema, SolrConfig config, SolrResourceLoader resourceLoader) throws IOException{

    String name = currentSchema.getResourceName();
    if (resourceLoader instanceof ZkSolrResourceLoader) {
      InputStream in = resourceLoader.openResource(name);
      if (in instanceof ZkSolrResourceLoader.ZkByteArrayInputStream) {
        int version = ((ZkSolrResourceLoader.ZkByteArrayInputStream) in).getStat().getVersion();
        log.info("managed schema loaded . version : {} ", version);
        return new ManagedIndexSchema(config, name, new InputSource(in), true, name, version,
            currentSchema.getSchemaUpdateLock());
      } else {
        return (ManagedIndexSchema) currentSchema;
      }
    } else {
      return (ManagedIndexSchema) currentSchema;
    }
  }
  public Map<String, Object> executeGET(String path, IndexSchema schema, SolrParams reqParams, SolrResourceLoader resourceLoader)
      throws KeeperException, InterruptedException {
    Map<String, Object> params = new HashMap<>();
    switch (path) {
      case "/schema":
        params.put(IndexSchema.SCHEMA, schema.getNamedPropertyValues());
        break;
      case "/schema/version":
        params.put(IndexSchema.VERSION, schema.getVersion());
        break;
      case "/schema/uniquekey":
        params.put(IndexSchema.UNIQUE_KEY, schema.getUniqueKeyField().getName());
        break;
      case "/schema/similarity":
        params.put(IndexSchema.SIMILARITY, schema.getSimilarityFactory().getNamedPropertyValues());
        break;
      case "/schema/name": {
        final String schemaName = schema.getSchemaName();
        if (null == schemaName) {
          String message = "Schema has no name";
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, message);
        }
        params.put(IndexSchema.NAME, schemaName);
        break;
      }
      case "/schema/zkversion": {
        int refreshIfBelowVersion = -1;
        Object refreshParam = reqParams.get("refreshIfBelowVersion");
        if (refreshParam != null)
          refreshIfBelowVersion = (refreshParam instanceof Number) ? ((Number) refreshParam).intValue()
              : Integer.parseInt(refreshParam.toString());
        int zkVersion = -1;
        if (schema instanceof ManagedIndexSchema) {
          ManagedIndexSchema managed = (ManagedIndexSchema) schema;
          zkVersion = managed.getSchemaZkVersion();
          if (refreshIfBelowVersion != -1 && zkVersion < refreshIfBelowVersion) {
            log.info("REFRESHING SCHEMA (refreshIfBelowVersion=" + refreshIfBelowVersion +
                ", currentVersion=" + zkVersion + ") before returning version!");
            if (resourceLoader instanceof ZkSolrResourceLoader) {
              ZkSolrResourceLoader zkSolrResourceLoader = (ZkSolrResourceLoader) resourceLoader;
              ZkIndexSchemaReader zkIndexSchemaReader = zkSolrResourceLoader.getZkIndexSchemaReader();
              managed = zkIndexSchemaReader.refreshSchemaFromZk(refreshIfBelowVersion);
              zkVersion = managed.getSchemaZkVersion();
            } else {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,"Doesn't implement /zkversion");
            }
          }
        }
        params.put("zkversion", zkVersion);
        break;
      }
      default: {
        List<String> parts = StrUtils.splitSmart(path, '/', true);
        if (parts.size() > 1 && level2.containsKey(parts.get(1))) {
          String realName = parts.get(1);
          String fieldName = IndexSchema.nameMapping.get(realName);

          String pathParam = level2.get(realName);
          //@todo check
//            if (parts.size() > 2) {
//              reqParams = SolrParams.wrapDefaults(new MapSolrParams(singletonMap(pathParam, parts.get(2))), req.getParams());
//            }
          Map propertyValues = schema.getNamedPropertyValues(realName, reqParams);
          Object o = propertyValues.get(fieldName);
          if (parts.size() > 2) {
            String name = parts.get(2);
            if (o instanceof List) {
              List list = (List) o;
              for (Object obj : list) {
                if (obj instanceof SimpleOrderedMap) {
                  SimpleOrderedMap simpleOrderedMap = (SimpleOrderedMap) obj;
                  if (name.equals(simpleOrderedMap.get("name"))) {
                    params.put(fieldName.substring(0, realName.length() - 1), simpleOrderedMap);
                    return params;
                  }
                }
              }
            }
            throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path " + path);
          } else {
            params.put(fieldName, o);
          }
          return params;
        }

        throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path " + path);
      }
    }
    return params;
  }
}

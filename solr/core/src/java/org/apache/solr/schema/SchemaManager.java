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


import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.util.CommandOperation;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.schema.FieldType.CLASS_NAME;
import static org.apache.solr.schema.IndexSchema.DESTINATION;
import static org.apache.solr.schema.IndexSchema.NAME;
import static org.apache.solr.schema.IndexSchema.SOURCE;
import static org.apache.solr.schema.IndexSchema.TYPE;

/**
 * A utility class to manipulate schema using the bulk mode.
 * This class takes in all the commands and processes them completely.
 * It is an all or nothing operation.
 */
public class SchemaManager {
  private static final Logger log = LoggerFactory.getLogger(SchemaManager.class);

  final SolrQueryRequest req;
  ManagedIndexSchema managedIndexSchema;

  public SchemaManager(SolrQueryRequest req){
    this.req = req;
  }

  /**
   * Take in a JSON command set and execute them. It tries to capture as many errors
   * as possible instead of failing at the first error it encounters
   * @param reader The input as a Reader
   * @return List of errors. If the List is empty then the operation was successful.
   */
  public List performOperations(Reader reader) throws Exception {
    List<CommandOperation> ops;
    try {
      ops = CommandOperation.parse(reader);
    } catch (Exception e) {
      String msg = "Error parsing schema operations ";
      log.warn(msg, e);
      return Collections.singletonList(singletonMap(CommandOperation.ERR_MSGS, msg + ":" + e.getMessage()));
    }
    List errs = CommandOperation.captureErrors(ops);
    if (!errs.isEmpty()) return errs;

    IndexSchema schema = req.getCore().getLatestSchema();
    if (!(schema instanceof ManagedIndexSchema)) {
      return singletonList(singletonMap(CommandOperation.ERR_MSGS, "schema is not editable"));
    }
    synchronized (schema.getSchemaUpdateLock()) {
      return doOperations(ops);
    }
  }

  private List doOperations(List<CommandOperation> operations) throws InterruptedException, IOException, KeeperException {
    int timeout = req.getParams().getInt(BaseSolrResource.UPDATE_TIMEOUT_SECS, -1);
    long startTime = System.nanoTime();
    long endTime = timeout > 0 ? System.nanoTime() + (timeout * 1000 * 1000) : Long.MAX_VALUE;
    SolrCore core = req.getCore();
    while (System.nanoTime() < endTime) {
      managedIndexSchema = getFreshManagedSchema();
      for (CommandOperation op : operations) {
        OpType opType = OpType.get(op.name);
        if (opType != null) {
          opType.perform(op, this);
        } else {
          op.addError("No such operation : " + op.name);
        }
      }
      List errs = CommandOperation.captureErrors(operations);
      if (!errs.isEmpty()) return errs;
      SolrResourceLoader loader = req.getCore().getResourceLoader();
      if (loader instanceof ZkSolrResourceLoader) {
        ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader) loader;
        StringWriter sw = new StringWriter();
        try {
          managedIndexSchema.persist(sw);
        } catch (IOException e) {
          log.info("race condition ");
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "unable to serialize schema");
          //unlikely
        }

        try {
          ZkController.persistConfigResourceToZooKeeper(zkLoader,
              managedIndexSchema.getSchemaZkVersion(),
              managedIndexSchema.getResourceName(),
              sw.toString().getBytes(StandardCharsets.UTF_8),
              true);
          return Collections.emptyList();
        } catch (ZkController.ResourceModifiedInZkException e) {
          log.info("Race condition schema modified by another node");
        } catch (Exception e) {
          String s = "Exception persisting schema";
          log.warn(s, e);
          return singletonList(s + e.getMessage());
        }
      } else {
        try {
          //only for non cloud stuff
          managedIndexSchema.persistManagedSchema(false);
          core.setLatestSchema(managedIndexSchema);
          waitForOtherReplicasToUpdate(timeout, startTime);
          return Collections.emptyList();
        } catch (ManagedIndexSchema.SchemaChangedInZkException e) {
          String s = "Failed to update schema because schema is modified";
          log.warn(s, e);
        } catch (Exception e) {
          String s = "Exception persisting schema";
          log.warn(s, e);
          return singletonList(s + e.getMessage());
        }
      }
    }
    return singletonList("Unable to persist schema");
  }

  private void waitForOtherReplicasToUpdate(int timeout, long startTime) {
    if (timeout > 0 && managedIndexSchema.getResourceLoader() instanceof ZkSolrResourceLoader) {
      CoreDescriptor cd = req.getCore().getCoreDescriptor();
      String collection = cd.getCollectionName();
      if (collection != null) {
        ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader) managedIndexSchema.getResourceLoader();
        long timeLeftSecs = timeout - TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        if (timeLeftSecs <= 0) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Not enough time left to update replicas. However, the schema is updated already.");
        }
        ManagedIndexSchema.waitForSchemaZkVersionAgreement(collection,
            cd.getCloudDescriptor().getCoreNodeName(),
            (managedIndexSchema).getSchemaZkVersion(),
            zkLoader.getZkController(),
            (int) timeLeftSecs);
      }
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
        if (op.hasError())
          return false;
        if ( ! op.getValuesExcluding(SOURCE, DESTINATION).isEmpty()) {
          op.addError("Only the '" + SOURCE + "' and '" + DESTINATION
              + "' params are allowed with the 'add-copy-field' operation");
          return false;
        }
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.addCopyFields(singletonMap(src, dests), false);
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
        FieldType ft = mgr.managedIndexSchema.getFieldTypeByName(type);
        if (ft == null) {
          op.addError("No such field type '" + type + "'");
          return false;
        }
        try {
          SchemaField field = SchemaField.create(name, ft, op.getValuesExcluding(NAME, TYPE));
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
        FieldType ft = mgr.managedIndexSchema.getFieldTypeByName(type);
        if (ft == null) {
          op.addError("No such field type '" + type + "'");
          return  false;
        }
        try {
          SchemaField field = SchemaField.create(name, ft, op.getValuesExcluding(NAME, TYPE)); 
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

  public ManagedIndexSchema getFreshManagedSchema() throws IOException, KeeperException, InterruptedException {
    SolrResourceLoader resourceLoader = req.getCore().getResourceLoader();
    if (resourceLoader instanceof ZkSolrResourceLoader) {
      InputStream in = resourceLoader.openResource(req.getSchema().getResourceName());
      if (in instanceof ZkSolrResourceLoader.ZkByteArrayInputStream) {
        int version = ((ZkSolrResourceLoader.ZkByteArrayInputStream) in).getStat().getVersion();
        log.info("managed schema loaded . version : {} ", version);
        return new ManagedIndexSchema(req.getCore().getSolrConfig(),
            req.getSchema().getResourceName() ,new InputSource(in),
            true,
            req.getSchema().getResourceName(),
            version,new Object());
      } else {
        return (ManagedIndexSchema) req.getCore().getLatestSchema();
      }
    } else {
      return (ManagedIndexSchema) req.getCore().getLatestSchema();
    }
  }
}

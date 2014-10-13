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


import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.util.CommandOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.schema.FieldType.CLASS_NAME;
import static org.apache.solr.schema.IndexSchema.DESTINATION;
import static org.apache.solr.schema.IndexSchema.NAME;
import static org.apache.solr.schema.IndexSchema.SOURCE;
import static org.apache.solr.schema.IndexSchema.TYPE;

/**A utility class to manipulate schema using the bulk mode.
 * This class takes in all the commands and process them completely. It is an all or none
 * operation
 */
public class SchemaManager {
  private static final Logger log = LoggerFactory.getLogger(SchemaManager.class);

  final SolrQueryRequest req;
  ManagedIndexSchema managedIndexSchema;

  public static final String ADD_FIELD = "add-field";
  public static final String ADD_COPY_FIELD = "add-copy-field";
  public static final String ADD_DYNAMIC_FIELD = "add-dynamic-field";
  public static final String ADD_FIELD_TYPE = "add-field-type";

  private static final Set<String> KNOWN_OPS = new HashSet<>();
  static {
    KNOWN_OPS.add(ADD_COPY_FIELD);
    KNOWN_OPS.add(ADD_FIELD);
    KNOWN_OPS.add(ADD_DYNAMIC_FIELD);
    KNOWN_OPS.add(ADD_FIELD_TYPE);
  }

  public SchemaManager(SolrQueryRequest req){
    this.req = req;

  }

  /**Take in a JSON command set and execute them . It tries to capture as many errors
   * as possible instead of failing at the frst error it encounters
   * @param rdr The input as a Reader
   * @return Lis of errors . If the List is empty then the operation is successful.
   */
  public List performOperations(Reader rdr)  {
    List<CommandOperation> ops = null;
    try {
      ops = CommandOperation.parse(rdr);
    } catch (Exception e) {
      String msg= "Error parsing schema operations ";
      log.warn(msg  ,e );
      return Collections.singletonList(singletonMap(CommandOperation.ERR_MSGS, msg + ":" + e.getMessage()));
    }
    List errs = CommandOperation.captureErrors(ops);
    if(!errs.isEmpty()) return errs;

    IndexSchema schema = req.getCore().getLatestSchema();
    if (!(schema instanceof ManagedIndexSchema)) {
      return singletonList( singletonMap(CommandOperation.ERR_MSGS,"schema is not editable"));
    }

    synchronized (schema.getSchemaUpdateLock()) {
      return doOperations(ops);
    }

  }

  private List doOperations(List<CommandOperation> operations){
    int timeout = req.getParams().getInt(BaseSolrResource.UPDATE_TIMEOUT_SECS, -1);
    long startTime = System.nanoTime();
    long endTime = timeout >0  ? System.nanoTime()+ (timeout * 1000*1000) : Long.MAX_VALUE;
    SolrCore core = req.getCore();
    for(;System.nanoTime() < endTime ;) {
      managedIndexSchema = (ManagedIndexSchema) core.getLatestSchema();
      for (CommandOperation op : operations) {
        if (ADD_FIELD.equals(op.name) || ADD_DYNAMIC_FIELD.equals(op.name)) {
          applyAddField(op);
        } else if(ADD_COPY_FIELD.equals(op.name)) {
          applyAddCopyField(op);
        } else if(ADD_FIELD_TYPE.equals(op.name)) {
          applyAddType(op);

        } else {
          op.addError("No such operation : " + op.name);
        }
      }
      List errs = CommandOperation.captureErrors(operations);
      if (!errs.isEmpty()) return errs;

      try {
        managedIndexSchema.persistManagedSchema(false);
        core.setLatestSchema(managedIndexSchema);
        waitForOtherReplicasToUpdate(timeout, startTime);
        return EMPTY_LIST;
      } catch (ManagedIndexSchema.SchemaChangedInZkException e) {
        String s = "Failed to update schema because schema is modified";
        log.warn(s, e);
        continue;
      } catch (Exception e){
        String s = "Exception persisting schema";
        log.warn(s, e);
        return singletonList(s + e.getMessage());
      }
    }

    return singletonList("Unable to persist schema");

  }

  private void waitForOtherReplicasToUpdate(int timeout, long startTime) {
    if(timeout > 0 && managedIndexSchema.getResourceLoader()instanceof ZkSolrResourceLoader){
      CoreDescriptor cd = req.getCore().getCoreDescriptor();
      String collection = cd.getCollectionName();
      if (collection != null) {
        ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader) managedIndexSchema.getResourceLoader();
        long timeLeftSecs = timeout -   TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        if(timeLeftSecs<=0) throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Not enough time left to update replicas. However the schema is updated already");
        ManagedIndexSchema.waitForSchemaZkVersionAgreement(collection,
            cd.getCloudDescriptor().getCoreNodeName(),
            (managedIndexSchema).getSchemaZkVersion(),
            zkLoader.getZkController(),
            (int) timeLeftSecs);
      }

    }
  }

  private boolean applyAddType(CommandOperation op) {
    String name = op.getStr(NAME);
    String clz = op.getStr(CLASS_NAME);
    if(op.hasError())
      return false;
    try {
      FieldType fieldType = managedIndexSchema.newFieldType(name, clz, op.getDataMap());
      managedIndexSchema = managedIndexSchema.addFieldTypes(singletonList(fieldType), false);
      return true;
    } catch (Exception e) {
      op.addError(getErrorStr(e));
      return false;
    }
  }

  public static String getErrorStr(Exception e) {
    StringBuilder sb = new StringBuilder();
    Throwable cause= e;
    for(int i =0;i<5;i++) {
      sb.append(cause.getMessage()).append("\n");
      if(cause.getCause() == null || cause.getCause() == cause) break;
      cause = cause.getCause();
    }
    return sb.toString();
  }

  private boolean applyAddCopyField(CommandOperation op) {
    String src  = op.getStr(SOURCE);
    List<String> dest = op.getStrs(DESTINATION);
    if(op.hasError())
      return false;
    try {
      managedIndexSchema = managedIndexSchema.addCopyFields(Collections.<String,Collection<String>>singletonMap(src,dest), false);
      return true;
    } catch (Exception e) {
      op.addError(getErrorStr(e));
      return false;
    }
  }


  private boolean applyAddField( CommandOperation op) {
    String name = op.getStr(NAME);
    String type = op.getStr(TYPE);
    if(op.hasError())
      return false;
    FieldType ft = managedIndexSchema.getFieldTypeByName(type);
    if(ft==null){
      op.addError("No such field type '"+type+"'");
      return  false;
    }
    try {
      if(ADD_DYNAMIC_FIELD.equals(op.name)){
        managedIndexSchema = managedIndexSchema.addDynamicFields(
            singletonList(SchemaField.create(name, ft, op.getValuesExcluding(NAME, TYPE))),
            EMPTY_MAP,false);
      } else {
        managedIndexSchema = managedIndexSchema.addFields(
            singletonList( SchemaField.create(name, ft, op.getValuesExcluding(NAME, TYPE))),
            EMPTY_MAP,
            false);
      }
    } catch (Exception e) {
      op.addError(getErrorStr(e));
      return false;
    }
    return true;
  }

}

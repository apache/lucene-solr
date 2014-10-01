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
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;
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
    List<Operation> ops = null;
    try {
      ops = SchemaManager.parse(rdr);
    } catch (Exception e) {
      String msg= "Error parsing schema operations ";
      log.warn(msg  ,e );
      return Collections.singletonList(singletonMap(ERR_MSGS, msg + ":" + e.getMessage()));
    }
    List errs = captureErrors(ops);
    if(!errs.isEmpty()) return errs;

    IndexSchema schema = req.getCore().getLatestSchema();
    if (!(schema instanceof ManagedIndexSchema)) {
      return singletonList( singletonMap(ERR_MSGS,"schema is not editable"));
    }

    synchronized (schema.getSchemaUpdateLock()) {
      return doOperations(ops);
    }

  }

  private List<String> doOperations(List<Operation> operations){
    int timeout = req.getParams().getInt(BaseSolrResource.UPDATE_TIMEOUT_SECS, -1);
    long startTime = System.nanoTime();
    long endTime = timeout >0  ? System.nanoTime()+ (timeout * 1000*1000) : Long.MAX_VALUE;
    SolrCore core = req.getCore();
    for(;System.nanoTime() < endTime ;) {
      managedIndexSchema = (ManagedIndexSchema) core.getLatestSchema();
      for (Operation op : operations) {
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
      List errs = captureErrors(operations);
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

  private boolean applyAddType(Operation op) {
    String name = op.getStr(NAME);
    String clz = op.getStr(CLASS_NAME);
    if(op.hasError())
      return false;
    try {
      FieldType fieldType = managedIndexSchema.newFieldType(name, clz, (Map<String, ?>) op.commandData);
      managedIndexSchema = managedIndexSchema.addFieldTypes(singletonList(fieldType), false);
      return true;
    } catch (Exception e) {
      op.addError(getErrorStr(e));
      return false;
    }
  }

  private String getErrorStr(Exception e) {
    StringBuilder sb = new StringBuilder();
    Throwable cause= e;
    for(int i =0;i<5;i++) {
      sb.append(cause.getMessage()).append("\n");
      if(cause.getCause() == null || cause.getCause() == cause) break;
      cause = cause.getCause();
    }
    return sb.toString();
  }

  private boolean applyAddCopyField(Operation op) {
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


  private boolean applyAddField( Operation op) {
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


  public static class Operation {
    public final String name;
    private Object commandData;//this is most often a map
    private List<String> errors = new ArrayList<>();

    Operation(String operationName, Object metaData) {
      commandData = metaData;
      this.name = operationName;
      if(!KNOWN_OPS.contains(this.name)) errors.add("Unknown Operation :"+this.name);
    }

    public String getStr(String key, String def){
      String s = (String) getMapVal(key);
      return s == null ? def : s;
    }

    private Object getMapVal(String key) {
      if (commandData instanceof Map) {
        Map metaData = (Map) commandData;
        return metaData.get(key);
      } else {
        String msg= " value has to be an object for operation :"+name;
        if(!errors.contains(msg)) errors.add(msg);
        return null;
      }
    }

    public List<String> getStrs(String key){
      List<String> val = getStrs(key, null);
      if(val == null) errors.add("'"+key + "' is a required field");
      return val;

    }

    /**Get collection of values for a key. If only one val is present a
     * single value collection is returned
     */
    public List<String> getStrs(String key, List<String> def){
      Object v = getMapVal(key);
      if(v == null){
        return def;
      } else {
        if (v instanceof List) {
          ArrayList<String> l =  new ArrayList<>();
          for (Object o : (List)v) {
            l.add(String.valueOf(o));
          }
          if(l.isEmpty()) return def;
          return  l;
        } else {
          return singletonList(String.valueOf(v));
        }
      }

    }

    /**Get a required field. If missing it adds to the errors
     */
    public String getStr(String key){
      String s = getStr(key,null);
      if(s==null) errors.add("'"+key + "' is a required field");
      return s;
    }

    private Map errorDetails(){
       return makeMap(name, commandData, ERR_MSGS, errors);
    }

    public boolean hasError() {
      return !errors.isEmpty();
    }

    public void addError(String s) {
      errors.add(s);
    }

    /**Get all the values from the metadata for the command
     * without the specified keys
     */
    public Map getValuesExcluding(String... keys) {
      getMapVal(null);
      if(hasError()) return emptyMap();//just to verify the type is Map
      LinkedHashMap<String, Object> cp = new LinkedHashMap<>((Map<String,?>) commandData);
      if(keys == null) return cp;
      for (String key : keys) {
        cp.remove(key);
      }
      return cp;
    }


    public List<String> getErrors() {
      return errors;
    }
  }

  /**Parse the command operations into command objects
   */
  static List<Operation> parse(Reader rdr ) throws IOException {
    JSONParser parser = new JSONParser(rdr);

    ObjectBuilder ob = new ObjectBuilder(parser);

    if(parser.lastEvent() != JSONParser.OBJECT_START) {
      throw new RuntimeException("The JSON must be an Object of the form {\"command\": {...},...");
    }
    List<Operation> operations = new ArrayList<>();
    for(;;) {
      int ev = parser.nextEvent();
      if (ev==JSONParser.OBJECT_END) return operations;
      Object key =  ob.getKey();
      ev = parser.nextEvent();
      Object val = ob.getVal();
      if (val instanceof List) {
        List list = (List) val;
        for (Object o : list) {
          operations.add(new Operation(String.valueOf(key), o));
        }
      } else {
        operations.add(new Operation(String.valueOf(key), val));
      }
    }

  }

  static List<Map> captureErrors(List<Operation> ops){
    List<Map> errors = new ArrayList<>();
    for (SchemaManager.Operation op : ops) {
      if(op.hasError()) {
        errors.add(op.errorDetails());
      }
    }
    return errors;
  }
  public static final String ERR_MSGS = "errorMessages";


}

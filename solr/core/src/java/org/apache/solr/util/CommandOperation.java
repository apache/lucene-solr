package org.apache.solr.util;

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

import java.io.IOException;
import java.io.Reader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;

public  class CommandOperation {
  public final String name;
  private Object commandData;//this is most often a map
  private List<String> errors = new ArrayList<>();

  CommandOperation(String operationName, Object metaData) {
    commandData = metaData;
    this.name = operationName;
  }

  public String getStr(String key, String def){
    if(ROOT_OBJ.equals(key)){
      Object obj = getRootPrimitive();
      return obj == def ? null: String.valueOf(obj);
    }
    String s = (String) getMapVal(key);
    return s == null ? def : s;
  }

  public Map<String,Object> getDataMap(){
    if (commandData instanceof Map) {
      return (Map) commandData;
    }
    addError(MessageFormat.format("The command {0} should have the values as a json object {key:val} format", name));
    return Collections.EMPTY_MAP;
  }

  private Object getRootPrimitive(){
    if (commandData instanceof Map) {
      errors.add(MessageFormat.format("The value has to be a string for command : {1}",name));
      return null;
    }
    return commandData;

  }

  public Object getVal(String key){
    return getMapVal(key);
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
    if(val == null) {
      errors.add(MessageFormat.format(REQD, key));
    }
    return val;

  }
  static final String REQD = "''{0}'' is a required field";


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
    if(ROOT_OBJ.equals(key)){
      Object obj = getRootPrimitive();
      if(obj == null) {
        errors.add(MessageFormat.format(REQD,name));
      }
      return obj == null ? null: String.valueOf(obj);
    }

    String s = getStr(key,null);
    if(s==null) errors.add(MessageFormat.format(REQD, key));
    return s;
  }

  private Map errorDetails(){
    return makeMap(name, commandData, ERR_MSGS, errors);
  }

  public boolean hasError() {
    return !errors.isEmpty();
  }

  public void addError(String s) {
    if(errors.contains(s)) return;
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
  public static final String ERR_MSGS = "errorMessages";
  public static final String ROOT_OBJ = "";
  public static List<Map> captureErrors(List<CommandOperation> ops){
    List<Map> errors = new ArrayList<>();
    for (CommandOperation op : ops) {
      if(op.hasError()) {
        errors.add(op.errorDetails());
      }
    }
    return errors;
  }


  /**Parse the command operations into command objects
   */
  public static List<CommandOperation> parse(Reader rdr ) throws IOException {
    JSONParser parser = new JSONParser(rdr);

    ObjectBuilder ob = new ObjectBuilder(parser);

    if(parser.lastEvent() != JSONParser.OBJECT_START) {
      throw new RuntimeException("The JSON must be an Object of the form {\"command\": {...},...");
    }
    List<CommandOperation> operations = new ArrayList<>();
    for(;;) {
      int ev = parser.nextEvent();
      if (ev==JSONParser.OBJECT_END) return operations;
      Object key =  ob.getKey();
      ev = parser.nextEvent();
      Object val = ob.getVal();
      if (val instanceof List) {
        List list = (List) val;
        for (Object o : list) {
          operations.add(new CommandOperation(String.valueOf(key), o));
        }
      } else {
        operations.add(new CommandOperation(String.valueOf(key), val));
      }
    }

  }

}

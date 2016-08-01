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

package org.apache.solr.security;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.solr.util.CommandOperation;

import static org.apache.solr.common.util.Utils.getDeepCopy;
import static org.apache.solr.handler.admin.SecurityConfHandler.getListValue;
import static org.apache.solr.handler.admin.SecurityConfHandler.getMapValue;

enum AutorizationEditOperation {
  SET_USER_ROLE("set-user-role") {
    @Override
    public Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op) {
      Map<String, Object> roleMap = getMapValue(latestConf, "user-role");
      Map<String, Object> map = op.getDataMap();
      if (op.hasError()) return null;
      for (Map.Entry<String, Object> e : map.entrySet()) {
        if (e.getValue() == null) {
          roleMap.remove(e.getKey());
          continue;
        }
        if (e.getValue() instanceof String || e.getValue() instanceof List) {
          roleMap.put(e.getKey(), e.getValue());
        } else {
          op.addError("Unexpected value ");
          return null;
        }
      }
      return latestConf;
    }
  },
  SET_PERMISSION("set-permission") {
    @Override
    public Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op) {
      Integer index = op.getInt("index", null);
      Integer beforeIdx = op.getInt("before",null);
      Map<String, Object> dataMap = op.getDataMap();
      if (op.hasError()) return null;
      dataMap = getDeepCopy(dataMap, 3);
      dataMap.remove("before");
      if (beforeIdx != null && index != null) {
        op.addError("Cannot use 'index' and 'before together ");
        return null;
      }

      for (String key : dataMap.keySet()) {
        if (!Permission.knownKeys.contains(key)) op.addError("Unknown key, " + key);
      }
      try {
        Permission.load(dataMap);
      } catch (Exception e) {
        op.addError(e.getMessage());
        return null;
      }
      if(op.hasError()) return null;
      List<Map> permissions = getListValue(latestConf, "permissions");
      setIndex(permissions);
      List<Map> permissionsCopy = new ArrayList<>();
      boolean beforeSatisfied = beforeIdx == null;
      boolean indexSatisfied = index == null;
      for (int i = 0; i < permissions.size(); i++) {
        Map perm = permissions.get(i);
        Integer thisIdx = (int) perm.get("index");
        if (thisIdx.equals(beforeIdx)) {
          beforeSatisfied = true;
          permissionsCopy.add(dataMap);
          permissionsCopy.add(perm);
        } else if (thisIdx.equals(index)) {
          //overwriting an existing one
          indexSatisfied = true;
          permissionsCopy.add(dataMap);
        } else {
          permissionsCopy.add(perm);
        }
      }

      if (!beforeSatisfied) {
        op.addError("Invalid 'before' :" + beforeIdx);
        return null;
      }
      if (!indexSatisfied) {
        op.addError("Invalid 'index' :" + index);
        return null;
      }

      if (!permissionsCopy.contains(dataMap)) permissionsCopy.add(dataMap);
      latestConf.put("permissions", permissionsCopy);
      setIndex(permissionsCopy);
      return latestConf;
    }

  },
  UPDATE_PERMISSION("update-permission") {
    @Override
    public Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op) {
      Integer index = op.getInt("index");
      if (op.hasError()) return null;
      List<Map> permissions = (List<Map>) getListValue(latestConf, "permissions");
      setIndex(permissions);
      for (Map permission : permissions) {
        if (index.equals(permission.get("index"))) {
          LinkedHashMap copy = new LinkedHashMap<>(permission);
          copy.putAll(op.getDataMap());
          op.setCommandData(copy);
          return SET_PERMISSION.edit(latestConf, op);
        }
      }
      op.addError("No such permission " + name);
      return null;
    }
  },
  DELETE_PERMISSION("delete-permission") {
    @Override
    public Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op) {
      Integer id = op.getInt("");
      if(op.hasError()) return null;
      List<Map> p = getListValue(latestConf, "permissions");
      setIndex(p);
      List<Map> c = p.stream().filter(map -> !id.equals(map.get("index"))).collect(Collectors.toList());
      if(c.size() == p.size()){
        op.addError("No such index :"+ id);
        return null;
      }
      latestConf.put("permissions", c);
      return latestConf;
    }
  };

  public abstract Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op);

  public final String name;

  public String getOperationName() {
    return name;
  }

  AutorizationEditOperation(String s) {
    this.name = s;
  }

  public static AutorizationEditOperation get(String name) {
    for (AutorizationEditOperation o : values()) if (o.name.equals(name)) return o;
    return null;
  }

  static void setIndex(List<Map> permissionsCopy) {
    AtomicInteger counter = new AtomicInteger(0);
    permissionsCopy.stream().forEach(map -> map.put("index", counter.incrementAndGet()));
  }
}

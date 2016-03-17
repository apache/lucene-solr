package org.apache.solr.security;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;

import static java.util.Collections.singleton;
import static org.apache.solr.common.params.CommonParams.NAME;

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


class Permission {
  String name;
  Set<String> path, role, collections, method;
  Map<String, Object> params;
  PermissionNameProvider.Name wellknownName;

  private Permission() {
  }

  static Permission load(Map m) {
    Permission p = new Permission();
    String name = (String) m.get(NAME);
    if (!m.containsKey("role")) throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "role not specified");
    p.role = readValueAsSet(m, "role");
    if (PermissionNameProvider.Name.get(name)!= null) {
      p.wellknownName = PermissionNameProvider.Name.get(name);
      HashSet<String> disAllowed = new HashSet<>(knownKeys);
      disAllowed.remove("role");//these are the only
      disAllowed.remove(NAME);//allowed keys for well-known permissions
      disAllowed.remove("collection");//allowed keys for well-known permissions
      disAllowed.remove("index");
      for (String s : disAllowed) {
        if (m.containsKey(s))
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, s + " is not a valid key for the permission : " + name);
      }

    }
    p.name = name;
    p.path = readSetSmart(name, m, "path");
    p.collections = readSetSmart(name, m, "collection");
    p.method = readSetSmart(name, m, "method");
    p.params = (Map<String, Object>) m.get("params");
    return p;
  }

  /**
   * This checks for the defaults available other rules for the keys
   */
  private static Set<String> readSetSmart(String permissionName, Map m, String key) {
    if(PermissionNameProvider.values.containsKey(permissionName) && !m.containsKey(key) && "collection".equals(key)) {
      return PermissionNameProvider.Name.get(permissionName).collName;
    }
    Set<String> set = readValueAsSet(m, key);
    if ("method".equals(key)) {
      if (set != null) {
        for (String s : set) if (!HTTP_METHODS.contains(s)) return null;
      }
      return set;
    }
    return set == null ? singleton(null) : set;
  }
  /**
   * read a key value as a set. if the value is a single string ,
   * return a singleton set
   *
   * @param m   the map from which to lookup
   * @param key the key with which to do lookup
   */
  static Set<String> readValueAsSet(Map m, String key) {
    Set<String> result = new HashSet<>();
    Object val = m.get(key);
    if (val == null) {
      if("collection".equals(key)) {
        //for collection collection: null means a core admin/ collection admin request
        // otherwise it means a request where collection name is ignored
        return m.containsKey(key) ? singleton(null) : singleton("*");
      }
      return null;
    }
    if (val instanceof Collection) {
      Collection list = (Collection) val;
      for (Object o : list) result.add(String.valueOf(o));
    } else if (val instanceof String) {
      result.add((String) val);
    } else {
      throw new RuntimeException("Bad value for : " + key);
    }
    return result.isEmpty() ? null : Collections.unmodifiableSet(result);
  }


  static final Set<String> knownKeys = ImmutableSet.of("collection", "role", "params", "path", "method", NAME,"index");
  public static final Set<String> HTTP_METHODS = ImmutableSet.of("GET", "POST", "DELETE", "PUT", "HEAD");

}

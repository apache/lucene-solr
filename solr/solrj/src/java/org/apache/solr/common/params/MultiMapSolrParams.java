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
package org.apache.solr.common.params;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class MultiMapSolrParams extends SolrParams {
  protected final Map<String,String[]> map;

  public static void addParam(String name, String val, Map<String,String[]> map) {
    String[] arr = map.get(name);
    if (arr == null) {
      arr = new String[]{val};
    } else {
      String[] newarr = new String[arr.length+1];
      System.arraycopy(arr, 0, newarr, 0, arr.length);
      newarr[arr.length] = val;
      arr = newarr;
    }
    map.put(name, arr);
  }

  public static void addParam(String name, String[] vals, Map<String,String[]> map) {
    String[] arr = map.put(name, vals);
    if (arr == null) {
      return;
    }

    String[] newarr = new String[arr.length+vals.length];
    System.arraycopy(arr, 0, newarr, 0, arr.length);
    System.arraycopy(vals, 0, newarr, arr.length, vals.length);
    arr = newarr;

    map.put(name, arr);
  }


  public MultiMapSolrParams(Map<String,String[]> map) {
    this.map = map;
  }

  @Override
  public String get(String name) {
    String[] arr = map.get(name);
    return arr==null ? null : arr[0];
  }

  @Override
  public String[] getParams(String name) {
    return map.get(name);
  }

  @Override
  public Iterator<String> getParameterNamesIterator() {
    return map.keySet().iterator();
  }

  @Override
  public Iterator<Map.Entry<String, String[]>> iterator() {
    return map.entrySet().iterator();
  }

  public Map<String,String[]> getMap() { return map; }

  /** Returns a MultiMap view of the SolrParams as efficiently as possible.  The returned map may or may not be a backing implementation. */
  public static Map<String,String[]> asMultiMap(SolrParams params) {
    return asMultiMap(params, false);
  }

  /** Returns a MultiMap view of the SolrParams.  A new map will be created if newCopy==true */
  public static Map<String,String[]> asMultiMap(SolrParams params, boolean newCopy) {
    if (params instanceof MultiMapSolrParams) {
      Map<String,String[]> map = ((MultiMapSolrParams)params).getMap();
      if (newCopy) {
        return new HashMap<>(map);
      }
      return map;
    } else if (params instanceof ModifiableSolrParams) {
      Map<String,String[]> map = ((ModifiableSolrParams)params).getMap();
      if (newCopy) {
        return new HashMap<>(map);
      }
      return map;
    } else {
      Map<String,String[]> map = new HashMap<>();
      for (Map.Entry<String, String[]> pair : params) {
        map.put(pair.getKey(), pair.getValue());
      }
      return map;
    }
  }

}

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

import org.apache.solr.common.util.StrUtils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.io.IOException;

/**
 *
 */
public class MapSolrParams extends SolrParams {
  protected final Map<String,String> map;

  public MapSolrParams(Map<String,String> map) {
    this.map = map;
  }

  @Override
  public String get(String name) {
    Object  o = map.get(name);
    if(o == null) return null;
    if (o instanceof String) return  (String) o;
    if (o instanceof String[]) {
      String[] strings = (String[]) o;
      if(strings.length == 0) return null;
      return strings[0];
    }
    return String.valueOf(o);
  }

  @Override
  public String[] getParams(String name) {
    Object val = map.get(name);
    if (val instanceof String[]) return (String[]) val;
    return val==null ? null : new String[]{String.valueOf(val)};
  }

  @Override
  public Iterator<String> getParameterNamesIterator() {
    return map.keySet().iterator();
  }

  public Map<String,String> getMap() { return map; }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(128);
    try {
      boolean first=true;

      for (Map.Entry<String,String> entry : map.entrySet()) {
        String key = entry.getKey();
        Object val = entry.getValue();
        if (val instanceof String[]) {
          String[] strings = (String[]) val;
          val =  StrUtils.join(Arrays.asList(strings),',');
        }
        if (!first) sb.append('&');
        first=false;
        sb.append(key);
        sb.append('=');
        StrUtils.partialURLEncodeVal(sb, val==null ? "" : String.valueOf(val));
      }
    }
    catch (IOException e) {throw new RuntimeException(e);}  // can't happen

    return sb.toString();
  }
}

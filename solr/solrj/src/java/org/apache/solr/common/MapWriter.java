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

package org.apache.solr.common;


import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.Utils;

/**
 * Use this class to push all entries of a Map into an output.
 * This avoids creating map instances and is supposed to be memory efficient.
 * If the entries are primitives, unnecessary boxing is also avoided
 */
public interface MapWriter extends MapSerializable {

  default String jsonStr(){
    return Utils.toJSONString(this);
  }
  @Override
  default Map toMap(Map<String, Object> map) {
    try {
      writeMap(new EntryWriter() {
        @Override
        public EntryWriter put(String k, Object v) throws IOException {
          if (v instanceof MapWriter) v = ((MapWriter) v).toMap(new LinkedHashMap<>());
          if (v instanceof IteratorWriter) v = ((IteratorWriter) v).toList(new ArrayList<>());
          if (v instanceof Iterable) {
            List lst = new ArrayList();
            for (Object vv : (Iterable)v) {
              if (vv instanceof MapWriter) vv = ((MapWriter) vv).toMap(new LinkedHashMap<>());
              if (vv instanceof IteratorWriter) vv = ((IteratorWriter) vv).toList(new ArrayList<>());
              lst.add(vv);
            }
            v = lst;
          }
          if (v instanceof Map) {
            Map map = new LinkedHashMap();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>)v).entrySet()) {
              Object vv = entry.getValue();
              if (vv instanceof MapWriter) vv = ((MapWriter) vv).toMap(new LinkedHashMap<>());
              if (vv instanceof IteratorWriter) vv = ((IteratorWriter) vv).toList(new ArrayList<>());
              map.put(entry.getKey(), vv);
            }
            v = map;
          }
          map.put(k, v);
          return this;
        }

      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return map;
  }

  void writeMap(EntryWriter ew) throws IOException;

  /**
   * An interface to push one entry at a time to the output
   */
  interface EntryWriter {

    /**
     * Writes a key value into the map
     *
     * @param k The key
     * @param v The value can be any supported object
     */
    EntryWriter put(String k, Object v) throws IOException;
    default EntryWriter putNoEx(String k, Object v) {
      try {
        put(k,v);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    default EntryWriter putIfNotNull(String k, Object v) throws IOException {
      if(v != null) put(k,v);
      return this;
    }


    default EntryWriter put(String k, int v) throws IOException {
      put(k, (Integer) v);
      return this;
    }


    default EntryWriter put(String k, long v) throws IOException {
      put(k, (Long) v);
      return this;
    }


    default EntryWriter put(String k, float v) throws IOException {
      put(k, (Float) v);
      return this;
    }

    default EntryWriter put(String k, double v) throws IOException {
      put(k, (Double) v);
      return this;
    }

    default EntryWriter put(String k, boolean v) throws IOException {
      put(k, (Boolean) v);
      return this;
    }
  }
}

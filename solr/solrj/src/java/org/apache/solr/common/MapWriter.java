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
import java.util.Map;

/**
 * Use this class if the Map size is not known
 */
public interface MapWriter extends MapSerializable {

  @Override
  default Map toMap(Map<String, Object> map) {
    try {
      writeMap(new EntryWriter() {
        @Override
        public EntryWriter put(String k, Object v) throws IOException {
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

  interface EntryWriter {
    /**Writes a key value into the map
     * @param k The key
     * @param v The value can be any supported object
     */
    EntryWriter put(String k, Object v) throws IOException;

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

    default EntryWriter put(String k, double v) throws IOException{
      put(k, (Double) v);
      return this;
    }
    default EntryWriter put(String k, boolean v) throws IOException{
      put(k, (Boolean) v);
      return this;
    }
  }
}

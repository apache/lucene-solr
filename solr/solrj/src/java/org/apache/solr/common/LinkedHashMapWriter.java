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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class LinkedHashMapWriter<V> extends LinkedHashMap<String, V> implements MapWriter {

  public LinkedHashMapWriter(int initialCapacity) {
    super(initialCapacity);
  }

  public LinkedHashMapWriter() {
    super();
  }

  public LinkedHashMapWriter(Map<? extends String, ? extends V> m) {
    super(m);
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    forEach(ew.getBiConsumer());
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object _get(String path, Object def) {
    if (path.indexOf('/') == -1) return getOrDefault(path, (V) def);
    return MapWriter.super._get(path, def);
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object _get(List<String> path, Object def) {
    if (path.size() == 1) return getOrDefault(path.get(0), (V) def);
    return MapWriter.super._get(path, def);
  }

  @Override
  public String toString() {
    return jsonStr();
  }
}

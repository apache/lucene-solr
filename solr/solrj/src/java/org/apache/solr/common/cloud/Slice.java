package org.apache.solr.common.cloud;

/**
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

import org.apache.noggit.JSONWriter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

// immutable
public class Slice implements JSONWriter.Writable {
  private final Map<String,ZkNodeProps> shards;
  private final String name;

  public Slice(String name, Map<String,ZkNodeProps> shards) {
    this.shards = shards;
    this.name = name;
  }
  
  public Map<String,ZkNodeProps> getShards() {
    return Collections.unmodifiableMap(shards);
  }

  public Map<String,ZkNodeProps> getShardsCopy() {
    Map<String,ZkNodeProps> shards = new HashMap<String,ZkNodeProps>();
    for (Map.Entry<String,ZkNodeProps> entry : this.shards.entrySet()) {
      ZkNodeProps zkProps = new ZkNodeProps(entry.getValue());
      shards.put(entry.getKey(), zkProps);
    }
    return shards;
  }
  
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "Slice [shards=" + shards + ", name=" + name + "]";
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    jsonWriter.write(shards);
  }
}

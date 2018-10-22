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
package org.apache.solr.index;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.MergePolicy;
import org.apache.solr.util.SolrPluginUtils;

public class MergePolicyFactoryArgs {

  private final Map<String,Object> args;

  public MergePolicyFactoryArgs() {
    this.args = new HashMap<>();
  }

  public MergePolicyFactoryArgs(Iterable<Map.Entry<String,Object>> args) {
    this.args = new HashMap<>();
    for (final Map.Entry<String,Object> arg : args) {
      this.args.put(arg.getKey(), arg.getValue());
    }
  }

  public void add(String key, Object val) {
    args.put(key, val);
  }

  public Object remove(String key) {
    return args.remove(key);
  }

  public Object get(String key) {
    return args.get(key);
  }

  public Set<String> keys() {
    return args.keySet();
  }

  public void invokeSetters(MergePolicy policy) {
    SolrPluginUtils.invokeSetters(policy, args.entrySet());
  }

  @Override
  public String toString() {
    return args.toString();
  }

}

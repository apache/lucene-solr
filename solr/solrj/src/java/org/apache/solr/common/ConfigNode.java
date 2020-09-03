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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.solr.cluster.api.SimpleMap;

/**
 * A generic interface that represents a config file, mostly XML
 */
public interface ConfigNode {

  /**Name of the tag
   */
  String name();

  /**Text value of the node
   */
  String textValue();

  /**Attributes
   */
  SimpleMap<String> attributes();

  /** Child by name
   */
  ConfigNode child(String name);

  default ConfigNode child(Predicate<ConfigNode> test) {
    ConfigNode[] result = new ConfigNode[1];
    forEachChild(it -> {
      if (test.test(it)) {
        result[0] = it;
        return Boolean.FALSE;
      }
      return Boolean.TRUE;
    });
    return result[0];
  }

  default List<ConfigNode> children(Predicate<ConfigNode> test) {
    List<ConfigNode> result = new ArrayList<>();
    forEachChild(it -> {
      if (test.test(it)) result.add(it);
      return Boolean.TRUE;
    });
    return result;
  }

  List<ConfigNode> children(String name);

  void forEachChild(Function<ConfigNode, Boolean> fun);


}

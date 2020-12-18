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

package org.apache.solr.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.solr.cluster.api.SimpleMap;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.util.PropertiesUtil;

/**
 * ConfigNode impl that copies and maintains data internally from DOM
 */
public class DataConfigNode implements ConfigNode {
  final String name;
  final SimpleMap<String> attributes;
  private final Map<String, List<ConfigNode>> kids = new HashMap<>();
  private final String textData;

  public DataConfigNode(ConfigNode root) {
    name = root.name();
    attributes = wrap(root.attributes());
    textData = root.textValue();
    root.forEachChild(it -> {
      List<ConfigNode> nodes = kids.computeIfAbsent(it.name(),
          k -> new ArrayList<>());

     nodes.add(new DataConfigNode(it));
      return Boolean.TRUE;
    });

  }

  public String subtituteVal(String s) {
    Function<String, String> props = SUBSTITUTES.get();
    if (props == null) return s;
    return PropertiesUtil.substitute(s, props);
  }

  private SimpleMap<String> wrap(SimpleMap<String> delegate) {
    return new SimpleMap<>() {
          @Override
          public String get(String key) {
            return subtituteVal(delegate.get(key));
          }

          @Override
          public void forEachEntry(BiConsumer<String, ? super String> fun) {
            delegate.forEachEntry((k, v) -> fun.accept(k, subtituteVal(v)));
          }

          @Override
          public int size() {
            return delegate.size();
          }
        };
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String textValue() {
    return  subtituteVal(textData);
  }

  @Override
  public SimpleMap<String> attributes() {
    return attributes;
  }

  @Override
  public ConfigNode child(String name) {
    List<ConfigNode> val = kids.get(name);
    return val == null || val.isEmpty() ? null : val.get(0);
  }

  @Override
  public List<ConfigNode> children(String name) {
    return kids.getOrDefault(name, Collections.emptyList());
  }

  @Override
  public List<ConfigNode> children(Predicate<ConfigNode> test, Set<String> matchNames) {
    List<ConfigNode> result = new ArrayList<>();
    for (String s : matchNames) {
      List<ConfigNode> vals = kids.get(s);
      if (vals != null) {
        vals.forEach(it -> {
          if (test == null || test.test(it)) {
            result.add(it);
          }
        });
      }
    }
    return result;
  }

  @Override
  public void forEachChild(Function<ConfigNode, Boolean> fun) {
    kids.forEach((s, configNodes) -> {
      if (configNodes != null) {
        configNodes.forEach(fun::apply);
      }
    });
  }
}

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
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.solr.cluster.api.SimpleMap;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.util.WrappedSimpleMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Read using DOM
 */
public class DOMConfigNode implements ConfigNode {

  private final Node node;
  SimpleMap<String> attrs;


  @Override
  public String name() {
    return node.getNodeName();
  }

  @Override
  public String txt() {
    return DOMUtil.getText(node);
  }

  public DOMConfigNode(Node node) {
    this.node = node;
  }

  @Override
  public SimpleMap<String> attributes() {
    if (attrs != null) return attrs;
    Map<String, String> attrs = DOMUtil.toMap(node.getAttributes());
    return this.attrs = attrs.size() == 0 ? EMPTY : new WrappedSimpleMap<>(attrs);
  }

  @Override
  public ConfigNode child(String name) {
    Node n  =  DOMUtil.getChild(node, name);
    return n == null? null: new DOMConfigNode(n);
  }

  @Override
  public List<ConfigNode> getAll(String name) {
    List<ConfigNode> result = new ArrayList<>();
    forEachChild(it -> {
      if (name.equals(it.name())) {
        result.add(it);
      }
      return Boolean.TRUE;
    });
    return result;
  }

  @Override
  public void forEachChild(Function<ConfigNode, Boolean> fun) {
    NodeList nlst = node.getChildNodes();
    for (int i = 0; i < nlst.getLength(); i++) {
      Node item = nlst.item(i);
      if(item.getNodeType() != Node.ELEMENT_NODE) continue;
      Boolean toContinue = fun.apply(new DOMConfigNode(item));
      if (Boolean.FALSE == toContinue) break;
    }
  }
  private static final SimpleMap<String> EMPTY = new WrappedSimpleMap<>(Collections.emptyMap());

}

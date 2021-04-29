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
package org.apache.solr.core;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.solr.cluster.api.SimpleMap;
import org.apache.solr.common.ConfigNode;

/**A config node impl which has an overlay
 *
 */
class OverlaidConfigNode implements ConfigNode {

  private final ConfigOverlay overlay;
  private final String _name;
  private final ConfigNode delegate;
  private final OverlaidConfigNode parent;

  OverlaidConfigNode(ConfigOverlay overlay, String name, OverlaidConfigNode parent, ConfigNode delegate) {
    this.overlay = overlay;
    this._name = name;
    this.delegate = delegate;
    this.parent = parent;
  }

  private List<String> path(List<String> path) {
    if(path== null) path = new ArrayList<>(5);
    try {
      if (parent != null) return parent.path(path);
    } finally {
      path.add(_name);
    }
    return path;
  }

  @Override
  public ConfigNode get(String name) {
    return wrap(delegate.get(name), name);
  }

  private ConfigNode wrap(ConfigNode n, String name) {
    return new OverlaidConfigNode(overlay, name,this, n);
  }

  @Override
  public ConfigNode get(String name, Predicate<ConfigNode> test) {
    return wrap(delegate.get(name, test), name);
  }

  @Override
  public String txt() {
    return overlayText(delegate.txt(), null);
  }

  @Override
  public ConfigNode get(String name, int idx) {
    return wrap(delegate.get(name, idx), name);
  }

  @Override
  public String name() {
    return delegate.name();
  }
  @Override
  public SimpleMap<String> attributes() {
    return delegate.attributes();
  }

  @Override
  public boolean exists() {
    return delegate.exists();
  }

  @Override
  public String attr(String name) {
    return overlayText(delegate.attr(name),name);
  }

  private String overlayText(String def, String appendToPath) {
    List<String> path = path(null);
    if(appendToPath !=null) path.add(appendToPath);
    Object val = overlay.getXPathProperty(path);
    return val ==null? def: val.toString();
  }

  @Override
  public void forEachChild(Function<ConfigNode, Boolean> fun) {
    delegate.forEachChild(fun);
  }
}

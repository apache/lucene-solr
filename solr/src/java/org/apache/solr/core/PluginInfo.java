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
package org.apache.solr.core;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.DOMUtil;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.*;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * An Object which represents a Plugin of any type 
 *
 */
public class PluginInfo {
  public final String name, className, type;
  public final NamedList initArgs;
  public final Map<String, String> attributes;
  public final List<PluginInfo> children;

  public PluginInfo(String type, Map<String, String> attrs ,NamedList initArgs, List<PluginInfo> children) {
    this.type = type;
    this.name = attrs.get("name");
    this.className = attrs.get("class");
    this.initArgs = initArgs;
    attributes = unmodifiableMap(attrs);
    this.children = children == null ? Collections.<PluginInfo>emptyList(): unmodifiableList(children);
  }


  public PluginInfo(Node node, String err, boolean requireName, boolean requireClass) {
    type = node.getNodeName();
    name = DOMUtil.getAttr(node, "name", requireName ? err : null);
    className = DOMUtil.getAttr(node, "class", requireClass ? err : null);
    initArgs = DOMUtil.childNodesToNamedList(node);
    attributes = unmodifiableMap(DOMUtil.toMap(node.getAttributes()));
    children = loadSubPlugins(node);
  }

  private List<PluginInfo> loadSubPlugins(Node node) {
    List<PluginInfo> children = new ArrayList<PluginInfo>();
    //if there is another sub tag with a non namedlist tag that has to be another plugin
    NodeList nlst = node.getChildNodes();
    for (int i = 0; i < nlst.getLength(); i++) {
      Node nd = nlst.item(i);
      if (nd.getNodeType() != Node.ELEMENT_NODE) continue;
      if (NL_TAGS.contains(nd.getNodeName())) continue;
      PluginInfo pluginInfo = new PluginInfo(nd, null, false, false);
      if (pluginInfo.isEnabled()) children.add(pluginInfo);
    }
    return children.isEmpty() ? Collections.<PluginInfo>emptyList() : unmodifiableList(children);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("{");
    if (type != null) sb.append("type = " + type + ",");
    if (name != null) sb.append("name = " + name + ",");
    if (className != null) sb.append("class = " + className + ",");
    if (initArgs.size() > 0) sb.append("args = " + initArgs);
    sb.append("}");
    return sb.toString();
  }

  public boolean isEnabled(){
    String enable = attributes.get("enable");
    return enable == null || Boolean.parseBoolean(enable); 
  }

  public boolean isDefault() {
    return Boolean.parseBoolean(attributes.get("default"));
  }

  public PluginInfo getChild(String type){
    List<PluginInfo> l = getChildren(type);
    return  l.isEmpty() ? null:l.get(0);
  }

  /**Filter children by type
   * @param type The type name. must not be null
   * @return The mathcing children
   */
  public List<PluginInfo> getChildren(String type){
    if(children.isEmpty()) return children;
    List<PluginInfo> result = new ArrayList<PluginInfo>();
    for (PluginInfo child : children) if(type.equals(child.type)) result.add(child);
    return result;
  }
  public static final PluginInfo EMPTY_INFO = new PluginInfo("",Collections.<String,String>emptyMap(), new NamedList(),Collections.<PluginInfo>emptyList());
  private static final HashSet<String> NL_TAGS = new HashSet<String>(Arrays.asList("lst","str","int","bool","arr","float","double"));
}

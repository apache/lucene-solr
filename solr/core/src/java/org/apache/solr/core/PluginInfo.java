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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.util.DOMUtil;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static org.apache.solr.common.params.CoreAdminParams.NAME;
import static org.apache.solr.schema.FieldType.CLASS_NAME;

/**
 * An Object which represents a Plugin of any type 
 *
 */
public class PluginInfo implements MapSerializable {
  public final String name, className, type, pkgName;
  public final NamedList initArgs;
  public final Map<String, String> attributes;
  public final List<PluginInfo> children;
  private boolean isFromSolrConfig;



  public PluginInfo(String type, Map<String, String> attrs, NamedList initArgs, List<PluginInfo> children) {
    this.type = type;
    this.name = attrs.get(NAME);
    Pair<String, String> parsed = parseClassName(attrs.get(CLASS_NAME));
    this.className = parsed.second();
    this.pkgName = parsed.first();
    this.initArgs = initArgs;
    attributes = unmodifiableMap(attrs);
    this.children = children == null ? Collections.<PluginInfo>emptyList(): unmodifiableList(children);
    isFromSolrConfig = false;
  }

  /** class names can be prefixed with package name e.g: my_package:my.pkg.Class
   * This checks if it is a package name prefixed classname.
   * the return value has first = package name & second = class name
   */
  static Pair<String,String > parseClassName(String name) {
    String pkgName = null;
    String className = name;
    if (name != null) {
      int colonIdx = name.indexOf(':');
      if (colonIdx > -1) {
        pkgName = name.substring(0, colonIdx);
        className = name.substring(colonIdx + 1);
      }
    }
    return new Pair<>(pkgName, className);

  }


  public PluginInfo(Node node, String err, boolean requireName, boolean requireClass) {
    type = node.getNodeName();
    name = DOMUtil.getAttr(node, NAME, requireName ? err : null);
    Pair<String, String> parsed = parseClassName(DOMUtil.getAttr(node, CLASS_NAME, requireClass ? err : null));
    className = parsed.second();
    pkgName = parsed.first();
    initArgs = DOMUtil.childNodesToNamedList(node);
    attributes = unmodifiableMap(DOMUtil.toMap(node.getAttributes()));
    children = loadSubPlugins(node);
    isFromSolrConfig = true;
  }

  public PluginInfo(String type, Map<String,Object> map) {
    LinkedHashMap m = new LinkedHashMap<>(map);
    initArgs = new NamedList();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (NAME.equals(entry.getKey()) || CLASS_NAME.equals(entry.getKey())) continue;
      Object value = entry.getValue();
      if (value instanceof List) {
        List list = (List) value;
        if (!list.isEmpty() && list.get(0) instanceof Map) {//this is a subcomponent
          for (Object o : list) {
            if (o instanceof Map) o = new NamedList<>((Map) o);
            initArgs.add(entry.getKey(), o);
          }
        } else {
          initArgs.add(entry.getKey(), value);
        }
      } else {
        if (value instanceof Map) value = new NamedList((Map) value);
        initArgs.add(entry.getKey(), value);
      }
    }
    this.type = type;
    this.name = (String) m.get(NAME);
    Pair<String, String> parsed = parseClassName((String) m.get(CLASS_NAME));
    this.className = parsed.second();
    this.pkgName = parsed.first();
    attributes = unmodifiableMap(m);
    this.children =  Collections.<PluginInfo>emptyList();
    isFromSolrConfig = true;
  }

  private List<PluginInfo> loadSubPlugins(Node node) {
    List<PluginInfo> children = new ArrayList<>();
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
    if (type != null) sb.append("type = ").append(type).append(',');
    if (name != null) sb.append("name = ").append(name).append(',');
    if (className != null) sb.append("class = ").append(className).append(',');
    if (attributes != null && attributes.size() > 0) sb.append("attributes = ").append(attributes).append(',');
    if (initArgs != null && initArgs.size() > 0) sb.append("args = ").append(initArgs);
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

  public Map<String, Object> toMap(Map<String, Object> map) {
    map.putAll(attributes);
    Map m = map;
    if(initArgs!=null ) m.putAll(initArgs.asMap(3));
    if(children != null){
      for (PluginInfo child : children) {
        Object old = m.get(child.name);
        if(old == null){
          m.put(child.name, child.toMap(new LinkedHashMap<>()));
        } else if (old instanceof List) {
          List list = (List) old;
          list.add(child.toMap(new LinkedHashMap<>()));
        }  else {
          ArrayList l = new ArrayList();
          l.add(old);
          l.add(child.toMap(new LinkedHashMap<>()));
          m.put(child.name,l);
        }
      }

    }
    return m;
  }

  /**Filter children by type
   * @param type The type name. must not be null
   * @return The mathcing children
   */
  public List<PluginInfo> getChildren(String type){
    if(children.isEmpty()) return children;
    List<PluginInfo> result = new ArrayList<>();
    for (PluginInfo child : children) if(type.equals(child.type)) result.add(child);
    return result;
  }
  public static final PluginInfo EMPTY_INFO = new PluginInfo("",Collections.<String,String>emptyMap(), new NamedList(),Collections.<PluginInfo>emptyList());

  private static final HashSet<String> NL_TAGS = new HashSet<>
    (asList("lst", "arr",
        "bool",
        "str",
        "int", "long",
        "float", "double"));
  public static final String DEFAULTS = "defaults";
  public static final String APPENDS = "appends";
  public static final String INVARIANTS = "invariants";

  public boolean isFromSolrConfig(){
    return isFromSolrConfig;

  }
  public PluginInfo copy() {
    PluginInfo result = new PluginInfo(type, attributes,
        initArgs != null ? initArgs.clone() : null, children);
    result.isFromSolrConfig = isFromSolrConfig;
    return result;
  }
}

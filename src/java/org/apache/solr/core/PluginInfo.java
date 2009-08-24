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
import org.w3c.dom.NamedNodeMap;

import java.util.*;

/**
 * An Object which represents a Plugin of any type 
 * @version $Id$
 */
public class PluginInfo {
  public final String startup, name, className, type;
  public final boolean isDefault;
  public final NamedList initArgs;
  public final Map<String, String> attributes;

  public PluginInfo(String type, String startup, String name, String className,
                    boolean isdefault, NamedList initArgs, Map<String, String> otherAttrs) {
    this.type = type;
    this.startup = startup;
    this.name = name;
    this.className = className;
    this.isDefault = isdefault;
    this.initArgs = initArgs;
    attributes = otherAttrs == null ? Collections.<String, String>emptyMap() : otherAttrs;
  }


  public PluginInfo(Node node, String err, boolean requireName) {
    type = node.getNodeName();
    name = DOMUtil.getAttr(node, "name", requireName ? err : null);
    className = DOMUtil.getAttr(node, "class", err);
    isDefault = Boolean.parseBoolean(DOMUtil.getAttr(node, "default", null));
    startup = DOMUtil.getAttr(node, "startup", null);
    initArgs = DOMUtil.childNodesToNamedList(node);
    Map<String, String> m = new HashMap<String, String>();
    NamedNodeMap nnm = node.getAttributes();
    for (int i = 0; i < nnm.getLength(); i++) {
      String name = nnm.item(i).getNodeName();
      m.put(name, nnm.item(i).getNodeValue());
    }
    attributes = Collections.unmodifiableMap(m);

  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("{");
    if (name != null) sb.append("name = " + name + ",");
    if (className != null) sb.append("class = " + className + ",");
    if (isDefault) sb.append("default = " + isDefault + ",");
    if (startup != null) sb.append("startup = " + startup + ",");
    if (initArgs.size() > 0) sb.append("args = " + initArgs);
    sb.append("}");
    return sb.toString();
  }

}

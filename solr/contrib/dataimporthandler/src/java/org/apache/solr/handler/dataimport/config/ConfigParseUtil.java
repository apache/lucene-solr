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
package org.apache.solr.handler.dataimport.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ConfigParseUtil {
  
  public static String getStringAttribute(Element e, String name, String def) {
    String r = e.getAttribute(name);
    if (r == null || "".equals(r.trim())) r = def;
    return r;
  }
  
  public static HashMap<String,String> getAllAttributes(Element e) {
    HashMap<String,String> m = new HashMap<>();
    NamedNodeMap nnm = e.getAttributes();
    for (int i = 0; i < nnm.getLength(); i++) {
      m.put(nnm.item(i).getNodeName(), nnm.item(i).getNodeValue());
    }
    return m;
  }
  
  public static String getText(Node elem, StringBuilder buffer) {
    if (elem.getNodeType() != Node.CDATA_SECTION_NODE) {
      NodeList childs = elem.getChildNodes();
      for (int i = 0; i < childs.getLength(); i++) {
        Node child = childs.item(i);
        short childType = child.getNodeType();
        if (childType != Node.COMMENT_NODE
            && childType != Node.PROCESSING_INSTRUCTION_NODE) {
          getText(child, buffer);
        }
      }
    } else {
      buffer.append(elem.getNodeValue());
    }
    
    return buffer.toString();
  }
  
  public static List<Element> getChildNodes(Element e, String byName) {
    List<Element> result = new ArrayList<>();
    NodeList l = e.getChildNodes();
    for (int i = 0; i < l.getLength(); i++) {
      if (e.equals(l.item(i).getParentNode())
          && byName.equals(l.item(i).getNodeName())) result.add((Element) l
          .item(i));
    }
    return result;
  }
}

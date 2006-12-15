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

package org.apache.solr.util;

import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * @author yonik
 * @version $Id$
 */
public class DOMUtil {

  public static Map<String,String> toMap(NamedNodeMap attrs) {
    return toMapExcept(attrs);
  }

  public static Map<String,String> toMapExcept(NamedNodeMap attrs, String... exclusions) {
    Map<String,String> args = new HashMap<String,String>();
    outer: for (int j=0; j<attrs.getLength(); j++) {
      Node attr = attrs.item(j);
      String attrName = attr.getNodeName();
      for (String ex : exclusions)
        if (ex.equals(attrName)) continue outer;
      String val = attr.getNodeValue();
      args.put(attrName, val);
    }
    return args;
  }

  public static Node getChild(Node node, String name) {
    if (!node.hasChildNodes()) return null;
    NodeList lst = node.getChildNodes();
    if (lst == null) return null;
    for (int i=0; i<lst.getLength(); i++) {
      Node child = lst.item(i);
      if (name.equals(child.getNodeName())) return child;
    }
    return null;
  }

  public static String getAttr(NamedNodeMap attrs, String name) {
    return getAttr(attrs,name,null);
  }

  public static String getAttr(Node nd, String name) {
    return getAttr(nd.getAttributes(), name);
  }

  public static String getAttr(NamedNodeMap attrs, String name, String missing_err) {
    Node attr = attrs==null? null : attrs.getNamedItem(name);
    if (attr==null) {
      if (missing_err==null) return null;
      throw new RuntimeException(missing_err + ": missing mandatory attribute '" + name + "'");
    }
    String val = attr.getNodeValue();
    return val;
  }

  public static String getAttr(Node node, String name, String missing_err) {
    return getAttr(node.getAttributes(), name, missing_err);
  }

  //////////////////////////////////////////////////////////
  // Routines to parse XML in the syntax of the Solr query
  // response schema.
  // Should these be moved to Config?  Should all of these things?
  //////////////////////////////////////////////////////////
  public static NamedList childNodesToNamedList(Node nd) {
    return nodesToNamedList(nd.getChildNodes());
  }

  public static List childNodesToList(Node nd) {
    return nodesToList(nd.getChildNodes());
  }

  public static NamedList nodesToNamedList(NodeList nlst) {
    NamedList clst = new NamedList();
    for (int i=0; i<nlst.getLength(); i++) {
      addToNamedList(nlst.item(i), clst, null);
    }
    return clst;
  }

  public static List nodesToList(NodeList nlst) {
    List lst = new ArrayList();
    for (int i=0; i<nlst.getLength(); i++) {
      addToNamedList(nlst.item(i), null, lst);
    }
    return lst;
  }


  public static void addToNamedList(Node nd, NamedList nlst, List arr) {
    // Nodes often include whitespace, etc... so just return if this
    // is not an Element.
    if (nd.getNodeType() != Node.ELEMENT_NODE) return;

    String type = nd.getNodeName();

    String name = null;
    if (nd.hasAttributes()) {
      NamedNodeMap attrs = nd.getAttributes();
      Node nameNd = attrs.getNamedItem("name");
      if (nameNd != null) name=nameNd.getNodeValue();
    }

    Object val=null;

    if ("str".equals(type)) {
      val = getText(nd);
    } else if ("int".equals(type)) {
      val = Integer.valueOf(getText(nd));
    } else if ("long".equals(type)) {
      val = Long.valueOf(getText(nd));
    } else if ("float".equals(type)) {
      val = Float.valueOf(getText(nd));
    } else if ("double".equals(type)) {
      val = Double.valueOf(getText(nd));
    } else if ("bool".equals(type)) {
      val = Boolean.valueOf(getText(nd));
    } else if ("lst".equals(type)) {
      val = childNodesToNamedList(nd);
    } else if ("arr".equals(type)) {
      val = childNodesToList(nd);
    }

    if (nlst != null) nlst.add(name,val);
    if (arr != null) arr.add(val);
  }

  /**
   * Drop in replacement for Node.getTextContent().
   *
   * <p>
   * This method is provided to support the same functionality as
   * Node.getTextContent() but in a way that is DOM Level 2 compatible.
   * </p>
   *
   * @see <a href="http://www.w3.org/TR/DOM-Level-3-Core/core.html#Node3-textContent">DOM Object Model Core</a>
   */
  public static String getText(Node nd) {

    short type = nd.getNodeType();
    
    // for most node types, we can defer to the recursive helper method,
    // but when asked for the text of these types, we must return null
    // (Not the empty string)
    switch (type) {
      
    case Node.DOCUMENT_NODE: /* fall through */
    case Node.DOCUMENT_TYPE_NODE: /* fall through */
    case Node.NOTATION_NODE: /* fall through */
      return null;
    }

    StringBuilder sb = new StringBuilder();
    getText(nd, sb);
    return sb.toString();
  }

  /** @see #getText(Node) */
  private static void getText(Node nd, StringBuilder buf) {
    
    short type = nd.getNodeType();

    switch (type) {
      
    case Node.ELEMENT_NODE: /* fall through */
    case Node.ATTRIBUTE_NODE: /* fall through */
    case Node.ENTITY_NODE: /* fall through */
    case Node.ENTITY_REFERENCE_NODE: /* fall through */
    case Node.DOCUMENT_FRAGMENT_NODE: 
      NodeList childs = nd.getChildNodes();
      for (int i = 0; i < childs.getLength(); i++) {
        Node child = childs.item(i);
        short childType = child.getNodeType();
        if (childType != Node.COMMENT_NODE &&
            childType != Node.PROCESSING_INSTRUCTION_NODE) {
          getText(child, buf);
        }
      }
      break;
      
    case Node.TEXT_NODE: /* fall through */
    case Node.CDATA_SECTION_NODE: /* fall through */
    case Node.COMMENT_NODE: /* fall through */
    case Node.PROCESSING_INSTRUCTION_NODE: /* fall through */
      buf.append(nd.getNodeValue());
      break;

    case Node.DOCUMENT_NODE: /* fall through */
    case Node.DOCUMENT_TYPE_NODE: /* fall through */
    case Node.NOTATION_NODE: /* fall through */
    default:
      /* :NOOP: */

    }
  }
}

package org.apache.solr.util;

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

import java.util.*;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 */
public class DOMUtil {

  public static final String XML_RESERVED_PREFIX = "xml";

  public static Map<String,String> toMap(NamedNodeMap attrs) {
    return toMapExcept(attrs);
  }

  public static Map<String,String> toMapExcept(NamedNodeMap attrs, String... exclusions) {
    Map<String,String> args = new HashMap<String,String>();
    outer: for (int j=0; j<attrs.getLength(); j++) {
      Node attr = attrs.item(j);

      // automatically exclude things in the xml namespace, ie: xml:base
      if (XML_RESERVED_PREFIX.equals(attr.getPrefix())) continue outer;

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
  public static NamedList<Object> childNodesToNamedList(Node nd) {
    return nodesToNamedList(nd.getChildNodes());
  }

  public static List childNodesToList(Node nd) {
    return nodesToList(nd.getChildNodes());
  }

  public static NamedList<Object> nodesToNamedList(NodeList nlst) {
    NamedList<Object> clst = new NamedList<Object>();
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

  /**
   * Examines a Node from the DOM representation of a NamedList and adds the
   * contents of that node to both the specified NamedList and List passed
   * as arguments.
   *
   * @param nd The Node whose type will be used to determine how to parse the
   *           text content.  If there is a 'name' attribute it will be used
   *           when adding to the NamedList
   * @param nlst A NamedList to add the item to with name if application.
   *             If this param is null it will be ignored.
   * @param arr A List to add the item to.
   *             If this param is null it will be ignored.
   */
  @SuppressWarnings("unchecked")
  public static void addToNamedList(Node nd, NamedList nlst, List arr) {
    // Nodes often include whitespace, etc... so just return if this
    // is not an Element.
    if (nd.getNodeType() != Node.ELEMENT_NODE) return;

    final String type = nd.getNodeName();

    final String name = getAttr(nd, "name");

    Object val=null;

    if ("lst".equals(type)) {
      val = childNodesToNamedList(nd);
    } else if ("arr".equals(type)) {
      val = childNodesToList(nd);
    } else {
      final String textValue = getText(nd);
      try {
        if ("str".equals(type)) {
          val = textValue;
        } else if ("int".equals(type)) {
          val = Integer.valueOf(textValue);
        } else if ("long".equals(type)) {
          val = Long.valueOf(textValue);
        } else if ("float".equals(type)) {
          val = Float.valueOf(textValue);
        } else if ("double".equals(type)) {
          val = Double.valueOf(textValue);
        } else if ("bool".equals(type)) {
          val = StrUtils.parseBool(textValue);
        }
        // :NOTE: Unexpected Node names are ignored
        // :TODO: should we generate an error here?
      } catch (NumberFormatException nfe) {
        throw new SolrException
          (SolrException.ErrorCode.SERVER_ERROR,
           "Value " + (null != name ? ("of '" +name+ "' ") : "") +
           "can not be parsed as '" +type+ "': \"" + textValue + "\"",
           nfe);
      }
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

    case Node.ATTRIBUTE_NODE: /* fall through */
      /* Putting Attribute nodes in this section does not exactly
         match the definition of how textContent should behave
         according to the DOM Level-3 Core documentation - which
         specifies that the Attr's children should have their
         textContent concated (Attr's can have a single child which
         is either Text node or an EntityReference).  In practice,
         DOM implementations do not seem to use child nodes of
         Attributes, storing the "text" directly as the nodeValue.
         Fortunately, the DOM Spec indicates that when Attr.nodeValue
         is read, it should return the nodeValue from the child Node,
         so this approach should work both for strict implementations,
         and implementations actually encountered.
      */
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

  /**
   * Replaces ${system.property[:default value]} references in all attributes
   * and text nodes of supplied node.  If the system property is not defined and no
   * default value is provided, a runtime exception is thrown.
   *
   * @param node DOM node to walk for substitutions
   */
  public static void substituteSystemProperties(Node node) {
    substituteProperties(node, null);
  }

  /**
   * Replaces ${property[:default value]} references in all attributes
   * and text nodes of supplied node.  If the property is not defined neither in the
   * given Properties instance nor in System.getProperty and no
   * default value is provided, a runtime exception is thrown.
   *
   * @param node DOM node to walk for substitutions
   * @param properties the Properties instance from which a value can be looked up
   */
  public static void substituteProperties(Node node, Properties properties) {
    // loop through child nodes
    Node child;
    Node next = node.getFirstChild();
    while ((child = next) != null) {

      // set next before we change anything
      next = child.getNextSibling();

      // handle child by node type
      if (child.getNodeType() == Node.TEXT_NODE) {
        child.setNodeValue(PropertiesUtil.substituteProperty(child.getNodeValue(), properties));
      } else if (child.getNodeType() == Node.ELEMENT_NODE) {
        // handle child elements with recursive call
        NamedNodeMap attributes = child.getAttributes();
        for (int i = 0; i < attributes.getLength(); i++) {
          Node attribute = attributes.item(i);
          attribute.setNodeValue(PropertiesUtil.substituteProperty(attribute.getNodeValue(), properties));
        }
        substituteProperties(child, properties);
      }
    }
  }


}

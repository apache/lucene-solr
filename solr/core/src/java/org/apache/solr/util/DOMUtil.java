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

import net.sf.saxon.om.AttributeInfo;
import net.sf.saxon.om.AttributeMap;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.type.Type;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;

import static org.apache.solr.common.params.CommonParams.NAME;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.LongAdder;

/**
 *
 */
public class DOMUtil {

  public static final String XML_RESERVED_PREFIX = "xml";

  public static Map<String,String> toMap(AttributeMap attrs) {
    return toMapExcept(attrs);
  }

  public static Map<String,String> toMapExcept(AttributeMap attrMap, String... exclusions) {
    Map<String,String> args = new HashMap<>(attrMap.size() - exclusions.length);
    List<AttributeInfo> attrs = attrMap.asList();
    outer: for (int j=0; j<attrs.size(); j++) {
      AttributeInfo attr = attrs.get(j);

      String attrName = attr.getNodeName().getDisplayName();
      for (String ex : exclusions)
        if (ex.equals(attrName)) continue outer;
      String val = attr.getValue();
      args.put(attrName, val);
    }
    return args;
  }

   public static String getAttr(NodeInfo nd, String name) {
      return getAttr(nd, name, null);
    }

  public static String getAttr(NodeInfo nd, String name, String missing_err) {
    String attr = nd.getAttributeValue("", name);
    if (attr==null) {
      if (missing_err==null) return null;
      throw new RuntimeException(missing_err + ": missing mandatory attribute '" + name + "'");
    }
    return attr;
  }

  public static String getAttrOrDefault(NodeInfo nd, String name, String def) {
    String attr = nd.getAttributeValue("", name);
    return attr == null ? def : attr;
  }

  public static NamedList<Object> childNodesToNamedList(NodeInfo nd) {
    return nodesToNamedList(nd.children());
  }

  public static List childNodesToList(NodeInfo nd) {
    return nodesToList(nd.children());
  }

  public static NamedList<Object> nodesToNamedList(Iterable<? extends NodeInfo> nlst) {
    NamedList<Object> clst = new NamedList<>();
    nlst.forEach(nodeInfo -> {
      addToNamedList(nodeInfo, clst, null);
    });

    return clst;
  }

  public static List nodesToList(ArrayList<NodeInfo> nlst) {
    List lst = new ArrayList(nlst.size());
    for (int i=0; i<nlst.size(); i++) {
      addToNamedList(nlst.get(i), null, lst);
    }
    return lst;
  }

  public static List nodesToList(Iterable<? extends NodeInfo> nlst) {
    List<NodeInfo> lst = new ArrayList();
    nlst.forEach(o -> {
      addToNamedList(o, null, lst);
    });
    return lst;
  }

  public static void addToNamedList(NodeInfo nd, NamedList nlst, List arr) {
    // Nodes often include whitespace, etc... so just return if this
    // is not an Element.

    if (nd.getNodeKind() != Type.ELEMENT) return;

    final String type = nd.getDisplayName();

    final String name = getAttr(nd, NAME);

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

    if (nlst != null && name != null) nlst.add(name, val);
    if (arr != null) arr.add(val);
  }

  private static String getAttribs(NodeInfo nd) {
    StringBuilder sb = new StringBuilder();

    nd.attributes().forEach(attributeInfo -> {
      sb.append(attributeInfo.getNodeName() + ":" + attributeInfo.getValue());
    });
    return sb.toString();
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
  public static String getText(NodeInfo nd) {

    int type = nd.getNodeKind();

    // for most node types, we can defer to the recursive helper method,
    // but when asked for the text of these types, we must return null
    // (Not the empty string)
    switch (type) {

      case Type.DOCUMENT: /* fall through */
      return null;
    }

    StringBuilder sb = new StringBuilder();
    getText(nd, sb);
    return sb.toString();
  }

  /** @see #getText(NodeInfo) */
  private static void getText(NodeInfo nd, StringBuilder buf) {

    int type = nd.getNodeKind();

    switch (type) {

      case Type.ELEMENT: /* fall through */
      case Type.NODE: /* fall through */
      Iterable<? extends NodeInfo> childs = nd.children();
      childs.forEach(child -> {
        int childType = child.getNodeKind();
        if (childType != Type.COMMENT &&
            childType != Type.PROCESSING_INSTRUCTION) {
          getText(child, buf);
        }
      });
      break;

      case Type.ATTRIBUTE: /* fall through */
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
      case Type.TEXT: /* fall through */
      case Type.COMMENT: /* fall through */
      case Type.PROCESSING_INSTRUCTION: /* fall through */
          buf.append(nd.getStringValue());
      break;

      case Type.DOCUMENT: /* fall through */
    default:
      /* :NOOP: */

    }
  }

  public static String substituteProperty(String value, Properties coreProperties) {
    if (value == null || value.indexOf('$') == -1) {
      return value;
    }

    List<String> fragments = new ArrayList<>();
    List<String> propertyRefs = new ArrayList<>();
    parsePropertyString(value, fragments, propertyRefs);

    StringBuilder sb = new StringBuilder();
    Iterator<String> i = fragments.iterator();
    Iterator<String> j = propertyRefs.iterator();

    while (i.hasNext()) {
      String fragment = i.next();
      if (fragment == null) {
        String propertyName = j.next();
        String defaultValue = null;
        int colon_index = propertyName.indexOf(':');
        if (colon_index > -1) {
          defaultValue = propertyName.substring(colon_index + 1);
          propertyName = propertyName.substring(0,colon_index);
        }
        if (coreProperties != null) {
          fragment = coreProperties.getProperty(propertyName);
        }
        if (fragment == null) {
          fragment = System.getProperty(propertyName, defaultValue);
        }
        if (fragment == null) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "No system property or default value specified for " + propertyName + " value:" + value);
        }
      }
      sb.append(fragment);
    }
    return sb.toString();
  }
  
  /*
   * This method borrowed from Ant's PropertyHelper.parsePropertyStringDefault:
   *   http://svn.apache.org/repos/asf/ant/core/trunk/src/main/org/apache/tools/ant/PropertyHelper.java
   */
  private static void parsePropertyString(String value, List<String> fragments, List<String> propertyRefs) {
      int prev = 0;
      int pos;
      //search for the next instance of $ from the 'prev' position
      while ((pos = value.indexOf("$", prev)) >= 0) {

          //if there was any text before this, add it as a fragment
          //TODO, this check could be modified to go if pos>prev;
          //seems like this current version could stick empty strings
          //into the list
          if (pos > 0) {
              fragments.add(value.substring(prev, pos));
          }
          //if we are at the end of the string, we tack on a $
          //then move past it
          if (pos == (value.length() - 1)) {
              fragments.add("$");
              prev = pos + 1;
          } else if (value.charAt(pos + 1) != '{') {
              //peek ahead to see if the next char is a property or not
              //not a property: insert the char as a literal
              /*
              fragments.addElement(value.substring(pos + 1, pos + 2));
              prev = pos + 2;
              */
              if (value.charAt(pos + 1) == '$') {
                  //backwards compatibility two $ map to one mode
                  fragments.add("$");
                  prev = pos + 2;
              } else {
                  //new behaviour: $X maps to $X for all values of X!='$'
                  fragments.add(value.substring(pos, pos + 2));
                  prev = pos + 2;
              }

          } else {
              //property found, extract its name or bail on a typo
              int endName = value.indexOf('}', pos);
              if (endName < 0) {
                throw new RuntimeException("Syntax error in property: " + value);
              }
              String propertyName = value.substring(pos + 2, endName);
              fragments.add(null);
              propertyRefs.add(propertyName);
              prev = endName + 1;
          }
      }
      //no more $ signs found
      //if there is any tail to the string, append it
      if (prev < value.length()) {
          fragments.add(value.substring(prev));
      }
  }

  public static long getChildrenCount(NodeInfo node) {
    if (!node.hasChildNodes()) return 0;
    Iterable<? extends NodeInfo> it = node.children();
    LongAdder count = new LongAdder();
    it.forEach(nodeInfo -> count.increment());
    return count.sum();
  }
}

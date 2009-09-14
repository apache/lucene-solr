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
package org.apache.solr.handler.dataimport;

import javax.xml.stream.XMLInputFactory;
import static javax.xml.stream.XMLStreamConstants.*;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * A streaming xpath parser which uses StAX for XML parsing. It supports only a
 * subset of xpath syntax.
 * </p>
 * /a/b/subject[@qualifier='fullTitle']
 * /a/b/subject/@qualifier
 * /a/b/c
 *
 * Keep in mind that the wild-card syntax  '//' is not supported
 *
 * <p/>
 * <b>This API is experimental and may change in the future.</b>
 * This class is thread-safe for parsing xml . But adding fields is not thread-safe. The recommended usage is
 * to addField() in one thread and then share the instance across threads.
 *
 * @version $Id$
 * @since solr 1.3
 */
public class XPathRecordReader {
  private Node rootNode = new Node("/", null);
  /**Use this flag in the addField() method to fetch all the cdata under a specific tag
   *
   */
  public static final int FLATTEN = 1;

  /**
   * @param forEachXpath  The XPATH for which a record is emitted. At the start of this xpath tag, it starts collecting the fields and at the close
   * of the tag ,a record is emitted and the fields collected since the tag start is included in the record. If there
   * are fields collected in the parent tag(s) they also will be included in the record but not cleared after emitting the record.
   * It can use the ' | ' syntax of XPATH to pass in multiple xpaths.
   */
  public XPathRecordReader(String forEachXpath) {
    String[] splits = forEachXpath.split("\\|");
    for (String split : splits) {
      split = split.trim();
      if (split.length() == 0)
        continue;
      addField0(split, split, false, true, 0);
    }
  }

  public synchronized XPathRecordReader addField(String name, String xpath, boolean multiValued) {
    if (!xpath.startsWith("/"))
      throw new RuntimeException("xpath must start with '/' : " + xpath);
    addField0(xpath, name, multiValued, false, 0);
    return this;
  }

  /**Add a field's XPATH and its name.
   * @param name . The name by which this field is referred in the emitted record
   * @param xpath . The xpath  to this field
   * @param multiValued . If this is 'true' , then the emitted record will have a List<String> as value
   * @param flags . The only supported flag is 'FLATTEN'
   */
  public synchronized XPathRecordReader addField(String name, String xpath, boolean multiValued, int flags) {
    if (!xpath.startsWith("/"))
      throw new RuntimeException("xpath must start with '/' : " + xpath);
    addField0(xpath, name, multiValued, false, flags);
    return this;
  }

  private void addField0(String xpath, String name, boolean multiValued,
                         boolean isRecord, int flags) {
    List<String> paths = splitEscapeQuote(xpath);
    if ("".equals(paths.get(0).trim()))
      paths.remove(0);
    rootNode.build(paths, name, multiValued, isRecord, flags);
  }

  public List<Map<String, Object>> getAllRecords(Reader r) {
    final List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    streamRecords(r, new Handler() {
      public void handle(Map<String, Object> record, String s) {
        results.add(record);
      }
    });
    return results;
  }

  /** Stream records as and when they are colected
   * @param r The reader
   * @param handler The callback instance
   */
  public void streamRecords(Reader r, Handler handler) {
    try {
      XMLStreamReader parser = factory.createXMLStreamReader(r);
      rootNode.parse(parser, handler, new HashMap<String, Object>(),
              new Stack<Set<String>>(), false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**For each node/leaf in the tree there is one object of this class
   */
  private class Node {
    /**name of the tag/attribute*/
    String name;

    /**The field name as passed in the addField() . This will be used in the record*/
    String fieldName;
    /**stores the xpath name such as '@attr='xyz'*/
    String xpathName;
    /**The xpath of the record. if this is a record node */
    String forEachPath;
    /**child attribute nodes */
    List<Node> attributes;
    /**child nodes*/
    List<Node> childNodes;
    /**if attribs are used in the xpath their names and values*/
    List<Map.Entry<String, String>> attribAndValues;

    /**Parent node of this node */
    Node parent;

    boolean hasText = false, multiValued = false, isRecord = false;

    private boolean flatten;

    public Node(String name, Node p) {
      xpathName = this.name = name;
      parent = p;
    }

    public Node(String name, String fieldName, boolean multiValued) {
      this.name = name;
      this.fieldName = fieldName;
      this.multiValued = multiValued;
    }

    /**This is the method where all the parsing happens. For each tag/subtag this gets called recursively.
     */
    private void parse(XMLStreamReader parser, Handler handler,
                       Map<String, Object> values, Stack<Set<String>> stack,
                       boolean recordStarted) throws IOException, XMLStreamException {
      Set<String> valuesAddedinThisFrame = null;
      if (isRecord) {
        recordStarted = true;
        valuesAddedinThisFrame = new HashSet<String>();
        stack.push(valuesAddedinThisFrame);
      } else if (recordStarted) {
        valuesAddedinThisFrame = stack.peek();
      } else {
        if (attributes != null || hasText)
          valuesAddedinThisFrame = new HashSet<String>();
        stack.push(valuesAddedinThisFrame);
      }
      try {
        if (attributes != null) {
          for (Node node : attributes) {
            String value = parser.getAttributeValue(null, node.name);
            if (value != null || (recordStarted && !isRecord)) {
              putText(values, value, node.fieldName, node.multiValued);
              valuesAddedinThisFrame.add(node.fieldName);
            }
          }
        }
        Set<Node> childrenFound = new HashSet<Node>();
        boolean skipNextEvent = false;
        int event = -1;
        while (true) {
          if (!skipNextEvent) {
            event = parser.next();
            skipNextEvent = false;
          }
          if (event == END_DOCUMENT) {
            return;
          }
          if (event == END_ELEMENT) {
            if (isRecord)
              handler.handle(getDeepCopy(values), forEachPath);
            if (recordStarted && !isRecord
                    && !childrenFound.containsAll(childNodes)) {
              for (Node n : childNodes) {
                if (!childrenFound.contains(n))
                  n.putNulls(values);
              }
            }
            return;
          }
          if ((event == CDATA || event == CHARACTERS || event == SPACE)
                  && hasText) {
            valuesAddedinThisFrame.add(fieldName);
            skipNextEvent = true;
            String text = parser.getText();
            event = parser.next();

            while (true) {
              if(event == CDATA || event == CHARACTERS || event == SPACE) {
                text = text + parser.getText();
              } else if(event == START_ELEMENT) {
                if (flatten) {
                  int starts = 1;
                  while (true) {
                    event = parser.next();
                    if (event == CDATA || event == CHARACTERS || event == SPACE) {
                      text = text + parser.getText();
                    } else if (event == START_ELEMENT) {
                      starts++;
                    } else if (event == END_ELEMENT) {
                      starts--;
                      if (starts == 0) break;
                    }
                  }
                } else {
                  handleStartElement(parser, childrenFound, handler, values, stack, recordStarted);
                }
              } else {
                break;
              }
              event = parser.next();
            }
            putText(values, text, fieldName, multiValued);
          } else if (event == START_ELEMENT) {
            handleStartElement(parser, childrenFound, handler, values, stack, recordStarted);
          }
        }
      } finally {
        /*If a record has ended  (tag closed) then clearup all the fields found
        in this record after this tag started */
        Set<String> cleanThis = null;
        if (isRecord || !recordStarted) {
          cleanThis = stack.pop();
        } else {
          return;
        }
        if (cleanThis != null) {
          for (String fld : cleanThis) {
            values.remove(fld);
          }
        }
      }
    }

    /**if a new tag is encountered, check if it is of interest of not (if there is a matching child Node).
     * if yes continue parsing else skip
     */
    private void handleStartElement(XMLStreamReader parser, Set<Node> childrenFound,
                                    Handler handler, Map<String, Object> values,
                                    Stack<Set<String>> stack, boolean recordStarted)
            throws IOException, XMLStreamException {
      Node n = getMatchingChild(parser);
      if (n != null) {
        childrenFound.add(n);
        n.parse(parser, handler, values, stack, recordStarted);
      } else {
        skipTag(parser);
      }
    }

    /**check if the current tag is to be parsed or not. if yes return the Node object
     */
    private Node getMatchingChild(XMLStreamReader parser) {
      if (childNodes == null)
        return null;
      String localName = parser.getLocalName();
      for (Node n : childNodes) {
        if (n.name.equals(localName)) {
          if (n.attribAndValues == null)
            return n;
          if (checkForAttributes(parser, n.attribAndValues))
            return n;
        }
      }
      return null;
    }

    private boolean checkForAttributes(XMLStreamReader parser,
                                       List<Map.Entry<String, String>> attrs) {
      for (Map.Entry<String, String> e : attrs) {
        String val = parser.getAttributeValue(null, e.getKey());
        if (val == null)
          return false;
        if (e.getValue() != null && !e.getValue().equals(val))
          return false;

      }
      return true;
    }

    /**If there is no value available for a field in a subtag then add a null
     * TODO : needs better explanation
     */
    private void putNulls(Map<String, Object> values) {
      if (attributes != null) {
        for (Node n : attributes) {
          if (n.multiValued)
            putText(values, null, n.fieldName, true);
        }
      }
      if (hasText && multiValued)
        putText(values, null, fieldName, true);
      if (childNodes != null) {
        for (Node childNode : childNodes)
          childNode.putNulls(values);
      }
    }

    /**Handle multivalued fields by adding List<String>
     */
    @SuppressWarnings("unchecked")
    private void putText(Map<String, Object> values, String value,
                         String fieldName, boolean multiValued) {
      if (multiValued) {
        List<String> v = (List<String>) values.get(fieldName);
        if (v == null) {
          v = new ArrayList<String>();
          values.put(fieldName, v);
        }
        v.add(value);
      } else {
        values.put(fieldName, value);
      }
    }

    /**Skip a tag w/o processing the tag or its subtags
     */
    private void skipTag(XMLStreamReader parser) throws IOException,
            XMLStreamException {
      int type;
      while ((type = parser.next()) != END_ELEMENT) {
        if (type == START_ELEMENT)
          skipTag(parser);
      }
    }

    /**Build the node structure from the xpath
     * @param paths the xpaths split by '/'
     * @param fieldName name of the field
     * @param multiValued . is multiValued or not
     * @param record is this xpath a record or a field
     * @param flags extra flags
     */
    private void build(List<String> paths, String fieldName,
                      boolean multiValued, boolean record, int flags) {
      String name = paths.remove(0);
      if (paths.isEmpty() && name.startsWith("@")) {
        if (attributes == null) {
          attributes = new ArrayList<Node>();
        }
        name = name.substring(1);
        attributes.add(new Node(name, fieldName, multiValued));

      } else {
        if (childNodes == null)
          childNodes = new ArrayList<Node>();
        Node n = getOrAddChildNode(name);
        if (paths.isEmpty()) {
          if (record) {
            n.isRecord = true;
            n.forEachPath = fieldName;
          } else {
            n.hasText = true;
            n.fieldName = fieldName;
            n.multiValued = multiValued;
            n.flatten = flags == FLATTEN;
          }
        } else {
          n.build(paths, fieldName, multiValued, record, flags);
        }
      }
    }

    private Node getOrAddChildNode(String xpathName) {
      for (Node n : childNodes)
        if (n.xpathName.equals(xpathName))
          return n;

      Node n = new Node(xpathName, this);
      Matcher m = ATTRIB_PRESENT_WITHVAL.matcher(xpathName);
      if (m.find()) {
        n.name = m.group(1);
        int start = m.start(2);
        while (true) {
          HashMap<String, String> attribs = new HashMap<String, String>();
          if (!m.find(start))
            break;
          attribs.put(m.group(3), m.group(5));
          start = m.end(6);
          if (n.attribAndValues == null)
            n.attribAndValues = new ArrayList<Map.Entry<String, String>>();
          n.attribAndValues.addAll(attribs.entrySet());

        }
      }
      childNodes.add(n);
      return n;
    }
  }

  /**If a field has List then they have to be deep-copied for thread safety
   */
  private Map<String, Object> getDeepCopy(Map<String, Object> values) {
    Map<String, Object> result = new HashMap<String, Object>();
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      if (entry.getValue() instanceof List) {
        result.put(entry.getKey(),new ArrayList((List) entry.getValue()));
      } else{
        result.put(entry.getKey(),entry.getValue());
      }
    }
    return result;
  }

  /**
   * Used for handling cases where there is a slash '/' character
   * inside the attribute value e.g. x@html='text/html'. We need to split
   * by '/' excluding the '/' which is a part of the attribute's value.
   */
  private static List<String> splitEscapeQuote(String str) {
    List<String> result = new LinkedList<String>();
    String[] ss = str.split("/");
    for (int i = 0; i < ss.length; i++) {
      if (ss[i].length() == 0 && result.size() == 0) continue;
      StringBuilder sb = new StringBuilder();
      int quoteCount = 0;
      while (true) {
        sb.append(ss[i]);
        for (int j = 0; j < ss[i].length(); j++) if (ss[i].charAt(j) == '\'') quoteCount++;
        if ((quoteCount % 2) == 0) break;
        i++;
        sb.append("/");
      }
      result.add(sb.toString());
    }
    return result;
  }

  static XMLInputFactory factory = XMLInputFactory.newInstance();
  static{
    factory.setProperty(XMLInputFactory.IS_VALIDATING , Boolean.FALSE); 
    factory.setProperty(XMLInputFactory.SUPPORT_DTD , Boolean.FALSE);
  }

  /**Implement this interface to stream records as and when it is found.
   *
   */
  public static interface Handler {
    /**
     * @param record The record map . The key is the field name as provided in the addField() methods. The value
     * can be a single String (for single valued) or a List<String> (for multiValued)
     * if an Exception is thrown from this method the parsing will be aborted
     * @param xpath . The forEach XPATH for which this record is being emitted
     */
    public void handle(Map<String, Object> record, String xpath);
  }

  private static final Pattern ATTRIB_PRESENT_WITHVAL = Pattern
          .compile("(\\S*?)?(\\[@)(\\S*?)(='(.*?)')?(\\])");
}

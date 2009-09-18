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
 * A streaming xpath parser which uses StAX for XML parsing. It supports only
 * a subset of xpath syntax.
 * </p><pre>
 * /a/b/subject[@qualifier='fullTitle']
 * /a/b/subject[@qualifier=]/subtag
 * /a/b/subject/@qualifier
 * /a/b/c
 * </pre>
 * Keep in mind that the wild-card syntax  '//' is not supported
 * A record is a Map<String,Object> . The key is the provided name
 * and the value is a String or a List<String>
 *
 * This class is thread-safe for parsing xml. But adding fields is not
 * thread-safe. The recommended usage is to addField() in one thread and 
 * then share the instance across threads.
 * </p>
 * <p/>
 * <b>This API is experimental and may change in the future.</b>
 * <p>
 * @version $Id$
 * @since solr 1.3
 */
public class XPathRecordReader {
  private Node rootNode = new Node("/", null);
  /** 
   * The FLATTEN flag indicates that all text and cdata under a specific
   * tag should be recursivly fetched and appended to the current Node's
   * value.
   */
  public static final int FLATTEN = 1;

  /**
   * A constructor called with a '|' seperated list of Xpath expressions
   * which define sub sections of the XML stream that are to be emitted
   * seperate records.
   * 
   * @param forEachXpath  The XPATH for which a record is emitted. Once the
   * xpath tag is encountered, the Node.parse method starts collecting wanted 
   * fields and at the close of the tag, a record is emitted containing all 
   * fields collected since the tag start. Once 
   * emitted the collected fields are cleared. Any fields collected in the parent tag or above
   * will also be included in the record, but these are not
   * cleared after emitting the record.

   * It uses the ' | ' syntax of XPATH to pass in multiple xpaths.
   */
  public XPathRecordReader(String forEachXpath) {
    String[] splits = forEachXpath.split("\\|");
    for (String split : splits) {
      split = split.trim();
      if (split.length() == 0)
        continue;
      // The created Node has a name set to the full forEach attribute xpath
      addField0(split, split, false, true, 0);
    }
  }

  /**
   * A wrapper around {@link #addField0 addField0()} to create a series of Nodes 
   * based on the supplied Xpath for the given fieldName. The created nodes 
   * are inserted into a Node tree.
   *
   * @param name The name for this field in the emitted record
   * @param xpath The xpath expression for this field
   * @param multiValued If 'true' then the emitted record will have values in 
   *                    a List<String>
   */
  public synchronized XPathRecordReader addField(String name, String xpath, boolean multiValued) {
    if (!xpath.startsWith("/"))
      throw new RuntimeException("xpath must start with '/' : " + xpath);
    addField0(xpath, name, multiValued, false, 0);
    return this;
  }

  /**
   * A wrapper around {@link #addField0 addField0()} to create a series of Nodes 
   * based on the supplied Xpath for the given fieldName. The created nodes 
   * are inserted into a Node tree.
   *
   * @param name The name for this field in the emitted record
   * @param xpath The xpath expression for this field
   * @param multiValued If 'true' then the emitted record will have values in 
   *                    a List<String>
   * @param flags FLATTEN: Recursivly combine text from all child XML elements
   */
  public synchronized XPathRecordReader addField(String name, String xpath, boolean multiValued, int flags) {
    if (!xpath.startsWith("/"))
      throw new RuntimeException("xpath must start with '/' : " + xpath);
    addField0(xpath, name, multiValued, false, flags);
    return this;
  }

  /**
   * Splits the XPATH into a List of xpath segments and calls build() to
   * construct a tree of Nodes representing xpath segments. The resulting
   * tree structure ends up describing all the Xpaths we are interested in.
   *
   * @param xpath The xpath expression for this field
   * @param name The name for this field in the emitted record
   * @param multiValued If 'true' then the emitted record will have values in 
   *                    a List<String>
   * @param isRecord When 'true' flags that this XPATH is from a forEach statement
   * @param flags The only supported flag is 'FLATTEN'
   */
  private void addField0(String xpath, String name, boolean multiValued,
                         boolean isRecord, int flags) {
    List<String> paths = splitEscapeQuote(xpath);
    if ("".equals(paths.get(0).trim()))
      paths.remove(0);
    rootNode.build(paths, name, multiValued, isRecord, flags);
  }

  /** 
   * Uses {@link #streamRecords streamRecords} to parse the XML source but 
   * collects the emitted records into a List which is returned upon completion.
   *
   * @param r the stream reader
   * @return results a List of emitted records
   *
   */
  public List<Map<String, Object>> getAllRecords(Reader r) {
    final List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    streamRecords(r, new Handler() {
      public void handle(Map<String, Object> record, String s) {
        results.add(record);
      }
    });
    return results;
  }

  /** 
   * Creates an XML stream reader on top of whatever reader has been
   * configured. Then calls parse() with a handler which is
   * invoked forEach record emitted.
   *
   * @param r the stream reader
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


  /**
   * For each node/leaf in the Node tree there is one object of this class.
   * This tree of objects represents all the XPaths we are interested in.
   * For each Xpath segment of interest we create a node. In most cases the
   * node (branch) is rather basic , but for the final portion (leaf) of any Xpath  we add
   * more information to the Node. When parsing the XML document we
   * step though this tree as we stream records from the reader. If the XML
   * document departs from this tree we skip start tags till we are back on 
   * the tree.
   *
   */
  private class Node {
    String name;      // genrally: segment of the Xpath represented by this Node
    String fieldName; // the fieldname in the emitted record (key of the map)
    String xpathName; // the segment of the Xpath represented by this Node
    String forEachPath; // the full Xpath from the forEach entity attribute
    List<Node> attributes; // a List of attribute Nodes associated with this Node
    List<Node> childNodes; // a List of child Nodes of this node
    List<Map.Entry<String, String>> attribAndValues;
    Node parent; // parent Node in the tree
    boolean hasText=false; // flag: store/emit streamed text for this node
    boolean multiValued=false; //flag: this fields values are returned as a List
    boolean isRecord=false; //flag: this Node starts a new record
    private boolean flatten; //flag: child text is also to be emitted


    public Node(String name, Node p) {
      // Create a basic Node, suitable for the mid portions of any Xpath.
      // Node.xpathName and Node.name are set to same value
      xpathName = this.name = name;
      parent = p;
    }

    public Node(String name, String fieldName, boolean multiValued) {
      // This is only called from build() when describing an attribute.
      this.name = name;               // a segment from the Xpath
      this.fieldName = fieldName;     // name to store collected values against
      this.multiValued = multiValued; // return collected values in a List
    }

    /**
     * This is the method where all the XML parsing happens. For each 
     * tag/subtag read from the source, this method is called recursively.
     *
     */
    private void parse(XMLStreamReader parser, Handler handler,
                       Map<String, Object> values, Stack<Set<String>> stack,
                       boolean recordStarted) throws IOException, XMLStreamException {
      Set<String> valuesAddedinThisFrame = null;
      if (isRecord) {
        // This Node is a match for an XPATH from a forEach attribute, 
        // prepare to emit a new record when its END_ELEMENT is matched 
        recordStarted = true;
        valuesAddedinThisFrame = new HashSet<String>();
        stack.push(valuesAddedinThisFrame);
      } else if (recordStarted) {
        // This node is a child of some parent which matched against forEach 
        // attribute. Continue to add values to an existing record.
        valuesAddedinThisFrame = stack.peek();
      } else {
        //if this tag has an attribute or text which is a brank/leaf just push an item up the stack
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
        // Internally we have to gobble CDATA | CHARACTERS | SPACE events as we
        // store text, the gobbling continues till we have fetched some other 
        // event. We use "isNextEventFetched" to indcate that the gobbling has
        // already fetched the next event.
        boolean isNextEventFetched = false;
        int event = -1;

        while (true) {
          if (!isNextEventFetched) {
            event = parser.next();
            isNextEventFetched = false;
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
            // becuase we are fetching events here we need to ensure the outer
            // loop does not end up doing an extra parser.next()
            isNextEventFetched = true;
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
                  // We are not flatten-ing, so look to see if any of the child
                  // elements are wanted, and recurse if any are found.
                  handleStartElement(parser, childrenFound, handler, values, stack, recordStarted);
                }
              } else {
                break;
              }
              event = parser.next();
            }
            // save the text we have read against the fieldName in the Map values
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
        }
      else {
        // skip ELEMENTS till source document is back within the tree
        int count=1; // we have had our first START_ELEMENT
        while ( count != 0 ) {
          int token = parser.next();
          if (token == START_ELEMENT) count++;
          else if (token == END_ELEMENT)  count--;
          }
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

    /**
     * A recursive routine that walks the Node tree from a supplied start
     * pushing a null string onto every multiValued fieldName's List of values.
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

    /**
     * Add the field name and text into the values Map. If it is a non multivalued field, then the text
     * is simply placed in the object portion of the Map. If it is a
     * multivalued field then the text is pushed onto a List which is
     * the object portion of the Map.
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


    /**
     * Build a Node tree structure representing all Xpaths of intrest to us.
     * This must be done before parsing of the XML stream starts. Each node 
     * holds one portion of an Xpath. Taking each Xpath segment in turn this
     * method walks the Node tree  and finds where the new segment should be
     * inserted. It creates a Node representing a field's name, XPATH and 
     * some flags and inserts the Node into the Node tree.
     *
     */
    private void build(
        List<String> paths,   // a List of segments from the split xpaths
        String fieldName,     // the fieldName assoc with this Xpath
        boolean multiValued,  // flag if this fieldName is multiValued or not
        boolean record,       // is this xpath a record or a field
        int flags             // are we to flatten matching xpaths
        ) {
      // recursivly walk the paths Lists adding new Nodes as required
      String name = paths.remove(0); // shift out next Xpath segment
      if (paths.isEmpty() && name.startsWith("@")) {
        // we have reached end of element portion of Xpath and can now only
        // have an element attribute. Add it to this nodes list of attributes
        if (attributes == null) {
          attributes = new ArrayList<Node>();
        }
        name = name.substring(1); // strip the '@'
        attributes.add(new Node(name, fieldName, multiValued));

      } else {
        if (childNodes == null)
          childNodes = new ArrayList<Node>();
        // does this "name" already exist as a child node.
        Node n = getOrAddChildNode(name);
        if (paths.isEmpty()) {
          // We have reached the end of paths. When parsing the actual
          // input we have traversed to a position where we actutally have to
          // do something. getOrAddChildNode() will have created and returned
          // a new minimal Node with name and xpathName already populated. We
          // need to add more information
          if (record) {
            // forEach attribute
            n.isRecord = true; // flag: forEach attribute, prepare to emit rec
            n.forEachPath = fieldName; // the full forEach attribute xpath
          } else {
            // xpath with content we want to store and return
            n.hasText = true;        // we have to store text found here
            n.fieldName = fieldName; // name to store collected text against
            n.multiValued = multiValued; // true: text be stored in a List
            n.flatten = flags == FLATTEN; // true: store text from child tags
          }
        } else {
          // recurse to handle next paths segment
          n.build(paths, fieldName, multiValued, record, flags);
        }
      }
    }

    private Node getOrAddChildNode(String xpathName) {
      for (Node n : childNodes)
        if (n.xpathName.equals(xpathName))
          return n;
      // new territory! add a new node for this Xpath bitty
      Node n = new Node(xpathName, this); // a minimal Node initalization
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
  } // end of class Node


  /**
   * Copies a supplied Map to a new Map which is returned. Used to copy a 
   * records values. If a fields value is a List then they have to be 
   * deep-copied for thread safety
   */
  private Map<String, Object> getDeepCopy(Map<String, Object> values) {
    Map<String, Object> result = new HashMap<String, Object>();
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      if (entry.getValue() instanceof List) {
        result.put(entry.getKey(),new ArrayList((List) entry.getValue()));
      } else {
        result.put(entry.getKey(),entry.getValue());
      }
    }
    return result;
  }

  /**
   * The Xpath is split into segments using the '/' s a seperator. However
   * this method deals with special cases where there is a slash '/' character
   * inside the attribute value e.g. x/@html='text/html'. We need to split
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

  /**Implement this interface to stream records as and when one is found.
   *
   */
  public static interface Handler {
    /**
     * @param record The record map. The key is the field name as provided in 
     * the addField() methods. The value can be a single String (for single 
     * valued fields) or a List<String> (for multiValued).
     * @param xpath The forEach XPATH for which this record is being emitted
     * If there is any change all parsing will be aborted and the Exception is propogated up
     */
    public void handle(Map<String, Object> record, String xpath);
  }

  private static final Pattern ATTRIB_PRESENT_WITHVAL = Pattern
          .compile("(\\S*?)?(\\[@)(\\S*?)(='(.*?)')?(\\])");
}

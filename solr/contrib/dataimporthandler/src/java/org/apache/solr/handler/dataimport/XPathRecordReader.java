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
package org.apache.solr.handler.dataimport;

import org.apache.solr.common.util.XMLErrorLogger;
import org.apache.solr.common.EmptyEntityResolver;
import javax.xml.stream.XMLInputFactory;
import static javax.xml.stream.XMLStreamConstants.*;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A streaming xpath parser which uses StAX for XML parsing. It supports only
 * a subset of xpath syntax.
 * </p><pre>
 * /a/b/subject[@qualifier='fullTitle']
 * /a/b/subject[@qualifier=]/subtag
 * /a/b/subject/@qualifier
 * //a
 * //a/b...
 * /a//b
 * /a//b...
 * /a/b/c
 * </pre>
 * A record is a Map&lt;String,Object&gt; . The key is the provided name
 * and the value is a String or a List&lt;String&gt;
 *
 * This class is thread-safe for parsing xml. But adding fields is not
 * thread-safe. The recommended usage is to addField() in one thread and 
 * then share the instance across threads.
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.3
 */
public class XPathRecordReader {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final XMLErrorLogger XMLLOG = new XMLErrorLogger(log);

  private Node rootNode = new Node("/", null);

  /** 
   * The FLATTEN flag indicates that all text and cdata under a specific
   * tag should be recursivly fetched and appended to the current Node's
   * value.
   */
  public static final int FLATTEN = 1;

  /**
   * A constructor called with a '|' separated list of Xpath expressions
   * which define sub sections of the XML stream that are to be emitted as
   * separate records.
   * 
   * @param forEachXpath  The XPATH for which a record is emitted. Once the
   * xpath tag is encountered, the Node.parse method starts collecting wanted 
   * fields and at the close of the tag, a record is emitted containing all 
   * fields collected since the tag start. Once 
   * emitted the collected fields are cleared. Any fields collected in the 
   * parent tag or above will also be included in the record, but these are
   * not cleared after emitting the record.
   *
   * It uses the ' | ' syntax of XPATH to pass in multiple xpaths.
   */
  public XPathRecordReader(String forEachXpath) {
    String[] splits = forEachXpath.split("\\|");
    for (String split : splits) {
      split = split.trim();
      if (split.startsWith("//"))
         throw new RuntimeException("forEach cannot start with '//': " + split);
      if (split.length() == 0)
        continue;
      // The created Node has a name set to the full forEach attribute xpath
      addField0(split, split, false, true, 0);
    }
  }

  /**
   * A wrapper around <code>addField0</code> to create a series of  
   * Nodes based on the supplied Xpath and a given fieldName. The created  
   * nodes are inserted into a Node tree.
   *
   * @param name The name for this field in the emitted record
   * @param xpath The xpath expression for this field
   * @param multiValued If 'true' then the emitted record will have values in 
   *                    a List&lt;String&gt;
   */
  public synchronized XPathRecordReader addField(String name, String xpath, boolean multiValued) {
    addField0(xpath, name, multiValued, false, 0);
    return this;
  }

  /**
   * A wrapper around <code>addField0</code> to create a series of  
   * Nodes based on the supplied Xpath and a given fieldName. The created  
   * nodes are inserted into a Node tree.
   *
   * @param name The name for this field in the emitted record
   * @param xpath The xpath expression for this field
   * @param multiValued If 'true' then the emitted record will have values in 
   *                    a List&lt;String&gt;
   * @param flags FLATTEN: Recursively combine text from all child XML elements
   */
  public synchronized XPathRecordReader addField(String name, String xpath, boolean multiValued, int flags) {
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
   *                    a List&lt;String&gt;
   * @param isRecord Flags that this XPATH is from a forEach statement
   * @param flags The only supported flag is 'FLATTEN'
   */
  private void addField0(String xpath, String name, boolean multiValued,
                         boolean isRecord, int flags) {
    if (!xpath.startsWith("/"))
      throw new RuntimeException("xpath must start with '/' : " + xpath);
    List<String> paths = splitEscapeQuote(xpath);
    // deal with how split behaves when separator starts a string!
    if ("".equals(paths.get(0).trim()))
      paths.remove(0);
    rootNode.build(paths, name, multiValued, isRecord, flags);
    rootNode.buildOptimise(null);
  }

  /** 
   * Uses {@link #streamRecords streamRecords} to parse the XML source but with
   * a handler that collects all the emitted records into a single List which 
   * is returned upon completion.
   *
   * @param r the stream reader
   * @return results a List of emitted records
   */
  public List<Map<String, Object>> getAllRecords(Reader r) {
    final List<Map<String, Object>> results = new ArrayList<>();
    streamRecords(r, (record, s) -> results.add(record));
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
      rootNode.parse(parser, handler, new HashMap<>(),
          new Stack<>(), false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * For each node/leaf in the Node tree there is one object of this class.
   * This tree of objects represents all the XPaths we are interested in.
   * For each Xpath segment of interest we create a node. In most cases the
   * node (branch) is rather basic , but for the final portion (leaf) of any
   * Xpath we add more information to the Node. When parsing the XML document 
   * we step though this tree as we stream records from the reader. If the XML
   * document departs from this tree we skip start tags till we are back on 
   * the tree.
   */
  private static class Node {
    String name;      // genrally: segment of the Xpath represented by this Node
    String fieldName; // the fieldname in the emitted record (key of the map)
    String xpathName; // the segment of the Xpath represented by this Node
    String forEachPath; // the full Xpath from the forEach entity attribute
    List<Node> attributes; // List of attribute Nodes associated with this Node
    List<Node> childNodes; // List of immediate child Nodes of this node
    List<Node> wildCardNodes; // List of '//' style decendants of this Node
    List<Map.Entry<String, String>> attribAndValues;
    Node wildAncestor; // ancestor Node containing '//' style decendants 
    Node parent; // parent Node in the tree
    boolean hasText=false; // flag: store/emit streamed text for this node
    boolean multiValued=false; //flag: this fields values are returned as a List
    boolean isRecord=false; //flag: this Node starts a new record
    private boolean flatten; //flag: child text is also to be emitted


    public Node(String name, Node p) {
      // Create a basic Node, suitable for the mid portions of any Xpath.
      // Node.xpathName and Node.name are set to same value.
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
    private void parse(XMLStreamReader parser, 
                       Handler handler,
                       Map<String, Object> values, 
                       Stack<Set<String>> stack, // lists of values to purge
                       boolean recordStarted
                       ) throws IOException, XMLStreamException {
      Set<String> valuesAddedinThisFrame = null;
      if (isRecord) {
        // This Node is a match for an XPATH from a forEach attribute, 
        // prepare for the clean up that will occurr when the record
        // is emitted after its END_ELEMENT is matched 
        recordStarted = true;
        valuesAddedinThisFrame = new HashSet<>();
        stack.push(valuesAddedinThisFrame);
      } else if (recordStarted) {
        // This node is a child of some parent which matched against forEach 
        // attribute. Continue to add values to an existing record.
        valuesAddedinThisFrame = stack.peek();
      }

      try {
        /* The input stream has deposited us at this Node in our tree of 
         * intresting nodes. Depending on how this node is of interest,
         * process further tokens from the input stream and decide what
         * we do next
         */
        if (attributes != null) {
          // we interested in storing attributes from the input stream
          for (Node node : attributes) {
            String value = parser.getAttributeValue(null, node.name);
            if (value != null || (recordStarted && !isRecord)) {
              putText(values, value, node.fieldName, node.multiValued);
              valuesAddedinThisFrame.add(node.fieldName);
            }
          }
        }

        Set<Node> childrenFound = new HashSet<>();
        int event = -1;
        int flattenedStarts=0; // our tag depth when flattening elements
        StringBuilder text = new StringBuilder();

        while (true) {  
          event = parser.next();
   
          if (event == END_ELEMENT) {
            if (flattenedStarts > 0) flattenedStarts--;
            else {
              if (hasText && valuesAddedinThisFrame != null) {
                valuesAddedinThisFrame.add(fieldName);
                putText(values, text.toString(), fieldName, multiValued);
              }
              if (isRecord) handler.handle(getDeepCopy(values), forEachPath);
              if (childNodes != null && recordStarted && !isRecord && !childrenFound.containsAll(childNodes)) {
                // nonReccord nodes where we have not collected text for ALL
                // the child nodes.
                for (Node n : childNodes) {
                  // For the multivalue child nodes where we could have, but
                  // didnt, collect text. Push a null string into values.
                  if (!childrenFound.contains(n)) n.putNulls(values, valuesAddedinThisFrame);
                }
              }
              return;
            }
          }
          else if (hasText && (event==CDATA || event==CHARACTERS || event==SPACE)) {
            text.append(parser.getText());
          } 
          else if (event == START_ELEMENT) {
            if ( flatten ) 
               flattenedStarts++;
            else 
               handleStartElement(parser, childrenFound, handler, values, stack, recordStarted);
          }
          // END_DOCUMENT is least likely to appear and should be 
          // last in if-then-else skip chain
          else if (event == END_DOCUMENT) return;
          }
        }finally {
        if ((isRecord || !recordStarted) && !stack.empty()) {
          Set<String> cleanThis = stack.pop();
          if (cleanThis != null) {
            for (String fld : cleanThis) values.remove(fld);
          }
        }
      }
    }

    /**
     * If a new tag is encountered, check if it is of interest or not by seeing
     * if it matches against our node tree. If we have deperted from the node 
     * tree then walk back though the tree's ancestor nodes checking to see if
     * any // expressions exist for the node and compare them against the new
     * tag. If matched then "jump" to that node, otherwise ignore the tag.
     *
     * Note, the list of // expressions found while walking back up the tree
     * is chached in the HashMap decends. Then if the new tag is to be skipped,
     * any inner chil tags are compared against the cache and jumped to if
     * matched.
     */
    private void handleStartElement(XMLStreamReader parser, Set<Node> childrenFound,
                                    Handler handler, Map<String, Object> values,
                                    Stack<Set<String>> stack, boolean recordStarted)
            throws IOException, XMLStreamException {
      Node n = getMatchingNode(parser,childNodes);
      Map<String, Object> decends=new HashMap<>();
      if (n != null) {
        childrenFound.add(n);
        n.parse(parser, handler, values, stack, recordStarted);
        return;
        }
      // The stream has diverged from the tree of interesting elements, but
      // are there any wildCardNodes ... anywhere in our path from the root?
      Node dn = this; // checking our Node first!
            
      do {
        if (dn.wildCardNodes != null) {
          // Check to see if the streams tag matches one of the "//" all
          // decendents type expressions for this node.
          n = getMatchingNode(parser, dn.wildCardNodes);
          if (n != null) {
            childrenFound.add(n);
            n.parse(parser, handler, values, stack, recordStarted);
            break;
          }
          // add the list of this nodes wild decendents to the cache
          for (Node nn : dn.wildCardNodes) decends.put(nn.name, nn);
        }
        dn = dn.wildAncestor; // leap back along the tree toward root
      } while (dn != null) ;
 
      if (n == null) {
        // we have a START_ELEMENT which is not within the tree of
        // interesting nodes. Skip over the contents of this element
        // but recursivly repeat the above for any START_ELEMENTs
        // found within this element.
        int count = 1; // we have had our first START_ELEMENT
        while (count != 0) {
          int token = parser.next();
          if (token == START_ELEMENT) {
            Node nn = (Node) decends.get(parser.getLocalName());
            if (nn != null) {
              // We have a //Node which matches the stream's parser.localName
              childrenFound.add(nn);
              // Parse the contents of this stream element
              nn.parse(parser, handler, values, stack, recordStarted);
            } 
            else count++;
          } 
          else if (token == END_ELEMENT) count--;
        }
      }
    }


    /**
     * Check if the current tag is to be parsed or not. We step through the
     * supplied List "searchList" looking for a match. If matched, return the
     * Node object.
     */
    private Node getMatchingNode(XMLStreamReader parser,List<Node> searchL){
      if (searchL == null)
        return null;
      String localName = parser.getLocalName();
      for (Node n : searchL) {
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
     * pushing a null string onto every multiValued fieldName's List of values
     * where a value has not been provided from the stream.
     */
    private void putNulls(Map<String, Object> values, Set<String> valuesAddedinThisFrame) {
      if (attributes != null) {
        for (Node n : attributes) {
          if (n.multiValued) {
            putANull(n.fieldName, values, valuesAddedinThisFrame);
          }
        }
      }
      if (hasText && multiValued) {
        putANull(fieldName, values, valuesAddedinThisFrame);
      }
      if (childNodes != null) {
        for (Node childNode : childNodes) {
          childNode.putNulls(values, valuesAddedinThisFrame);
        }
      }
    }
    
    private void putANull(String thisFieldName, Map<String, Object> values, Set<String> valuesAddedinThisFrame) {
      putText(values, null, thisFieldName, true);
      if( valuesAddedinThisFrame != null) {
        valuesAddedinThisFrame.add(thisFieldName);
      }
    }

    /**
     * Add the field name and text into the values Map. If it is a non
     * multivalued field, then the text is simply placed in the object
     * portion of the Map. If it is a multivalued field then the text is
     * pushed onto a List which is the object portion of the Map.
     */
    @SuppressWarnings("unchecked")
    private void putText(Map<String, Object> values, String value,
                         String fieldName, boolean multiValued) {
      if (multiValued) {
        List<String> v = (List<String>) values.get(fieldName);
        if (v == null) {
          v = new ArrayList<>();
          values.put(fieldName, v);
        }
        v.add(value);
      } else {
        values.put(fieldName, value);
      }
    }


    /**
     * Walk the Node tree propagating any wildDescentant information to
     * child nodes. This allows us to optimise the performance of the
     * main parse method.
     */
    private void buildOptimise(Node wa) {
     wildAncestor=wa;
     if ( wildCardNodes != null ) wa = this;
     if ( childNodes != null )
       for ( Node n : childNodes ) n.buildOptimise(wa);
     }

    /**
     * Build a Node tree structure representing all Xpaths of intrest to us.
     * This must be done before parsing of the XML stream starts. Each node 
     * holds one portion of an Xpath. Taking each Xpath segment in turn this
     * method walks the Node tree  and finds where the new segment should be
     * inserted. It creates a Node representing a field's name, XPATH and 
     * some flags and inserts the Node into the Node tree.
     */
    private void build(
        List<String> paths,   // a List of segments from the split xpaths
        String fieldName,     // the fieldName assoc with this Xpath
        boolean multiValued,  // flag if this fieldName is multiValued or not
        boolean record,       // is this xpath a record or a field
        int flags             // are we to flatten matching xpaths
        ) {
      // recursivly walk the paths Lists adding new Nodes as required
      String xpseg = paths.remove(0); // shift out next Xpath segment

      if (paths.isEmpty() && xpseg.startsWith("@")) {
        // we have reached end of element portion of Xpath and can now only
        // have an element attribute. Add it to this nodes list of attributes
        if (attributes == null) {
          attributes = new ArrayList<>();
        }
        xpseg = xpseg.substring(1); // strip the '@'
        attributes.add(new Node(xpseg, fieldName, multiValued));
      }
      else if ( xpseg.length() == 0) {
        // we have a '//' selector for all decendents of the current nodes
        xpseg = paths.remove(0); // shift out next Xpath segment
        if (wildCardNodes == null) wildCardNodes = new ArrayList<>();
        Node n = getOrAddNode(xpseg, wildCardNodes);
        if (paths.isEmpty()) {
          // We are current a leaf node.
          // xpath with content we want to store and return
          n.hasText = true;        // we have to store text found here
          n.fieldName = fieldName; // name to store collected text against
          n.multiValued = multiValued; // true: text be stored in a List
          n.flatten = flags == FLATTEN; // true: store text from child tags
        }
        else {
          // recurse to handle next paths segment
          n.build(paths, fieldName, multiValued, record, flags);
        }
      }
      else {
        if (childNodes == null)
          childNodes = new ArrayList<>();
        // does this "name" already exist as a child node.
        Node n = getOrAddNode(xpseg,childNodes);
        if (paths.isEmpty()) {
          // We have emptied paths, we are for the moment a leaf of the tree.
          // When parsing the actual input we have traversed to a position 
          // where we actutally have to do something. getOrAddNode() will
          // have created and returned a new minimal Node with name and
          // xpathName already populated. We need to add more information.
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

    private Node getOrAddNode(String xpathName, List<Node> searchList ) {
      for (Node n : searchList)
        if (n.xpathName.equals(xpathName)) return n;
      // new territory! add a new node for this Xpath bitty
      Node n = new Node(xpathName, this); // a minimal Node initialization
      Matcher m = ATTRIB_PRESENT_WITHVAL.matcher(xpathName);
      if (m.find()) {
        n.name = m.group(1);
        int start = m.start(2);
        while (true) {
          HashMap<String, String> attribs = new HashMap<>();
          if (!m.find(start))
            break;
          attribs.put(m.group(3), m.group(5));
          start = m.end(6);
          if (n.attribAndValues == null)
            n.attribAndValues = new ArrayList<>();
          n.attribAndValues.addAll(attribs.entrySet());
        }
      }
      searchList.add(n);
      return n;
    }

    /**
     * Copies a supplied Map to a new Map which is returned. Used to copy a
     * records values. If a fields value is a List then they have to be
     * deep-copied for thread safety
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Map<String, Object> getDeepCopy(Map<String, Object> values) {
      Map<String, Object> result = new HashMap<>();
      for (Map.Entry<String, Object> entry : values.entrySet()) {
        if (entry.getValue() instanceof List) {
          result.put(entry.getKey(), new ArrayList((List) entry.getValue()));
        } else {
          result.put(entry.getKey(), entry.getValue());
        }
      }
      return result;
    }
  } // end of class Node


  /**
   * The Xpath is split into segments using the '/' as a separator. However
   * this method deals with special cases where there is a slash '/' character
   * inside the attribute value e.g. x/@html='text/html'. We split by '/' but 
   * then reassemble things were the '/' appears within a quoted sub-string.
   *
   * We have already enforced that the string must begin with a separator. This
   * method depends heavily on how split behaves if the string starts with the
   * separator or if a sequence of multiple separator's appear. 
   */
  private static List<String> splitEscapeQuote(String str) {
    List<String> result = new LinkedList<>();
    String[] ss = str.split("/");
    for (int i=0; i<ss.length; i++) { // i=1: skip separator at start of string
      StringBuilder sb = new StringBuilder();
      int quoteCount = 0;
      while (true) {
        sb.append(ss[i]);
        for (int j=0; j<ss[i].length(); j++)
            if (ss[i].charAt(j) == '\'') quoteCount++;
        // have we got a split inside quoted sub-string?
        if ((quoteCount % 2) == 0) break;
        // yes!; replace the '/' and loop to concat next token
        i++;
        sb.append("/");
      }
      result.add(sb.toString());
    }
    return result;
  }

  static XMLInputFactory factory = XMLInputFactory.newInstance();
  static {
    EmptyEntityResolver.configureXMLInputFactory(factory);
    factory.setXMLReporter(XMLLOG);
    try {
      // The java 1.6 bundled stax parser (sjsxp) does not currently have a thread-safe
      // XMLInputFactory, as that implementation tries to cache and reuse the
      // XMLStreamReader.  Setting the parser-specific "reuse-instance" property to false
      // prevents this.
      // All other known open-source stax parsers (and the bea ref impl)
      // have thread-safe factories.
      factory.setProperty("reuse-instance", Boolean.FALSE);
    } catch (IllegalArgumentException ex) {
      // Other implementations will likely throw this exception since "reuse-instance"
      // isimplementation specific.
      log.debug("Unable to set the 'reuse-instance' property for the input chain: {}", factory);
    }
  }

  /**Implement this interface to stream records as and when one is found.
   *
   */
  public interface Handler {
    /**
     * @param record The record map. The key is the field name as provided in 
     * the addField() methods. The value can be a single String (for single 
     * valued fields) or a List&lt;String&gt; (for multiValued).
     * @param xpath The forEach XPATH for which this record is being emitted
     * If there is any change all parsing will be aborted and the Exception
     * is propagated up
     */
    void handle(Map<String, Object> record, String xpath);
  }

  private static final Pattern ATTRIB_PRESENT_WITHVAL = Pattern
          .compile("(\\S*?)?(\\[@)(\\S*?)(='(.*?)')?(\\])");
}

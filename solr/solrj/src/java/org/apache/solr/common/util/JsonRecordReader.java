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
package org.apache.solr.common.util;


import java.io.IOException;
import java.io.Reader;
import java.util.*;

import org.noggit.JSONParser;

import static org.noggit.JSONParser.*;

/**
 * A Streaming parser for json to emit one record at a time.
 */

public class JsonRecordReader {
  public static final String DELIM = ".";

  private Node rootNode = new Node("/", (Node) null);

  public static JsonRecordReader getInst(String split, List<String> fieldMappings) {

    JsonRecordReader jsonRecordReader = new JsonRecordReader();
    jsonRecordReader.addSplit(split);
    for (String s : fieldMappings) {
      String path = s;
      int idx = s.indexOf(':');
      String fieldName = null;
      if (idx > 0) {
        fieldName = s.substring(0, idx);
        path = s.substring(idx + 1);
      }
      jsonRecordReader.addField(path, fieldName, true, false);
    }
    return jsonRecordReader;
  }


  private JsonRecordReader() {
  }
  /**
   * a '|' separated list of path expressions
   * which define sub sections of the JSON stream that are to be emitted as
   * separate records.
   * It is possible to have multiple levels of split one for parent and one for child
   * each child record (or a list of records) will be emitted as a part of the parent record with
   * null as the key
   *
   * @param splitPath The PATH for which a record is emitted.  A record is emitted containing all
   *                  fields collected since the tag start. Once
   *                  emitted the collected fields are cleared. Any fields collected in the
   *                  parent tag or above will also be included in the record, but these are
   *                  not cleared after emitting the record.
   *                  <p>
   *                  It uses the ' | ' syntax of PATH to pass in multiple paths.
   */

  void addSplit(String splitPath) {
    String[] splits = splitPath.split("\\|");
    for (String split : splits) {
      split = split.trim();
      if (split.startsWith("//"))
        throw new RuntimeException("split cannot start with '//': " + split);
      if (split.length() == 0)
        continue;
      // The created Node has a name set to the full split attribute path
      addField(split, split, false, true);
    }
  }

  /**
   * Splits the path into a List of segments and calls build() to
   * construct a tree of Nodes representing path segments. The resulting
   * tree structure ends up describing all the paths we are interested in.
   *
   * @param path        The path expression for this field
   * @param fieldName   The name for this field in the emitted record
   * @param multiValued If 'true' then the emitted record will have values in
   *                    a List&lt;String&gt;
   * @param isRecord    Flags that this PATH is from a forEach statement
   */
  private void addField(String path, String fieldName, boolean multiValued, boolean isRecord) {
    if (!path.startsWith("/")) throw new RuntimeException("All paths must start with '/' " + path);
    List<String> paths = splitEscapeQuote(path);
    if (paths.size() == 0) {
      if (isRecord) rootNode.setAsRecord();
      return;//the path is "/"
    }
    // deal with how split behaves when separator starts with an empty string!
    if ("".equals(paths.get(0).trim()))
      paths.remove(0);
    rootNode.build(paths, fieldName, multiValued, isRecord, path);
    rootNode.buildOptimize();
  }

  /**
   * Uses {@link #streamRecords streamRecords} to getInst the JSON source but with
   * a handler that collects all the emitted records into a single List which
   * is returned upon completion.
   *
   * @param r the stream reader
   * @return results a List of emitted records
   */
  @SuppressWarnings({"unchecked"})
  public List<Map<String, Object>> getAllRecords(Reader r) throws IOException {
    final List<Map<String, Object>> results = new ArrayList<>();
    // Deep copy is required here because the stream might hold on to the map
    streamRecords(r, (record, path) -> results.add(Utils.getDeepCopy(record, 2)));
    return results;
  }

  /**
   * Creates an JSONParser on top of whatever reader has been
   * configured. Then calls getInst() with a handler which is
   * invoked forEach record emitted.
   *
   * @param r       the stream reader
   * @param handler The callback instance
   */
  public void streamRecords(Reader r, Handler handler) throws IOException {
    streamRecords(new JSONParser(r), handler);
  }

  public void streamRecords(JSONParser parser, Handler handler) throws IOException {
    rootNode.parse(parser, handler,
        new LinkedHashMap<>());
  }


  /**
   * For each node/leaf in the Node tree there is one object of this class.
   * This tree of objects represents all the Paths we are interested in.
   * For each path segment of interest we create a node. In most cases the
   * node (branch) is rather basic , but for the final portion (leaf) of any
   * path we add more information to the Node. When parsing the JSON document
   * we step though this tree as we stream records from the reader. If the JSON
   * document departs from this tree we skip start tags till we are back on
   * the tree.
   */
  private static class Node {
    String name;      // generally: segment of the path represented by this Node
    String fieldName; // the fieldname in the emitted record (key of the map)
    String splitPath; // the full path from the forEach entity attribute
    final LinkedHashMap<String, Node> childNodes = new LinkedHashMap<>(); // List of immediate child Nodes of this node
    Node parent; // parent Node in the tree
    boolean isLeaf = false; // flag: store/emit streamed text for this node
    boolean isRecord = false; //flag: this Node starts a new record
    boolean isChildRecord = false;
    Node wildCardChild;
    Node recursiveWildCardChild;
    private boolean useFqn = false;


    public Node(String name, Node p) {
      // Create a basic Node, suitable for the mid portions of any path.
      // Node.pathName and Node.name are set to same value.
      this.name = name;
      parent = p;
    }

    public Node(String name, String fieldName) {
      // This is only called from build() when describing an attribute.
      this.name = name;               // a segment from the path
      this.fieldName = fieldName;     // name to store collected values against
    }

    void setAsRecord() {
      if (isMyChildARecord()) throw new RuntimeException(name + " has a parent node at my level or lower");
      isChildRecord = hasParentRecord();
      isRecord = true;
    }

    private boolean hasParentRecord() {
      return isRecord || parent != null && parent.hasParentRecord();
    }

    private boolean isMyChildARecord() {
      if (isRecord) return true;
      for (Node node : childNodes.values()) {
        if (node.isMyChildARecord()) return true;
      }
      return false;
    }


    /**
     * Walk the Node tree propagating any wild Descendant information to
     * child nodes.
     */
    private void buildOptimize() {
      if (parent != null && parent.recursiveWildCardChild != null && this.recursiveWildCardChild == null) {
        this.recursiveWildCardChild = parent.recursiveWildCardChild;
      }
      for (Node n : childNodes.values()) n.buildOptimize();
    }
    static final String WILDCARD_PATH = "*";
    static final String RECURSIVE_WILDCARD_PATH = "**";

    /**
     * Build a Node tree structure representing all paths of interest to us.
     * This must be done before parsing of the JSON stream starts. Each node
     * holds one portion of an path. Taking each path segment in turn this
     * method walks the Node tree  and finds where the new segment should be
     * inserted. It creates a Node representing a field's name, PATH and
     * some flags and inserts the Node into the Node tree.
     */
    private void build(
        List<String> paths,   // a List of segments from the split paths
        String fieldName,     // the fieldName assoc with this path
        boolean multiValued,  // flag if this fieldName is multiValued or not
        boolean record,       // is this path a record or a field
        String path) {
      // recursively walk the paths Lists adding new Nodes as required
      String segment = paths.remove(0); // shift out next path segment

      if (segment.length() < 1) throw new RuntimeException("all pieces in path must be non empty " + path);

      // does this "name" already exist as a child node.
      Node n = getOrAddNode(segment, childNodes);
      if (paths.isEmpty()) {
        // We have emptied paths, we are for the moment a leaf of the tree.
        // When parsing the actual input we have traversed to a position
        // where we actually have to do something. getOrAddNode() will
        // have created and returned a new minimal Node with name and
        // pathName already populated. We need to add more information.
        if (record) {
          //wild cards cannot be used in split
          assert !WILDCARD_PATH.equals(n.name);
          assert !RECURSIVE_WILDCARD_PATH.equals(n.name);
          // split attribute
          n.setAsRecord(); // flag: split attribute, prepare to emit rec
          n.splitPath = fieldName; // the full split attribute path
        } else {
          if (n.name.equals(WILDCARD_PATH)) {
            wildCardChild = n;
          }
          if (n.name.equals(RECURSIVE_WILDCARD_PATH)) {
            recursiveWildCardChild = n.recursiveWildCardChild = n;
          }

          // path with content we want to store and return
          n.isLeaf = true;        // we have to store text found here
          n.fieldName = fieldName; // name to store collected text against
          if ("$FQN".equals(n.fieldName)) {
            n.fieldName = null;
            n.useFqn = true;
          }
        }
      } else {
        //wildcards must only come at the end
        if (WILDCARD_PATH.equals(name) || RECURSIVE_WILDCARD_PATH.equals(name))
          throw new RuntimeException("wild cards are allowed only in the end " + path);
        // recurse to handle next paths segment
        n.build(paths, fieldName, multiValued, record, path);
      }
    }

    private Node getOrAddNode(String pathName, Map<String, Node> children) {
      Node n = children.get(pathName);
      if (n != null) return n;
      // new territory! add a new node for this path bitty
      children.put(pathName, n = new Node(pathName, this));
      return n;
    }

    private void parse(JSONParser parser,
                       Handler handler,
                       Map<String, Object> values) throws IOException {

      int event = -1;
      boolean recordStarted = false;
      for (; ; ) {
        event = parser.nextEvent();
        if (event == EOF) break;
        if (event == OBJECT_START) {
          handleObjectStart(parser, handler, new LinkedHashMap<>(), new Stack<>(), recordStarted, null);
        } else if (event == ARRAY_START) {
          for (; ; ) {
            event = parser.nextEvent();
            if (event == ARRAY_END) break;
            if (event == OBJECT_START) {
              handleObjectStart(parser, handler, new LinkedHashMap<>(), new Stack<>(), recordStarted, null);
            }
          }
        }
      }

    }

    /**
     * If a new tag is encountered, check if it is of interest or not by seeing
     * if it matches against our node tree. If we have departed from the node
     * tree then walk back though the tree's ancestor nodes checking to see if
     * any // expressions exist for the node and compare them against the new
     * tag. If matched then "jump" to that node, otherwise ignore the tag.
     * <p>
     * Note, the list of // expressions found while walking back up the tree
     * is cached in the HashMap descendants. Then if the new tag is to be skipped,
     * any inner child tags are compared against the cache and jumped to if
     * matched.
     */
    private void handleObjectStart(final JSONParser parser,
                                   final Handler handler, final Map<String, Object> values,
                                   final Stack<Set<String>> stack, boolean recordStarted,
                                   MethodFrameWrapper frameWrapper)
        throws IOException {

      final boolean isRecordStarted = recordStarted || isRecord;
      Set<String> valuesAddedinThisFrame = null;
      if (isRecord || !recordStarted) {
        // This Node is a match for an PATH from a forEach attribute,
        // prepare for the clean up that will occur when the record
        // is emitted after its END_ELEMENT is matched
        valuesAddedinThisFrame = new HashSet<>();
        stack.push(valuesAddedinThisFrame);
      } else if (recordStarted) {
        // This node is a child of some parent which matched against forEach
        // attribute. Continue to add values to an existing record.
        valuesAddedinThisFrame = stack.peek();
      }

      class Wrapper extends MethodFrameWrapper {
        Wrapper(Node node, MethodFrameWrapper parent, String name) {
          this.node = node;
          this.parent = parent;
          this.name = name;
        }

        @Override
        @SuppressWarnings({"unchecked"})
        public void walk(int event) throws IOException {
          if (event == OBJECT_START) {
            walkObject();
          } else if (event == ARRAY_START) {
            for (; ; ) {
              event = parser.nextEvent();
              if (event == ARRAY_END) {
                // ensure that the value is of type List
                final Object val = values.get(name);
                if (val != null && !(val instanceof List)) {
                  @SuppressWarnings({"rawtypes"})
                  final ArrayList listVal = new ArrayList(1);
                  listVal.add(val);
                  values.put(name, listVal);
                }
                break;
              }
              if (event == OBJECT_START) {
                walkObject();
              }
            }
          }
        }

        void walkObject() throws IOException {
          if (node.isChildRecord) {
            node.handleObjectStart(parser,
                (record, path) -> addChildDoc2ParentDoc(record, values, getPathSuffix(path)),
                new LinkedHashMap<>(),
                new Stack<>(),
                true,
                this
            );
          } else {
            node.handleObjectStart(parser, handler, values, stack, isRecordStarted, this);
          }
        }
      }

      try {
        for (; ; ) {
          int event = parser.nextEvent();
          if (event == OBJECT_END) {
            if (isRecord()) {
              handler.handle(values, splitPath);
            }
            return;
          }
          assert event == STRING;
          assert parser.wasKey();
          String name = parser.getString();

          Node node = childNodes.get(name);
          if (node == null) node = wildCardChild;
          if (node == null) node = recursiveWildCardChild;

          if (node != null) {
            if (node.isLeaf) {//this is a leaf. Collect data here
              event = parser.nextEvent();
              String nameInRecord = node.fieldName == null ? getNameInRecord(name, frameWrapper, node) : node.fieldName;
              MethodFrameWrapper runnable = null;
              if (event == OBJECT_START || event == ARRAY_START) {
                if (node.recursiveWildCardChild != null) runnable = new Wrapper(node, frameWrapper, name);
              }
              Object val = parseSingleFieldValue(event, parser, runnable);
              if (val != null) {
                putValue(values, nameInRecord, val);
                valuesAddedinThisFrame.add(nameInRecord);
              }

            } else {
              event = parser.nextEvent();
              new Wrapper(node, frameWrapper, name).walk(event);
            }
          } else {
            //this is not something we are interested in. Skip it
            event = parser.nextEvent();
            if (event == STRING ||
                event == LONG ||
                event == NUMBER ||
                event == BIGNUMBER ||
                event == BOOLEAN ||
                event == NULL) {
              continue;
            }
            if (event == ARRAY_START) {
              consumeTillMatchingEnd(parser, 0, 1);
              continue;
            }
            if (event == OBJECT_START) {
              consumeTillMatchingEnd(parser, 1, 0);
              continue;
            } else throw new RuntimeException("unexpected token " + event);

          }
        }
      } finally {
        if ((isRecord() || !isRecordStarted)) {
          for (String fld : valuesAddedinThisFrame) {
            values.remove(fld);
          }
        }
      }
    }

    @SuppressWarnings({"unchecked"})
    private void addChildDoc2ParentDoc(Map<String, Object> record, Map<String, Object> values, String key) {
      record =  Utils.getDeepCopy(record, 2);
      Object oldVal = values.get(key);
      if (oldVal == null) {
        values.put(key, record);
      } else if (oldVal instanceof List) {
        ((List) oldVal).add(record);
      } else {
        @SuppressWarnings({"rawtypes"})
        ArrayList l = new ArrayList();
        l.add(oldVal);
        l.add(record);
        values.put(key, l);
      }
    }

    /**
     * Construct the name as it would appear in the final record
     */
    private String getNameInRecord(String name, MethodFrameWrapper frameWrapper, Node n) {
      if (frameWrapper == null || !n.useFqn || frameWrapper.node.isChildRecord) return name;
      StringBuilder sb = new StringBuilder();
      frameWrapper.addName(sb);
      return sb.append(DELIM).append(name).toString();
    }

    private boolean isRecord() {
      return isRecord;
    }


    @SuppressWarnings({"unchecked"})
    private void putValue(Map<String, Object> values, String fieldName, Object o) {
      if (o == null) return;
      Object val = values.get(fieldName);
      if (val == null) {
        values.put(fieldName, o);
        return;
      }
      if (val instanceof List) {
        @SuppressWarnings({"rawtypes"})
        List list = (List) val;
        list.add(o);
        return;
      }
      @SuppressWarnings({"rawtypes"})
      ArrayList l = new ArrayList();
      l.add(val);
      l.add(o);
      values.put(fieldName, l);
    }

    // returns the last key of the path
    private String getPathSuffix(String path) {
      int indexOf = path.lastIndexOf("/");
      if (indexOf == -1) return path;
      return path.substring(indexOf + 1);
    }

    @Override
    public String toString() {
      return name;
    }
  } // end of class Node


  /**
   * The path is split into segments using the '/' as a separator. However
   * this method deals with special cases where there is a slash '/' character
   * inside the attribute value e.g. x/@html='text/html'. We split by '/' but
   * then reassemble things were the '/' appears within a quoted sub-string.
   * <p>
   * We have already enforced that the string must begin with a separator. This
   * method depends heavily on how split behaves if the string starts with the
   * separator or if a sequence of multiple separators appear.
   */
  private static List<String> splitEscapeQuote(String str) {
    List<String> result = new LinkedList<>();
    String[] ss = str.split("/");
    for (int i = 0; i < ss.length; i++) { // i=1: skip separator at start of string
      StringBuilder sb = new StringBuilder();
      int quoteCount = 0;
      while (true) {
        sb.append(ss[i]);
        for (int j = 0; j < ss[i].length(); j++)
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


  /**
   * Implement this interface to stream records as and when one is found.
   */
  public interface Handler {
    /**
     * @param record The record map. The key is the field name as provided in
     *               the addField() methods. The value can be a single String (for single
     *               valued fields) or a List&lt;String&gt; (for multiValued).
     *               This map is mutable. DO NOT alter the map or store it for later use.
     *               If it must be stored, make a deep copy before doing so
     * @param path   The forEach path for which this record is being emitted
     *               If there is any change all parsing will be aborted and the Exception
     *               is propagated up
     */
    void handle(Map<String, Object> record, String path);
  }

  public static Object parseSingleFieldValue(int ev, JSONParser parser, MethodFrameWrapper runnable) throws IOException {
    switch (ev) {
      case STRING:
        return parser.getString();
      case LONG:
        return parser.getLong();
      case NUMBER:
        return parser.getDouble();
      case BIGNUMBER:
        return parser.getNumberChars().toString();
      case BOOLEAN:
        return parser.getBoolean();
      case NULL:
        parser.getNull();
        return null;
      case ARRAY_START:
        return parseArrayFieldValue(ev, parser, runnable);
      case OBJECT_START:
        if (runnable != null) {
          runnable.walk(OBJECT_START);
          return null;
        }
        consumeTillMatchingEnd(parser, 1, 0);
        return null;
      default:
        throw new RuntimeException("Error parsing JSON field value. Unexpected " + JSONParser.getEventString(ev));
    }
  }

  static abstract class MethodFrameWrapper {
    Node node;
    MethodFrameWrapper parent;
    String name;

    void addName(StringBuilder sb) {
      if (parent != null && !parent.node.isChildRecord) {
        parent.addName(sb);
        sb.append(DELIM);
      }
      sb.append(name);
    }

    public abstract void walk(int event) throws IOException;
  }

  @SuppressWarnings({"unchecked"})
  public static List<Object> parseArrayFieldValue(int ev, JSONParser parser, MethodFrameWrapper runnable) throws IOException {
    assert ev == ARRAY_START;

    @SuppressWarnings({"rawtypes"})
    ArrayList lst = new ArrayList(2);
    for (; ; ) {
      ev = parser.nextEvent();
      if (ev == ARRAY_END) {
        if (lst.isEmpty()) return null;
        return lst;
      }
      Object val = parseSingleFieldValue(ev, parser, runnable);
      if (val != null) lst.add(val);
    }
  }

  public static void consumeTillMatchingEnd(JSONParser parser, int obj, int arr) throws IOException {
    for (; ; ) {
      int event = parser.nextEvent();
      if (event == OBJECT_START) obj++;
      if (event == OBJECT_END) obj--;
      assert obj >= 0;
      if (event == ARRAY_START) arr++;
      if (event == ARRAY_END) arr--;
      assert arr >= 0;
      if (obj == 0 && arr == 0) break;
    }
  }

}

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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SpecProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkOperation;
import org.apache.solr.common.params.CommonParams;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.noggit.CharArr;
import org.noggit.JSONParser;
import org.noggit.JSONWriter;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;

public class Utils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static Map getDeepCopy(Map map, int maxDepth) {
    return getDeepCopy(map, maxDepth, true, false);
  }

  public static Map getDeepCopy(Map map, int maxDepth, boolean mutable) {
    return getDeepCopy(map, maxDepth, mutable, false);
  }

  public static Map getDeepCopy(Map map, int maxDepth, boolean mutable, boolean sorted) {
    if(map == null) return null;
    if (maxDepth < 1) return map;
    Map copy;
    if (sorted) {
      copy = new TreeMap();
    } else {
      copy = new LinkedHashMap();
    }
    for (Object o : map.entrySet()) {
      Map.Entry e = (Map.Entry) o;
      copy.put(e.getKey(), makeDeepCopy(e.getValue(),maxDepth, mutable, sorted));
    }
    return mutable ? copy : Collections.unmodifiableMap(copy);
  }

  private static Object makeDeepCopy(Object v, int maxDepth, boolean mutable, boolean sorted) {
    if (v instanceof MapWriter && maxDepth > 1) {
      v = ((MapWriter) v).toMap(new LinkedHashMap<>());
    } else if (v instanceof IteratorWriter && maxDepth > 1) {
      v = ((IteratorWriter) v).toList(new ArrayList<>());
      if (sorted) {
        Collections.sort((List)v);
      }
    }

    if (v instanceof Map) {
      v = getDeepCopy((Map) v, maxDepth - 1, mutable, sorted);
    } else if (v instanceof Collection) {
      v = getDeepCopy((Collection) v, maxDepth - 1, mutable, sorted);
    }
    return v;
  }

  public static InputStream toJavabin(Object o) throws IOException {
    try (final JavaBinCodec jbc = new JavaBinCodec()) {
      BinaryRequestWriter.BAOS baos = new BinaryRequestWriter.BAOS();
      jbc.marshal(o,baos);
      return new ByteBufferInputStream(ByteBuffer.wrap(baos.getbuf(),0,baos.size()));
    }
  }

  public static Collection getDeepCopy(Collection c, int maxDepth, boolean mutable) {
    return getDeepCopy(c, maxDepth, mutable, false);
  }

  public static Collection getDeepCopy(Collection c, int maxDepth, boolean mutable, boolean sorted) {
    if (c == null || maxDepth < 1) return c;
    Collection result = c instanceof Set ?
        ( sorted? new TreeSet() : new HashSet()) : new ArrayList();
    for (Object o : c) result.add(makeDeepCopy(o, maxDepth, mutable, sorted));
    if (sorted && (result instanceof List)) {
      Collections.sort((List)result);
    }
    return mutable ? result : result instanceof Set ? unmodifiableSet((Set) result) : unmodifiableList((List) result);
  }

  public static byte[] toJSON(Object o) {
    if(o == null) return new byte[0];
    CharArr out = new CharArr();
    if (!(o instanceof List) && !(o instanceof Map)) {
      if (o instanceof MapWriter)  {
        o = ((MapWriter)o).toMap(new LinkedHashMap<>());
      } else if(o instanceof IteratorWriter){
        o = ((IteratorWriter)o).toList(new ArrayList<>());
      }
    }
    new JSONWriter(out, 2).write(o); // indentation by default
    return toUTF8(out);
  }

  public static String toJSONString(Object o) {
    return new String(toJSON(o), StandardCharsets.UTF_8);
  }

  public static byte[] toUTF8(CharArr out) {
    byte[] arr = new byte[out.size() * 3];
    int nBytes = ByteUtils.UTF16toUTF8(out, 0, out.size(), arr, 0);
    return Arrays.copyOf(arr, nBytes);
  }

  public static Object fromJSON(byte[] utf8) {
    // convert directly from bytes to chars
    // and parse directly from that instead of going through
    // intermediate strings or readers
    CharArr chars = new CharArr();
    ByteUtils.UTF8toUTF16(utf8, 0, utf8.length, chars);
    JSONParser parser = new JSONParser(chars.getArray(), chars.getStart(), chars.length());
    parser.setFlags(parser.getFlags() |
        JSONParser.ALLOW_MISSING_COLON_COMMA_BEFORE_OBJECT |
        JSONParser.OPTIONAL_OUTER_BRACES);
    try {
      return ObjectBuilder.getVal(parser);
    } catch (IOException e) {
      throw new RuntimeException(e); // should never happen w/o using real IO
    }
  }

  public static Map<String, Object> makeMap(Object... keyVals) {
    return makeMap(false, keyVals);
  }

  public static Map<String, Object> makeMap(boolean skipNulls, Object... keyVals) {
    if ((keyVals.length & 0x01) != 0) {
      throw new IllegalArgumentException("arguments should be key,value");
    }
    Map<String, Object> propMap = new LinkedHashMap<>(keyVals.length >> 1);
    for (int i = 0; i < keyVals.length; i += 2) {
      Object keyVal = keyVals[i + 1];
      if (skipNulls && keyVal == null) continue;
      propMap.put(keyVals[i].toString(), keyVal);
    }
    return propMap;
  }

  public static Object fromJSON(InputStream is){
    try {
      return new ObjectBuilder(getJSONParser((new InputStreamReader(is, StandardCharsets.UTF_8)))).getObject();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Parse error", e);
    }
  }

  public static Object fromJSONResource(String resourceName){
   return fromJSON(Utils.class.getClassLoader().getResourceAsStream(resourceName));

  }
  public static JSONParser getJSONParser(Reader reader){
    JSONParser parser = new JSONParser(reader);
    parser.setFlags(parser.getFlags() |
        JSONParser.ALLOW_MISSING_COLON_COMMA_BEFORE_OBJECT |
        JSONParser.OPTIONAL_OUTER_BRACES);
    return parser;
  }

  public static Object fromJSONString(String json)  {
    try {
      return new ObjectBuilder(getJSONParser(new StringReader(json))).getObject();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Parse error", e);
    }
  }

  public static Object getObjectByPath(Object root, boolean onlyPrimitive, String hierarchy) {
    List<String> parts = StrUtils.splitSmart(hierarchy, '/');
    if (parts.get(0).isEmpty()) parts.remove(0);
    return getObjectByPath(root, onlyPrimitive, parts);
  }

  public static boolean setObjectByPath(Object root, String hierarchy, Object value) {
    List<String> parts = StrUtils.splitSmart(hierarchy, '/');
    if (parts.get(0).isEmpty()) parts.remove(0);
    return setObjectByPath(root, parts, value);
  }

  public static boolean setObjectByPath(Object root, List<String> hierarchy, Object value) {
    if (root == null) return false;
    if (!isMapLike(root)) throw new RuntimeException("must be a Map or NamedList");
    Object obj = root;
    for (int i = 0; i < hierarchy.size(); i++) {
      int idx = -2; //-1 means append to list, -2 means not found
      String s = hierarchy.get(i);
      if (s.endsWith("]")) {
        Matcher matcher = ARRAY_ELEMENT_INDEX.matcher(s);
        if (matcher.find()) {
          s = matcher.group(1);
          idx = Integer.parseInt(matcher.group(2));
        }
      }
      if (i < hierarchy.size() - 1) {
        Object o = getVal(obj, s);
        if (o == null) return false;
        if (idx > -1) {
          List l = (List) o;
          o = idx < l.size() ? l.get(idx) : null;
        }
        if (!isMapLike(o)) return false;
        obj = o;
      } else {
        if (idx == -2) {
          if (obj instanceof NamedList) {
            NamedList namedList = (NamedList) obj;
            int location = namedList.indexOf(s, 0);
            if (location == -1) namedList.add(s, value);
            else namedList.setVal(location, value);
          } else if (obj instanceof Map) {
            ((Map) obj).put(s, value);
          }
          return true;
        } else {
          Object v = getVal(obj, s);
          if (v instanceof List) {
            List list = (List) v;
            if (idx == -1) {
              list.add(value);
            } else {
              if (idx < list.size()) list.set(idx, value);
              else return false;
            }
            return true;
          } else {
            return false;
          }
        }
      }
    }

    return false;

  }


  public static Object getObjectByPath(Object root, boolean onlyPrimitive, List<String> hierarchy) {
    if(root == null) return null;
    if(!isMapLike(root)) return null;
    Object obj = root;
    for (int i = 0; i < hierarchy.size(); i++) {
      int idx = -1;
      String s = hierarchy.get(i);
      if (s.endsWith("]")) {
        Matcher matcher = ARRAY_ELEMENT_INDEX.matcher(s);
        if (matcher.find()) {
          s = matcher.group(1);
          idx = Integer.parseInt(matcher.group(2));
        }
      }
      if (i < hierarchy.size() - 1) {
        Object o = getVal(obj, s);
        if (o == null) return null;
        if (idx > -1) {
          List l = (List) o;
          o = idx < l.size() ? l.get(idx) : null;
        }
        if (!isMapLike(o)) return null;
        obj = o;
      } else {
        Object val = getVal(obj, s);
        if (val == null) return null;
        if (idx > -1) {
          List l = (List) val;
          val = idx < l.size() ? l.get(idx) : null;
        }
        if (onlyPrimitive && isMapLike(val)) {
          return null;
        }
        return val;
      }
    }

    return false;
  }

  private static boolean isMapLike(Object o) {
    return o instanceof Map || o instanceof NamedList;
  }

  private static Object getVal(Object obj, String key) {
    if(obj instanceof NamedList) return ((NamedList) obj).get(key);
    else if (obj instanceof Map) return ((Map) obj).get(key);
    else throw new RuntimeException("must be a NamedList or Map");
  }

  /**
   * If the passed entity has content, make sure it is fully
   * read and closed.
   * 
   * @param entity to consume or null
   */
  public static void consumeFully(HttpEntity entity) {
    if (entity != null) {
      try {
        // make sure the stream is full read
        readFully(entity.getContent());
      } catch (UnsupportedOperationException e) {
        // nothing to do then
      } catch (IOException e) {
        // quiet
      } finally {
        // close the stream
        EntityUtils.consumeQuietly(entity);
      }
    }
  }

  /**
   * Make sure the InputStream is fully read.
   * 
   * @param is to read
   * @throws IOException on problem with IO
   */
  private static void readFully(InputStream is) throws IOException {
    is.skip(is.available());
    while (is.read() != -1) {}
  }

  /**
   * Assumes data in ZooKeeper is a JSON string, deserializes it and returns as a Map
   *
   * @param zkClient the zookeeper client
   * @param path the path to the znode being read
   * @param retryOnConnLoss whether to retry the operation automatically on connection loss, see {@link org.apache.solr.common.cloud.ZkCmdExecutor#retryOperation(ZkOperation)}
   * @return a Map if the node exists and contains valid JSON or an empty map if znode does not exist or has a null data
   */
  public static Map<String, Object> getJson(SolrZkClient zkClient, String path, boolean retryOnConnLoss) throws KeeperException, InterruptedException {
    try {
      byte[] bytes = zkClient.getData(path, null, null, retryOnConnLoss);
      if (bytes != null && bytes.length > 0) {
        return (Map<String, Object>) Utils.fromJSON(bytes);
      }
    } catch (KeeperException.NoNodeException e) {
      return Collections.emptyMap();
    }
    return Collections.emptyMap();
  }

  public static final Pattern ARRAY_ELEMENT_INDEX = Pattern
      .compile("(\\S*?)\\[([-]?\\d+)\\]");

  public static SpecProvider getSpec(final String name) {
    return () -> {
      return ValidatingJsonMap.parse(CommonParams.APISPEC_LOCATION + name + ".json", CommonParams.APISPEC_LOCATION);
    };
  }
}

package org.apache.solr.common.util;

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


import java.io.IOException;
import java.io.StringReader;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.noggit.CharArr;
import org.noggit.JSONParser;
import org.noggit.JSONWriter;
import org.noggit.ObjectBuilder;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;

public class Utils {
  public static Map getDeepCopy(Map map, int maxDepth) {
    return getDeepCopy(map, maxDepth, true);
  }

  public static Map getDeepCopy(Map map, int maxDepth, boolean mutable) {
    if(map == null) return null;
    if (maxDepth < 1) return map;
    Map copy = new LinkedHashMap();
    for (Object o : map.entrySet()) {
      Map.Entry e = (Map.Entry) o;
      Object v = e.getValue();
      if (v instanceof Map) v = getDeepCopy((Map) v, maxDepth - 1, mutable);
      else if (v instanceof Collection) v = getDeepCopy((Collection) v, maxDepth - 1, mutable);
      copy.put(e.getKey(), v);
    }
    return mutable ? copy : Collections.unmodifiableMap(copy);
  }

  public static Collection getDeepCopy(Collection c, int maxDepth, boolean mutable) {
    if (c == null || maxDepth < 1) return c;
    Collection result = c instanceof Set ? new HashSet() : new ArrayList();
    for (Object o : c) {
      if (o instanceof Map) {
        o = getDeepCopy((Map) o, maxDepth - 1, mutable);
      }
      result.add(o);
    }
    return mutable ? result : result instanceof Set ? unmodifiableSet((Set) result) : unmodifiableList((List) result);
  }

  public static byte[] toJSON(Object o) {
    CharArr out = new CharArr();
    new JSONWriter(out, 2).write(o); // indentation by default
    return toUTF8(out);
  }

  public static String toJSONString(Object o) {
    return new String(toJSON(o), StandardCharsets.UTF_8);
  }

  public static byte[] toUTF8(CharArr out) {
    byte[] arr = new byte[out.size() << 2]; // is 4x the real worst-case upper-bound?
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
    try {
      return ObjectBuilder.getVal(parser);
    } catch (IOException e) {
      throw new RuntimeException(e); // should never happen w/o using real IO
    }
  }

  public static Map<String, Object> makeMap(Object... keyVals) {
    if ((keyVals.length & 0x01) != 0) {
      throw new IllegalArgumentException("arguments should be key,value");
    }
    Map<String, Object> propMap = new LinkedHashMap<>(keyVals.length >> 1);
    for (int i = 0; i < keyVals.length; i += 2) {
      propMap.put(keyVals[i].toString(), keyVals[i + 1]);
    }
    return propMap;
  }

  public static Object fromJSONString(String json)  {
    try {
      return new ObjectBuilder(new JSONParser(new StringReader(
          json))).getObject();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Parse error", e);
    }
  }

  public static Object getObjectByPath(Map root, boolean onlyPrimitive, String hierarchy) {
    return getObjectByPath(root, onlyPrimitive, StrUtils.splitSmart(hierarchy, '/'));
  }

  public static Object getObjectByPath(Map root, boolean onlyPrimitive, List<String> hierarchy) {
    Map obj = root;
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
        Object o = obj.get(s);
        if (o == null) return null;
        if (idx > -1) {
          List l = (List) o;
          o = idx < l.size() ? l.get(idx) : null;
        }
        if (!(o instanceof Map)) return null;
        obj = (Map) o;
      } else {
        Object val = obj.get(s);
        if (idx > -1) {
          List l = (List) val;
          val = idx < l.size() ? l.get(idx) : null;
        }
        if (onlyPrimitive && val instanceof Map) {
          return null;
        }
        return val;
      }
    }

    return false;
  }

  public static final Pattern ARRAY_ELEMENT_INDEX = Pattern
      .compile("(\\S*?)\\[(\\d+)\\]");
}

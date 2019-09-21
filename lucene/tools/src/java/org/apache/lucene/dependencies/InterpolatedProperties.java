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
package org.apache.lucene.dependencies;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Parse a properties file, performing recursive Ant-like
 * property value interpolation, and return the resulting Properties.
 */
public class InterpolatedProperties extends Properties {
  private static final Pattern PROPERTY_REFERENCE_PATTERN = Pattern.compile("\\$\\{(?<name>[^}]+)\\}");

  /**
   * Loads the properties file via {@link Properties#load(InputStream)},
   * then performs recursive Ant-like property value interpolation.
   */
  @Override
  public void load(InputStream inStream) throws IOException {
    throw new UnsupportedOperationException("InterpolatedProperties.load(InputStream) is not supported.");
  }

  /**
   * Loads the properties file via {@link Properties#load(Reader)},
   * then performs recursive Ant-like property value interpolation.
   */
  @Override
  public void load(Reader reader) throws IOException {
    Properties p = new Properties();
    p.load(reader);

    LinkedHashMap<String, String> props = new LinkedHashMap<>();
    Enumeration<?> e = p.propertyNames();
    while (e.hasMoreElements()) {
      String key = (String) e.nextElement();
      props.put(key, p.getProperty(key));
    }

    resolve(props).forEach((k, v) -> this.setProperty(k, v));
  }

  private static Map<String,String> resolve(Map<String,String> props) {
    LinkedHashMap<String, String> resolved = new LinkedHashMap<>();
    HashSet<String> recursive = new HashSet<>();
    props.forEach((k, v) -> {
      resolve(props, resolved, recursive, k, v);
    });
    return resolved;
  }

  private static String resolve(Map<String,String> props,
                               LinkedHashMap<String, String> resolved,
                               Set<String> recursive,
                               String key,
                               String value) {
    if (value == null) {
      throw new IllegalArgumentException("Missing replaced property key: " + key);
    }

    if (recursive.contains(key)) {
      throw new IllegalArgumentException("Circular recursive property resolution: " + recursive);
    }

    if (!resolved.containsKey(key)) {
      recursive.add(key);
      StringBuffer buffer = new StringBuffer();
      Matcher matcher = PROPERTY_REFERENCE_PATTERN.matcher(value);
      while (matcher.find()) {
        String referenced = matcher.group("name");
        String concrete = resolve(props, resolved, recursive, referenced, props.get(referenced));
        matcher.appendReplacement(buffer, Matcher.quoteReplacement(concrete));
      }
      matcher.appendTail(buffer);
      resolved.put(key, buffer.toString());
      recursive.remove(key);
    }
    assert resolved.get(key).equals(value);
    return resolved.get(key);
  }

  public static void main(String [] args) {
    {
      Map<String, String> props = new LinkedHashMap<>();
      props.put("a", "${b}");
      props.put("b", "${c}");
      props.put("c", "foo");
      props.put("d", "${a}/${b}/${c}");
      assertEquals(resolve(props), "a=foo", "b=foo", "c=foo", "d=foo/foo/foo");
    }

    {
      Map<String, String> props = new LinkedHashMap<>();
      props.put("a", "foo");
      props.put("b", "${a}");
      assertEquals(resolve(props), "a=foo", "b=foo");
    }

    {
      Map<String, String> props = new LinkedHashMap<>();
      props.put("a", "${b}");
      props.put("b", "${c}");
      props.put("c", "${a}");
      try {
        resolve(props);
      } catch (IllegalArgumentException e) {
        // Expected, circular reference.
        if (!e.getMessage().contains("Circular recursive")) {
          throw new AssertionError();
        }
      }
    }

    {
      Map<String, String> props = new LinkedHashMap<>();
      props.put("a", "${b}");
      try {
        resolve(props);
      } catch (IllegalArgumentException e) {
        // Expected, no referenced value.
        if (!e.getMessage().contains("Missing replaced")) {
          throw new AssertionError();
        }
      }
    }
  }

  private static void assertEquals(Map<String,String> resolved, String... keyValuePairs) {
    List<String> result = resolved.entrySet().stream().sorted((a, b) -> a.getKey().compareTo(b.getKey()))
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(Collectors.toList());
    if (!result.equals(Arrays.asList(keyValuePairs))) {
      throw new AssertionError("Mismatch: \n" + result + "\nExpected: " + Arrays.asList(keyValuePairs));
    }
  }
}

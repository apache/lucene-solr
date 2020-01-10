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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.SolrTestCaseJ4;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.solr.api.ApiBag.HANDLER_NAME;

public class TestPathTrie extends SolrTestCaseJ4 {

  public void testPathTrie() {
    PathTrie<String> pathTrie = new PathTrie<>(ImmutableSet.of("_introspect"));
    pathTrie.insert("/", emptyMap(), "R");
    pathTrie.insert("/aa", emptyMap(), "d");
    pathTrie.insert("/aa/bb/{cc}/dd", emptyMap(), "a");
    pathTrie.insert("/$handlerName/{cc}/dd", singletonMap(HANDLER_NAME, "test"), "test");
    pathTrie.insert("/aa/bb/{cc}/{xx}", emptyMap(), "b");
    pathTrie.insert("/aa/bb", emptyMap(), "c");

    HashMap templateValues = new HashMap<>();
    assertEquals("R", pathTrie.lookup("/", templateValues, null));
    assertEquals("d", pathTrie.lookup("/aa", templateValues, null));
    assertEquals("a", pathTrie.lookup("/aa/bb/hello/dd", templateValues, null));
    templateValues.clear();
    assertEquals("test", pathTrie.lookup("/test/hello/dd", templateValues, null));
    assertEquals("hello", templateValues.get("cc"));
    templateValues.clear();
    assertEquals("b", pathTrie.lookup("/aa/bb/hello/world", templateValues, null));
    assertEquals("hello", templateValues.get("cc"));
    assertEquals("world", templateValues.get("xx"));
    Set<String> subPaths =  new HashSet<>();
    templateValues.clear();
    pathTrie.lookup("/aa",templateValues, subPaths);
    assertEquals(3, subPaths.size());

    pathTrie = new PathTrie<>(ImmutableSet.of("_introspect"));
    pathTrie.insert("/aa/bb/{cc}/tt/*", emptyMap(), "W");

    templateValues.clear();
    assertEquals("W" ,pathTrie.lookup("/aa/bb/somepart/tt/hello", templateValues));
    assertEquals(templateValues.get("*"), "/hello");

    templateValues.clear();
    assertEquals("W" ,pathTrie.lookup("/aa/bb/somepart/tt", templateValues));
    assertEquals(templateValues.get("*"), null);

    templateValues.clear();
    assertEquals("W" ,pathTrie.lookup("/aa/bb/somepart/tt/hello/world/from/solr", templateValues));
    assertEquals(templateValues.get("*"), "/hello/world/from/solr");
  }
}

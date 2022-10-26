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
package org.apache.solr.common.cloud;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;

/** Unit test for {@link Aliases} */
public class AliasesTest extends SolrTestCase {

  public void testCloneWithRenameNullBefore() {
    Aliases aliases = Aliases.EMPTY;
    SolrException e =
        assertThrows(SolrException.class, () -> aliases.cloneWithRename(null, "alias1"));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
  }

  public void testCloneWithRenameEmptyBefore() {
    Aliases aliases = Aliases.EMPTY;
    SolrException e =
        assertThrows(SolrException.class, () -> aliases.cloneWithRename("", "alias1"));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
  }

  public void testCloneWithRenameEmptyAfter() {
    Aliases aliases = Aliases.EMPTY;
    SolrException e =
        assertThrows(SolrException.class, () -> aliases.cloneWithRename("alias0", ""));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
  }

  public void testCloneWithRenameBeforeEqAfter() {
    Aliases aliases = Aliases.EMPTY;
    Aliases same = aliases.cloneWithRename("same", "same");
    assertEquals(same, aliases);
  }

  public void testCloneWithRenameNewAlias() {
    Map<String, List<String>> collectionAliases = new HashMap<>();
    Map<String, Map<String, String>> collectionAliasProperties = new HashMap<>();
    Aliases aliases = new Aliases(collectionAliases, collectionAliasProperties, -1);
    Aliases renamed = aliases.cloneWithRename("col0", "alias0");
    Map<String, String> expected = Collections.singletonMap("alias0", "col0");
    assertEquals(expected, renamed.getCollectionAliasMap());
  }

  public void testCloneWithRename() {
    Map<String, List<String>> collectionAliases = new HashMap<>();
    collectionAliases.put("alias0", Collections.singletonList("col0"));
    collectionAliases.put("alias5", Arrays.asList("col5", "col0", "col7"));
    Map<String, Map<String, String>> collectionAliasProperties = Collections.singletonMap("col0", Collections.singletonMap("p0", "v0"));
    Aliases aliases = new Aliases(collectionAliases, collectionAliasProperties, -1);
    Aliases renamed = aliases.cloneWithRename("col0", "alias1");
    Map<String, String> expected = new HashMap<>();
    expected.put("alias0", "alias1");
    expected.put("alias1", "col0");
    expected.put("alias5", "col5,alias1,col7");
    assertEquals(expected, renamed.getCollectionAliasMap());
    assertEquals(new HashMap<>(), renamed.getCollectionAliasProperties("col0"));
    assertEquals(Collections.singletonMap("p0", "v0"), renamed.getCollectionAliasProperties("alias1"));
  }

  public void testCloneWithCollectionAliasNullAlias() {
    Aliases aliases = Aliases.EMPTY;
    SolrException e =
        assertThrows(SolrException.class, () -> aliases.cloneWithCollectionAlias(null, "col"));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
  }

  public void testCloneWithCollectionAlias() {
    Map<String, List<String>> collectionAliases = new HashMap<>();
    collectionAliases.put("alias0", Collections.singletonList("col0"));
    collectionAliases.put("alias5", Arrays.asList("col5", "col0", "col7"));
    Map<String, Map<String, String>> collectionAliasProperties =
        Collections.singletonMap("alias0", Collections.singletonMap("p0", "v0"));
    Aliases aliases = new Aliases(collectionAliases, collectionAliasProperties, -1);
    Aliases cloned = aliases.cloneWithCollectionAlias("alias7", "col9");

    Map<String, String> expected = new HashMap<>();
    expected.put("alias0", "col0");
    expected.put("alias5", "col5,col0,col7");
    expected.put("alias7", "col9");
    assertEquals(expected, cloned.getCollectionAliasMap());
    assertEquals(Collections.singletonMap("p0", "v0"), cloned.getCollectionAliasProperties("alias0"));
    assertEquals(new HashMap<>(), cloned.getCollectionAliasProperties("alias7"));
  }

  public void testCloneWithCollectionAliasRemoval() {
    Map<String, List<String>> collectionAliases = new HashMap<>();
    collectionAliases.put("alias0", Collections.singletonList("col0"));
    collectionAliases.put("alias5", Arrays.asList("col5", "col0", "col7"));
    Map<String, Map<String, String>> collectionAliasProperties =
        Collections.singletonMap("alias0", Collections.singletonMap("p0", "v0"));
    Aliases aliases = new Aliases(collectionAliases, collectionAliasProperties, -1);
    Aliases cloned = aliases.cloneWithCollectionAlias("alias0", null);

    Map<String, String> expected = Collections.singletonMap("alias5", "col5,col0,col7");
    assertEquals(expected, cloned.getCollectionAliasMap());
    assertEquals(new HashMap<>(), cloned.getCollectionAliasProperties("alias0"));
  }
}

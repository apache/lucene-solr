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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

/**
 * Holds collection aliases -- virtual collections that point to one or more other collections.
 * We might add other types of aliases here some day.
 * Immutable.
 */
public class Aliases {

  public static final Aliases EMPTY = new Aliases(Collections.emptyMap());

  /** Map of "collection" string constant to ->
   *   alias name -> comma delimited list of collections */
  private final Map<String,Map<String,String>> aliasMap; // not-null

  private final Map<String, List<String>> collectionAliasListMap; // not-null; computed from aliasMap

  public static Aliases fromJSON(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return EMPTY;
    }
    return new Aliases((Map<String,Map<String,String>>) Utils.fromJSON(bytes));
  }

  private Aliases(Map<String, Map<String,String>> aliasMap) {
    this.aliasMap = aliasMap;
    collectionAliasListMap = convertMapOfCommaDelimitedToMapOfList(getCollectionAliasMap());
  }

  public static Map<String, List<String>> convertMapOfCommaDelimitedToMapOfList(Map<String, String> collectionAliasMap) {
    Map<String, List<String>> collectionAliasListMap = new LinkedHashMap<>(collectionAliasMap.size());
    for (Map.Entry<String, String> entry : collectionAliasMap.entrySet()) {
      collectionAliasListMap.put(entry.getKey(), StrUtils.splitSmart(entry.getValue(), ",", true));
    }
    return collectionAliasListMap;
  }

  /**
   * Returns an unmodifiable Map of collection aliases mapped to a comma delimited list of what the alias maps to.
   * Does not return null.
   * Prefer use of {@link #getCollectionAliasListMap()} instead, where appropriate.
   */
  public Map<String,String> getCollectionAliasMap() {
    Map<String,String> cam = aliasMap.get("collection");
    return cam == null ? Collections.emptyMap() : Collections.unmodifiableMap(cam);
  }

  /**
   * Returns an unmodifiable Map of collection aliases mapped to a list of what the alias maps to.
   * Does not return null.
   */
  public Map<String,List<String>> getCollectionAliasListMap() {
    return Collections.unmodifiableMap(collectionAliasListMap);
  }
  
  public boolean hasCollectionAliases() {
    return !collectionAliasListMap.isEmpty();
  }

  /**
   * Returns a list of collections that the input alias name maps to. If there
   * are none, the input is returned. One level of alias indirection is supported (alias to alias to collection).
   * Treat the result as unmodifiable.
   */
  public List<String> resolveAliases(String aliasName) {
    return resolveAliasesGivenAliasMap(collectionAliasListMap, aliasName);
  }

  /** @lucene.internal */
  public static List<String> resolveAliasesGivenAliasMap(Map<String, List<String>> collectionAliasListMap, String aliasName) {
    //return collectionAliasListMap.getOrDefault(aliasName, Collections.singletonList(aliasName));
    // TODO deprecate and remove this dubious feature?
    // Due to another level of indirection, this is more complicated...
    List<String> level1 = collectionAliasListMap.get(aliasName);
    if (level1 == null) {
      return Collections.singletonList(aliasName);// is a collection
    }
    List<String> result = new ArrayList<>(level1.size());
    for (String level1Alias : level1) {
      List<String> level2 = collectionAliasListMap.get(level1Alias);
      if (level2 == null) {
        result.add(level1Alias);
      } else {
        result.addAll(level2);
      }
    }
    return result;
  }

  /**
   * Creates a new Aliases instance with the same data as the current one but with a modification based on the
   * parameters. If {@code collections} is null, then the {@code alias} is removed, otherwise it is added/updated.
   */
  public Aliases cloneWithCollectionAlias(String alias, String collections) {
    Map<String,String> newCollectionMap = new HashMap<>(getCollectionAliasMap());
    if (collections == null) {
      newCollectionMap.remove(alias);
    } else {
      newCollectionMap.put(alias, collections);
    }
    if (newCollectionMap.isEmpty()) {
      return EMPTY;
    } else {
      return new Aliases(Collections.singletonMap("collection", newCollectionMap));
    }
  }

  /** Serialize to ZooKeeper. */
  public byte[] toJSON() {
    if (collectionAliasListMap.isEmpty()) {
      return null;
    } else {
      return Utils.toJSON(aliasMap);
    }
  }

  @Override
  public String toString() {
    return "Aliases [aliasMap=" + aliasMap + "]";
  }
}

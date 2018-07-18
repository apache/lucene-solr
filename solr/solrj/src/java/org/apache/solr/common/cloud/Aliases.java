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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

/**
 * Holds collection aliases -- virtual collections that point to one or more other collections.
 * We might add other types of aliases here some day.
 * Immutable.
 */
public class Aliases {

  /**
   * An empty, minimal Aliases primarily used to support the non-cloud solr use cases. Not normally useful
   * in cloud situations where the version of the node needs to be tracked even if all aliases are removed.
   * The -1 version makes it subordinate to any real version, and furthermore we never "set" this EMPTY instance
   * into ZK.
   */
  public static final Aliases EMPTY = new Aliases(Collections.emptyMap(), Collections.emptyMap(), -1);

  // These two constants correspond to the top level elements in aliases.json. The first one denotes
  // a section containing a list of aliases and their attendant collections, the second contains a list of
  // aliases and their attendant properties (metadata) They probably should be
  // named "aliases" and "alias_properties" but for back compat reasons, we cannot change them
  private static final String COLLECTION = "collection";
  private static final String COLLECTION_METADATA = "collection_metadata";

  // aliasName -> list of collections.  (note: the Lists here should be unmodifiable)
  private final Map<String, List<String>> collectionAliases; // not null

  // aliasName --> propertiesKey --> propertiesValue (note: the inner Map here should be unmodifiable)
  private final Map<String, Map<String, String>> collectionAliasProperties; // notnull

  private final int zNodeVersion;

  /** Construct aliases directly with this information -- caller should not retain.
   * Any deeply nested collections are assumed to already be unmodifiable. */
  private Aliases(Map<String, List<String>> collectionAliases,
                 Map<String, Map<String, String>> collectionAliasProperties,
                 int zNodeVersion) {
    this.collectionAliases = Objects.requireNonNull(collectionAliases);
    this.collectionAliasProperties = Objects.requireNonNull(collectionAliasProperties);
    this.zNodeVersion = zNodeVersion;
  }

  /**
   * Create an instance from the JSON bytes read from zookeeper. Generally this should
   * only be done by a ZkStateReader.
   *
   * @param bytes The bytes read via a getData request to zookeeper (possibly null)
   * @param zNodeVersion the version of the data in zookeeper that this instance corresponds to
   * @return A new immutable Aliases object
   */
  @SuppressWarnings("unchecked")
  public static Aliases fromJSON(byte[] bytes, int zNodeVersion) {
    Map<String, Map> aliasMap;
    if (bytes == null || bytes.length == 0) {
      aliasMap = Collections.emptyMap();
    } else {
      aliasMap = (Map<String, Map>) Utils.fromJSON(bytes);
    }

    Map colAliases = aliasMap.getOrDefault(COLLECTION, Collections.emptyMap());
    colAliases = convertMapOfCommaDelimitedToMapOfList(colAliases); // also unmodifiable

    Map<String, Map<String, String>> colMeta = aliasMap.getOrDefault(COLLECTION_METADATA, Collections.emptyMap());
    colMeta.replaceAll((k, metaMap) -> Collections.unmodifiableMap(metaMap));

    return new Aliases(colAliases, colMeta, zNodeVersion);
  }

  /**
   * Serialize our state.
   */
  public byte[] toJSON() {
    if (collectionAliases.isEmpty()) {
      assert collectionAliasProperties.isEmpty();
      return null;
    } else {
      Map<String,Map> tmp = new LinkedHashMap<>();
      tmp.put(COLLECTION, convertMapOfListToMapOfCommaDelimited(collectionAliases));
      if (!collectionAliasProperties.isEmpty()) {
        tmp.put(COLLECTION_METADATA, collectionAliasProperties);
      }
      return Utils.toJSON(tmp);
    }
  }

  public static Map<String, List<String>> convertMapOfCommaDelimitedToMapOfList(Map<String, String> collectionAliasMap) {
    return collectionAliasMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> splitCollections(e.getValue()),
            (a, b) -> {throw new IllegalStateException(String.format(Locale.ROOT, "Duplicate key %s", b));},
            LinkedHashMap::new));
  }

  private static List<String> splitCollections(String collections) {
    return Collections.unmodifiableList(StrUtils.splitSmart(collections, ",", true));
  }

  public static Map<String,String> convertMapOfListToMapOfCommaDelimited(Map<String,List<String>> collectionAliasMap) {
    return collectionAliasMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> String.join(",", e.getValue()),
            (a, b) -> {throw new IllegalStateException(String.format(Locale.ROOT, "Duplicate key %s", b));},
            LinkedHashMap::new));
  }

  public int getZNodeVersion() {
    return zNodeVersion;
  }

  /**
   * Get a map similar to the JSON data as stored in zookeeper. Callers may prefer use of
   * {@link #getCollectionAliasListMap()} instead, if collection names will be iterated.
   *
   * @return an unmodifiable Map of collection aliases mapped to a comma delimited string of the collection(s) the
   * alias maps to. Does not return null.
   */
  @SuppressWarnings("unchecked")
  public Map<String, String> getCollectionAliasMap() {
    return Collections.unmodifiableMap(convertMapOfListToMapOfCommaDelimited(collectionAliases));
  }

  /**
   * Get a fully parsed map of collection aliases.
   *
   * @return an unmodifiable Map of collection aliases mapped to a list of the collection(s) the alias maps to.
   * Does not return null.
   */
  public Map<String,List<String>> getCollectionAliasListMap() {
    // Note: Lists contained by this map are already unmodifiable and can be shared safely
    return Collections.unmodifiableMap(collectionAliases);
  }

  /**
   * Returns an unmodifiable Map of properties for a given alias. If an alias by the given name
   * exists, this method will never return null.
   *
   * @param alias the name of an alias also found as a key in {@link #getCollectionAliasListMap()}
   * @return The properties for the alias (possibly empty) or null if the alias does not exist.
   */
  public Map<String,String> getCollectionAliasProperties(String alias) {
    // Note: map is already unmodifiable; it can be shared safely
    return collectionAliasProperties.getOrDefault(alias, Collections.emptyMap());
  }

  /**
   * List the collections associated with a particular alias. One level of alias indirection is supported
   * (alias to alias to collection). Such indirection may be deprecated in the future, use with caution.
   *
   * @return  An unmodifiable list of collections names that the input alias name maps to. If there
   * are none, the input is returned.
   */
  public List<String> resolveAliases(String aliasName) {
    return resolveAliasesGivenAliasMap(collectionAliases, aliasName);
  }

  /** @lucene.internal */
  @SuppressWarnings("JavaDoc")
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
    return Collections.unmodifiableList(result);
  }

  /**
   * Creates a new Aliases instance with the same data as the current one but with a modification based on the
   * parameters.
   * <p>
   * Note that the state in zookeeper is unaffected by this method and the change must still be persisted via
   * {@link ZkStateReader.AliasesManager#applyModificationAndExportToZk(UnaryOperator)}
   *
   * @param alias       the alias to update, must not be null
   * @param collections the comma separated list of collections for the alias, null to remove the alias
   */
  public Aliases cloneWithCollectionAlias(String alias, String collections) {
    if (alias == null) {
      throw new NullPointerException("Alias name cannot be null");
    }
    Map<String, Map<String, String>> newColProperties;
    Map<String, List<String>> newColAliases = new LinkedHashMap<>(this.collectionAliases);//clone to modify
    if (collections == null) { // REMOVE:
      newColProperties = new LinkedHashMap<>(this.collectionAliasProperties);//clone to modify
      newColProperties.remove(alias);
      newColAliases.remove(alias);
    } else {
      newColProperties = this.collectionAliasProperties;// no changes
      // java representation is a list, so split before adding to maintain consistency
      newColAliases.put(alias, splitCollections(collections)); // note: unmodifiableList
    }
    return new Aliases(newColAliases, newColProperties, zNodeVersion);
  }

  /**
   * Set the value for some properties on a collection alias. This is done by creating a new Aliases instance
   * with the same data as the current one but with a modification based on the parameters.
   * <p>
   * Note that the state in zookeeper is unaffected by this method and the change must still be persisted via
   * {@link ZkStateReader.AliasesManager#applyModificationAndExportToZk(UnaryOperator)}
   *
   * @param alias the alias to update
   * @param propertiesKey the key for the properties
   * @param propertiesValue the properties to add/replace, null to remove the key.
   * @return An immutable copy of the aliases with the new properties.
   */
  public Aliases cloneWithCollectionAliasProperties(String alias, String propertiesKey, String propertiesValue) {
    return cloneWithCollectionAliasProperties(alias, Collections.singletonMap(propertiesKey,propertiesValue));
  }

  /**
   * Set the values for some properties keys on a collection alias. This is done by creating a new Aliases instance
   * with the same data as the current one but with a modification based on the parameters.
   * <p>
   * Note that the state in zookeeper is unaffected by this method and the change must still be persisted via
   * {@link ZkStateReader.AliasesManager#applyModificationAndExportToZk(UnaryOperator)}
   *
   * @param alias the alias to update
   * @param properties the properties to add/replace, null values in the map will remove the key.
   * @return An immutable copy of the aliases with the new properties.
   */
  public Aliases cloneWithCollectionAliasProperties(String alias, Map<String,String> properties) {
    if (!collectionAliases.containsKey(alias)) {
      throw new IllegalArgumentException(alias + " is not a valid alias");
    }
    if (properties == null) {
      throw new IllegalArgumentException("Null is not a valid properties map");
    }
    Map<String,Map<String,String>> newColProperties = new LinkedHashMap<>(this.collectionAliasProperties);//clone to modify
    Map<String, String> newMetaMap = new LinkedHashMap<>(newColProperties.getOrDefault(alias, Collections.emptyMap()));
    for (Map.Entry<String, String> metaEntry : properties.entrySet()) {
      if (metaEntry.getValue() != null) {
        newMetaMap.put(metaEntry.getKey(), metaEntry.getValue());
      } else {
        newMetaMap.remove(metaEntry.getKey());
      }
    }
    newColProperties.put(alias, Collections.unmodifiableMap(newMetaMap));
    return new Aliases(collectionAliases, newColProperties, zNodeVersion);
  }

  @Override
  public String toString() {
    return "Aliases{" +
        "collectionAliases=" + collectionAliases +
        ", collectionAliasProperties=" + collectionAliasProperties +
        ", zNodeVersion=" + zNodeVersion +
        '}';
  }
}

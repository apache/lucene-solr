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

package org.apache.solr.cloud.api.collections;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.update.AddUpdateCommand;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.common.params.CollectionAdminParams.ROUTER_PREFIX;

public interface RoutedAlias {

  /**
   * Types supported. Every entry here must have a case in the switch statement in {@link #fromProps(String, Map)}
   */
  enum SupportedRouterTypes {
    TIME,
    CATEGORY
  }

  String ROUTER_TYPE_NAME = ROUTER_PREFIX + "name";
  String ROUTER_FIELD = ROUTER_PREFIX + "field";
  String CREATE_COLLECTION_PREFIX = "create-collection.";
  Set<String> MINIMAL_REQUIRED_PARAMS = Sets.newHashSet(ROUTER_TYPE_NAME, ROUTER_FIELD);
  String ROUTED_ALIAS_NAME_CORE_PROP = "routedAliasName"; // core prop

  static SolrException newAliasMustExistException(String aliasName) {
    throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
        "Routed alias " + aliasName + " appears to have been removed during request processing.");
  }

  /**
   * Factory method for implementations of this interface. There should be no reason to construct instances
   * elsewhere, and routed alias types are encouraged to have package private constructors.
   *
   * @param aliasName The alias name (will be returned by {@link #getAliasName()}
   * @param props     The properties from an overseer message.
   * @return An implementation appropriate for the supplied properties, or null if no type is specified.
   * @throws SolrException If the properties are invalid or the router type is unknown.
   */
  static RoutedAlias fromProps(String aliasName, Map<String, String> props) throws SolrException {

    String typeStr = props.get(ROUTER_TYPE_NAME);
    if (typeStr == null) {
      return null; // non-routed aliases are being created
    }
    SupportedRouterTypes routerType;
    try {
       routerType = SupportedRouterTypes.valueOf(typeStr.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new SolrException(BAD_REQUEST, "Router name: " + typeStr + " is not in supported types, "
          + Arrays.asList(SupportedRouterTypes.values()));
    }
    switch (routerType) {
      case TIME:
        return new TimeRoutedAlias(aliasName, props);
      case CATEGORY:
        return new CategoryRoutedAlias(aliasName, props);
      default:
        // if we got a type not handled by the switch there's been a bogus implementation.
        throw new SolrException(SERVER_ERROR, "Router " + routerType + " is not fully implemented. If you see this" +
            "error in an official release please file a bug report. Available types were:"
            + Arrays.asList(SupportedRouterTypes.values()));

    }
  }

  /**
   * Ensure our parsed version of the alias collection list is up to date. If it was modified, return true.
   * Note that this will return true if some other alias was modified or if properties were modified. These
   * are spurious and the caller should be written to be tolerant of no material changes.
   */
  boolean updateParsedCollectionAliases(ZkController zkController);

  /**
   * Create the initial collection for this RoutedAlias if applicable.
   *
   * Routed Aliases do not aggregate existing collections, instead they create collections on the fly. If the initial
   * collection can be determined from initialization parameters it should be calculated here.
   *
   * @return optional string of initial collection name
   */
  String computeInitialCollectionName();


  /**
   * The name of the alias. This name is used in place of a collection name for both queries and updates.
   *
   * @return The name of the Alias.
   */
  String getAliasName();

  String getRouteField();



  /**
   * Check that the value we will be routing on is legal for this type of routed alias.
   *
   * @param cmd the command containing the document
   */
  void validateRouteValue(AddUpdateCommand cmd) throws SolrException;

  /**
   * Create any required collections and return the name of the collection to which the current document should be sent.
   *
   * @param cmd The command that might cause collection creation
   * @return The name of the proper destination collection for the document which may or may not be a
   * newly created collection
   */
  String createCollectionsIfRequired(AddUpdateCommand cmd);

  /**
   * @return get alias related metadata
   */
  Map<String, String> getAliasMetadata();

  Set<String> getRequiredParams();

  Set<String> getOptionalParams();

}

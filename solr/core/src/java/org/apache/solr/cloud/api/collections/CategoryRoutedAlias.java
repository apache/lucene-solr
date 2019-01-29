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

import java.lang.invoke.MethodHandles;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CategoryRoutedAlias implements RoutedAlias<String> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String COLLECTION_INFIX = "__CRA__";

  /**
   * Parameters required for creating a category routed alias
   */
  public static final Set<String> REQUIRED_ROUTER_PARAMS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      CommonParams.NAME,
      ROUTER_TYPE_NAME,
      ROUTER_FIELD)));

  public static final String ROUTER_MAX_CARDINALITY = "router.maxCardinality";
  public static final String ROUTER_MUST_MATCH = "router.mustMatch";

  /**
   * Optional parameters for creating a category routed alias excluding parameters for collection creation.
   */
  public static final Set<String> OPTIONAL_ROUTER_PARAMS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      ROUTER_MAX_CARDINALITY,
      ROUTER_MUST_MATCH)));

  private List<String> collectionNames; // List of collections currently in the CRA
  private Aliases parsedCollectionsAliases; // a cached reference to the source of what we parse into parsedCollectionsDesc
  private final String aliasName;
  private final Map<String, String> aliasMetadata;

  CategoryRoutedAlias(String aliasName, Map<String, String> aliasMetadata) {
    this.aliasName = aliasName;
    this.aliasMetadata = aliasMetadata;
  }

  @Override
  public boolean updateParsedCollectionAliases(ZkController zkController) {
    final Aliases aliases = zkController.getZkStateReader().getAliases(); // note: might be different from last request
    if (this.parsedCollectionsAliases != aliases) {
      if (this.parsedCollectionsAliases != null) {
        log.debug("Observing possibly updated alias: {}", getAliasName());
      }
      // slightly inefficient, but not easy to make changes to the return value of parseCollections
      this.collectionNames = parseCollections(aliases).stream().map(Map.Entry::getValue).collect(Collectors.toList());
      this.parsedCollectionsAliases = aliases;
      return true;
    }
    return false;
  }

  @Override
  public String getAliasName() {
    return aliasName;
  }

  @Override
  public String getRouteField() {
    return aliasMetadata.get(ROUTER_FIELD);
  }

  @Override
  public List<Map.Entry<String, String>> parseCollections(Aliases aliases) {
    final List<String> collections = aliases.getCollectionAliasListMap().get(aliasName);
    if (collections == null) {
      throw RoutedAlias.newAliasMustExistException(getAliasName());
    }
    List<Map.Entry<String,String>> result = new ArrayList<>(collections.size());
    for (String collection : collections) {
      String collCategory = parseCategoryFromCollectionName(aliasName, collection);
      result.add(new AbstractMap.SimpleImmutableEntry<>(collCategory, collection));
    }
    // TODO Think about this... is order needed? if so perhaps better if insertion maintains order?
    result.sort((e1, e2) -> e2.getKey().compareTo(e1.getKey())); // reverse sort by key

    // note that this is also sorted by value since the value corresponds to the key plus a the alias name which
    // is constant within a given alias.
    return result;
  }

  /**
   * Pattern for Category Routed Alias is aliasName__CRA__datadrivincategory. The __CRA__ infix is to
   * reduce the chance of inadvertently duplicating (or worse yet, adopting) other collections
   * that are not supposed to be included in the alias. With Time routed aliases the timestamp in
   * the collection name was sufficiently unique, but given that aliasName could be anything and
   * 2 part collection names of the form foo_bar are probably common in the wild, the infix seems
   * necessary.
   *
   */
  private String parseCategoryFromCollectionName(String aliasName, String collection) {
    String prefix = aliasName + COLLECTION_INFIX;
    if (!collection.startsWith(prefix)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Category Routed Alias collection names " +
          "must start with the name of the alias plus " + COLLECTION_INFIX);
    }
    return collection.substring(prefix.length(),collection.length());
  }

  @Override
  public void validateRouteValue(AddUpdateCommand cmd) throws SolrException {
    //Mostly this will be filled out by SOLR-13150 and SOLR-13151
  }

  /**
   * Calculate a safe collection name from a data value. Any non-word character is
   * replace with an underscore
   *
   * @param dataValue a value from the route field for a particular document
   * @return the suffix value for it's corresponding collection name.
   */
  private String safeKeyValue(String dataValue) {
    return dataValue.trim().replaceAll("\\W", "_");
  }

  private String buildCollectionNameFromValue(String value) {
    return aliasName + COLLECTION_INFIX + safeKeyValue(value);
  }

  @Override
  public String createCollectionsIfRequired(AddUpdateCommand cmd) {
    SolrQueryRequest req = cmd.getReq();
    SolrCore core = req.getCore();
    CoreContainer coreContainer = core.getCoreContainer();
    CollectionsHandler collectionsHandler = coreContainer.getCollectionsHandler();
    String dataValue = String.valueOf(cmd.getSolrInputDocument().getFieldValue(getRouteField()));

   String candidateCollectionName = buildCollectionNameFromValue(dataValue);

    try {
      // Note: CRA's have no way to predict values that determine collection so preemptive async creation
      // is not possible. We have no choice but to block and wait (to do otherwise would imperil the overseer).
      do {
        if (this.collectionNames.contains(candidateCollectionName)) {
          return candidateCollectionName;
        } else {
          // this could time out in which case we simply let it throw an error
          MaintainCategoryRoutedAliasCmd.remoteInvoke(collectionsHandler, getAliasName(), candidateCollectionName);
          // It's possible no collection was created because of a race and that's okay... we'll retry.

          // Ensure our view of the aliases has updated. If we didn't do this, our zkStateReader might
          //  not yet know about the new alias (thus won't see the newly added collection to it), and we might think
          //  we failed.
          collectionsHandler.getCoreContainer().getZkController().getZkStateReader().aliasesManager.update();

          // we should see some sort of update to our aliases
          if (!updateParsedCollectionAliases(coreContainer.getZkController())) { // thus we didn't make progress...
            // this is not expected, even in known failure cases, but we check just in case
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "We need to create a new category routed collection but for unknown reasons were unable to do so.");
          }
        }
      } while (true);
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public Optional<String> computeInitialCollectionName() {
    return Optional.empty();
  }

  @Override
  public Map<String, String> getAliasMetadata() {
    return aliasMetadata;
  }

  @Override
  public Set<String> getRequiredParams() {
    return REQUIRED_ROUTER_PARAMS;
  }

  @Override
  public Set<String> getOptionalParams() {
    return OPTIONAL_ROUTER_PARAMS;
  }
}

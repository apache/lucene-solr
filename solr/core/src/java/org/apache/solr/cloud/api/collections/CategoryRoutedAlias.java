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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.solr.client.solrj.RoutedAliasTypes;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.update.AddUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;

public class CategoryRoutedAlias extends RoutedAlias {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION_INFIX = "__CRA__";

  // This constant is terribly annoying but a great many things fall apart if we allow an alias with
  // no collections to be created. So this kludge seems better than reworking every request path that
  // expects a collection but also works with an alias to handle or error out on empty alias. The
  // collection with this constant as a suffix is automatically removed after the alias begins to
  // receive data.
  public static final String UNINITIALIZED = "NEW_CATEGORY_ROUTED_ALIAS_WAITING_FOR_DATA_TEMP";

  @SuppressWarnings("WeakerAccess")
  public static final String ROUTER_MAX_CARDINALITY = "router.maxCardinality";

  /**
   * Parameters required for creating a category routed alias
   */
  @SuppressWarnings("WeakerAccess")
  public static final Set<String> REQUIRED_ROUTER_PARAMS = Set.of(
      CommonParams.NAME,
      ROUTER_TYPE_NAME,
      ROUTER_FIELD,
      ROUTER_MAX_CARDINALITY
  );

  public static final String ROUTER_MUST_MATCH = "router.mustMatch";

  /**
   * Optional parameters for creating a category routed alias excluding parameters for collection creation.
   */
  @SuppressWarnings("WeakerAccess")
  public static final Set<String> OPTIONAL_ROUTER_PARAMS = Set.of(
      ROUTER_MAX_CARDINALITY,
      ROUTER_MUST_MATCH);

  private Aliases aliases;
  private final String aliasName;
  private final Map<String, String> aliasMetadata;
  private final Integer maxCardinality;
  private final Pattern mustMatch;

  CategoryRoutedAlias(String aliasName, Map<String, String> aliasMetadata) {
    this.aliasName = aliasName;
    this.aliasMetadata = aliasMetadata;
    this.maxCardinality = parseMaxCardinality(aliasMetadata.get(ROUTER_MAX_CARDINALITY));
    final String mustMatch = this.aliasMetadata.get(ROUTER_MUST_MATCH);
    this.mustMatch = mustMatch == null ? null : compileMustMatch(mustMatch);
  }

  @Override
  public boolean updateParsedCollectionAliases(ZkStateReader zkStateReader, boolean contextualize) {
    final Aliases aliases = zkStateReader.getAliases(); // note: might be different from last request
    if (this.aliases != aliases) {
      if (this.aliases != null) {
        log.debug("Observing possibly updated alias: {}", getAliasName());
      }
      // slightly inefficient, but not easy to make changes to the return value of parseCollections
      this.aliases = aliases;
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
  public RoutedAliasTypes getRoutedAliasType() {
    return RoutedAliasTypes.CATEGORY;
  }

  @Override
  public void validateRouteValue(AddUpdateCommand cmd) throws SolrException {
    if (this.aliases == null) {
      updateParsedCollectionAliases(cmd.getReq().getCore().getCoreContainer().getZkController().zkStateReader, false);
    }

    Object fieldValue = cmd.getSolrInputDocument().getFieldValue(getRouteField());
    // possible future enhancement: allow specification of an "unknown" category name to where we can send
    // docs that are uncategorized.
    if (fieldValue == null) {
      throw new SolrException(BAD_REQUEST, "Route value is null");
    }

    String dataValue = String.valueOf(fieldValue);

    String candidateCollectionName = buildCollectionNameFromValue(dataValue);
    List<String> cols = getCollectionList(this.aliases);

    if (cols.contains(candidateCollectionName)) {
      return;
    }

    // this check will become very important for future work
    int infix = candidateCollectionName.indexOf(COLLECTION_INFIX);
    int valueStart = infix + COLLECTION_INFIX.length();
    if (candidateCollectionName.substring(valueStart).contains(COLLECTION_INFIX)) {
      throw new SolrException(BAD_REQUEST, "No portion of the route value may resolve to the 7 character sequence " +
          "__CRA__");
    }

    if (mustMatch != null && !mustMatch.matcher(dataValue).matches()) {
      throw new SolrException(BAD_REQUEST, "Route value " + dataValue
          + " does not match " + ROUTER_MUST_MATCH + ": " + mustMatch);
    }

    if (cols.stream()
        .filter(x -> !x.contains(UNINITIALIZED)).count() >= maxCardinality) {
      throw new SolrException(BAD_REQUEST, "Max cardinality " + maxCardinality
          + " reached for Category Routed Alias: " + getAliasName());
    }
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

  String buildCollectionNameFromValue(String value) {
    return aliasName + COLLECTION_INFIX + safeKeyValue(value);
  }


  private Integer parseMaxCardinality(String maxCardinality) {
    try {
      return Integer.valueOf(maxCardinality);
    } catch (NumberFormatException e) {
      throw new SolrException(BAD_REQUEST, ROUTER_MAX_CARDINALITY + " must be a valid Integer"
          + ", instead got: " + maxCardinality);
    }
  }

  private Pattern compileMustMatch(String mustMatch) {
    try {
      return Pattern.compile(mustMatch);
    } catch (PatternSyntaxException e) {
      throw new SolrException(BAD_REQUEST, ROUTER_MUST_MATCH + " must be a valid regular"
          + " expression, instead got: " + mustMatch);
    }
  }

  @Override
  public String computeInitialCollectionName() {
    return buildCollectionNameFromValue(UNINITIALIZED);
  }

  @Override
  String[] formattedRouteValues(SolrInputDocument doc) {
    String routeField = getRouteField();
    String fieldValue = (String) doc.getFieldValue(routeField);
    return new String[] {safeKeyValue(fieldValue)};
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

  @Override
  public CandidateCollection findCandidateGivenValue(AddUpdateCommand cmd) {
    Object value = cmd.getSolrInputDocument().getFieldValue(getRouteField());
    String targetColName = buildCollectionNameFromValue(String.valueOf(value));
    ZkStateReader zkStateReader = cmd.getReq().getCore().getCoreContainer().getZkController().zkStateReader;
    updateParsedCollectionAliases(zkStateReader, true);
    List<String> collectionList = getCollectionList(this.aliases);
    if (collectionList.contains(targetColName)) {
      return new CandidateCollection(CreationType.NONE, targetColName);
    } else {
      return new CandidateCollection(CreationType.SYNCHRONOUS, targetColName);
    }
  }

  @Override
  protected String getHeadCollectionIfOrdered(AddUpdateCommand cmd) {
    return buildCollectionNameFromValue(String.valueOf(cmd.getSolrInputDocument().getFieldValue(getRouteField())));
  }

  @Override
  protected List<Action> calculateActions(String targetCol) {
    List<String> collectionList = getCollectionList(aliases);
    if (!collectionList.contains(targetCol)) {
      ArrayList<Action> actionList = new ArrayList<>();
      actionList.add(new Action(this,ActionType.ENSURE_EXISTS, targetCol));
      for (String s : collectionList) {
        // can't remove the uninitialized on the first pass otherwise there is a risk of momentarily having
        // an empty alias if thread scheduling plays tricks on us.
        if (s.contains(UNINITIALIZED) && collectionList.size() > 1) {
          actionList.add(new Action(this,ActionType.ENSURE_REMOVED, s));
        }
      }
      return  actionList;
    } else {
      return Collections.emptyList();
    }
  }


}
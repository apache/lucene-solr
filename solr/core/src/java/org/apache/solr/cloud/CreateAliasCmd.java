
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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.solr.cloud.OverseerCollectionMessageHandler.Cmd;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.update.processor.TimeRoutedAliasUpdateProcessor;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.TimeZoneUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.TZ;
import static org.apache.solr.update.processor.TimeRoutedAliasUpdateProcessor.DATE_TIME_FORMATTER;


public class CreateAliasCmd implements Cmd {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String START = "start"; //TODO, router related
  public static final String ROUTING_TYPE = "router.name";
  public static final String ROUTING_FIELD = "router.field";
  public static final String ROUTING_INCREMENT = "router.interval";
  public static final String ROUTING_MAX_FUTURE = "router.max-future-ms";

  public static final String CREATE_COLLECTION_PREFIX = "create-collection.";

  private final OverseerCollectionMessageHandler ocmh;

  /**
   * Parameters required for creating a routed alias
   */
  public static final List<String> REQUIRED_ROUTING_PARAMS = Collections.unmodifiableList(Arrays.asList(
          CommonParams.NAME,
          START,
          ROUTING_FIELD,
          ROUTING_TYPE,
          ROUTING_INCREMENT));

  /**
   * Optional parameters for creating a routed alias excluding parameters for collection creation.
   */
  public static final List<String> NONREQUIRED_ROUTING_PARAMS = Collections.unmodifiableList(Arrays.asList(
          ROUTING_MAX_FUTURE,
          TZ));

  private static Predicate<String> PARAM_IS_METADATA = key -> !key.equals(NAME) && !key.equals(START)
      && (REQUIRED_ROUTING_PARAMS.contains(key) || NONREQUIRED_ROUTING_PARAMS.contains(key)
      || key.startsWith(CREATE_COLLECTION_PREFIX));

  private static boolean anyRoutingParams(ZkNodeProps message) {
    return message.containsKey(ROUTING_FIELD) || message.containsKey(ROUTING_TYPE) || message.containsKey(START)
        || message.containsKey(ROUTING_INCREMENT) || message.containsKey(TZ);
  }

  public CreateAliasCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results)
      throws Exception {
    final String aliasName = message.getStr(CommonParams.NAME);
    ZkStateReader zkStateReader = ocmh.zkStateReader;
    ZkStateReader.AliasesManager holder = zkStateReader.aliasesHolder;

    //TODO refactor callCreatePlainAlias
    if (!anyRoutingParams(message)) {

      final List<String> canonicalCollectionList = parseCollectionsParameter(message.get("collections"));
      final String canonicalCollectionsString = StrUtils.join(canonicalCollectionList, ',');
      validateAllCollectionsExistAndNoDups(canonicalCollectionList, zkStateReader);
      holder.applyModificationAndExportToZk(aliases -> aliases.cloneWithCollectionAlias(aliasName, canonicalCollectionsString));

    } else { //TODO refactor callCreateRoutedAlias

      // Validate we got everything we need
      if (!message.getProperties().keySet().containsAll(REQUIRED_ROUTING_PARAMS)) {
        throw new SolrException(BAD_REQUEST, "A routed alias requires these params: " + REQUIRED_ROUTING_PARAMS
        + " plus some create-collection prefixed ones.");
      }

      Map<String, String> aliasMetadata = new LinkedHashMap<>();
      message.getProperties().entrySet().stream()
          .filter(entry -> PARAM_IS_METADATA.test(entry.getKey()))
          .forEach(entry -> aliasMetadata.put(entry.getKey(), (String) entry.getValue()));

      //TODO read these from metadata where appropriate. This leads to consistent logic between initial routed alias
      //  collection creation, and subsequent collections to be created.

      final String routingType = message.getStr(ROUTING_TYPE);
      final String tz = message.getStr(TZ);
      final String start = message.getStr(START);
      final String increment = message.getStr(ROUTING_INCREMENT);
      final String maxFutureMs = message.getStr(ROUTING_MAX_FUTURE);

      try {
        if (maxFutureMs != null && 0 > Long.parseLong(maxFutureMs)) {
          throw new NumberFormatException("Negative value not allowed here");
        }
      } catch (NumberFormatException e) {
        throw new SolrException(BAD_REQUEST, ROUTING_MAX_FUTURE + " must be a valid long integer representing a number " +
            "of milliseconds greater than or equal to zero");
      }

      if (!"time".equals(routingType)) {
        throw new SolrException(BAD_REQUEST, "Only time based routing is supported at this time");
      }

      // Check for invalid timezone
      TimeZone zone = TimeZoneUtils.parseTimezone(tz);

      // check that the increment is valid date math
      try {
        new DateMathParser(zone).parseMath(increment);
      } catch (ParseException e) {
        throw new SolrException(BAD_REQUEST,e.getMessage(),e);
      }

      Instant startTime = parseStart(start, zone);

      // It's too much work to check the routed field against the schema, there seems to be no good way to get
      // a copy of the schema aside from loading it directly from zookeeper based on the config name, but that
      // also requires I load solrconfig.xml to check what the value for managedSchemaResourceName is too, (or
      // discover that managed schema is not turned on and read schema.xml instead... and check for dynamic
      // field patterns too. As much as it would be nice to validate all inputs it's not worth the effort.

      String initialCollectionName = TimeRoutedAliasUpdateProcessor
          .formatCollectionNameFromInstant(aliasName, startTime, DATE_TIME_FORMATTER);

      // Create the collection
      NamedList createResults = new NamedList();
      RoutedAliasCreateCollectionCmd.createCollectionAndWait(state, createResults, aliasName, aliasMetadata, initialCollectionName, ocmh);
      validateAllCollectionsExistAndNoDups(Collections.singletonList(initialCollectionName), zkStateReader);

      // Create/update the alias
      holder.applyModificationAndExportToZk(aliases -> aliases
          .cloneWithCollectionAlias(aliasName, initialCollectionName)
          .cloneWithCollectionAliasMetadata(aliasName, aliasMetadata));
    }

    // Sleep a bit to allow ZooKeeper state propagation.
    //
    // THIS IS A KLUDGE.
    //
    // Solr's view of the cluster is eventually consistent. *Eventually* all nodes and CloudSolrClients will be aware of
    // alias changes, but not immediately. If a newly created alias is queried, things should work right away since Solr
    // will attempt to see if it needs to get the latest aliases when it can't otherwise resolve the name.  However
    // modifications to an alias will take some time.
    //
    // We could levy this requirement on the client but they would probably always add an obligatory sleep, which is
    // just kicking the can down the road.  Perhaps ideally at this juncture here we could somehow wait until all
    // Solr nodes in the cluster have the latest aliases?
    Thread.sleep(100);
  }

  private Instant parseStart(String str, TimeZone zone) {
    Instant start = DateMathParser.parseMath(new Date(), str, zone).toInstant();
    checkMilis(start);
    return start;
  }

  private void checkMilis(Instant date) {
    if (!date.truncatedTo(ChronoUnit.SECONDS).equals(date)) {
      throw new SolrException(BAD_REQUEST,
          "Date or date math for start time includes milliseconds, which is not supported. " +
              "(Hint: 'NOW' used without rounding always has this problem)");
    }
  }

  private void validateAllCollectionsExistAndNoDups(List<String> collectionList, ZkStateReader zkStateReader) {
    final String collectionStr = StrUtils.join(collectionList, ',');

    if (new HashSet<>(collectionList).size() != collectionList.size()) {
      throw new SolrException(BAD_REQUEST,
          String.format(Locale.ROOT,  "Can't create collection alias for collections='%s', since it contains duplicates", collectionStr));
    }
    ClusterState clusterState = zkStateReader.getClusterState();
    Set<String> aliasNames = zkStateReader.getAliases().getCollectionAliasListMap().keySet();
    for (String collection : collectionList) {
      if (clusterState.getCollectionOrNull(collection) == null && !aliasNames.contains(collection)) {
        throw new SolrException(BAD_REQUEST,
            String.format(Locale.ROOT,  "Can't create collection alias for collections='%s', '%s' is not an existing collection or alias", collectionStr, collection));
      }
    }
  }

  /**
   * The v2 API directs that the 'collections' parameter be provided as a JSON array (e.g. ["a", "b"]).  We also
   * maintain support for the legacy format, a comma-separated list (e.g. a,b).
   */
  @SuppressWarnings("unchecked")
  private List<String> parseCollectionsParameter(Object colls) {
    if (colls == null) throw new SolrException(BAD_REQUEST, "missing collections param");
    if (colls instanceof List) return (List<String>) colls;
    return StrUtils.splitSmart(colls.toString(), ",", true).stream()
        .map(String::trim)
        .collect(Collectors.toList());
  }

  private ZkNodeProps selectByPrefix(String prefix, ZkNodeProps source) {
    ZkNodeProps subSet = new ZkNodeProps();
    for (Map.Entry<String, Object> entry : source.getProperties().entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        subSet = subSet.plus(entry.getKey().substring(prefix.length()), entry.getValue());
      }
    }
    return subSet;
  }

}

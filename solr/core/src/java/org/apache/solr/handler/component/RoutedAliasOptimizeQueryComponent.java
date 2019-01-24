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

package org.apache.solr.handler.component;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


import com.google.common.collect.Sets;
import org.apache.commons.collections.iterators.ReverseListIterator;
import org.apache.solr.cloud.api.collections.TimeRoutedAlias;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

public class RoutedAliasOptimizeQueryComponent extends SearchComponent {
  public static final String COMPONENT_NAME = "routedAlias";
  public static final String ALIAS = "alias";
  private static final Set<String> knownRouters = Collections.singleton("time"); // currently only supports TRA

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    // only works in SolrCloud
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    // only works in SolrCloud
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    if (rb.stage == ResponseBuilder.STAGE_START) {
      SolrQueryRequest req = rb.req;
      String path = req.getHttpSolrCall().getReq().getPathInfo();
      final String aliasName = path.substring(1, path.indexOf('/', 1));
      Aliases allAliases = rb.req.getCore().getCoreContainer().getZkController().zkStateReader.getAliases();
      Map<String, String> aliasConf = allAliases.getCollectionAliasProperties(aliasName);
      if (!knownRouters.contains(aliasConf.get(TimeRoutedAlias.ROUTER_TYPE_NAME))) {
        // alias is not routed by a supported URP
        return ResponseBuilder.STAGE_DONE;
      }

      if(rb.getSortSpec() == null || rb.getSortSpec().getSchemaFields().size() == 0) {
        // no sort was specified
        return ResponseBuilder.STAGE_DONE;
      }

      // main sort field must equal router.field
      if (!rb.getSortSpec().getSort().getSort()[0].getField().equals(aliasConf.get(TimeRoutedAlias.ROUTER_FIELD))) {
        // query is not sorted by TRA router field
        return ResponseBuilder.STAGE_DONE;
      }
      ModifiableSolrParams newParams = new ModifiableSolrParams(req.getParams())
          .set(ALIAS, aliasName);
      req.setParams(newParams);

      return ResponseBuilder.STAGE_GET_FIELDS;
    }

    if (rb.stage < ResponseBuilder.STAGE_GET_FIELDS && rb.req.getParams().get(ALIAS) != null) {
      // was already set up
      return ResponseBuilder.STAGE_GET_FIELDS;
    }

    // not querying an alias
    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if(rb.stage == ResponseBuilder.STAGE_EXECUTE_QUERY) {
      SolrParams params = rb.req.getParams();
      final String aliasName = params.get(ALIAS);
      if (aliasName == null) {
        // no alias was detected
        return;
      }
      final boolean debug = rb.isDebug();
      Aliases allAliases = rb.req.getCore().getCoreContainer().getZkController().getZkStateReader().getAliases();
      Map<String, String> aliasConf = allAliases.getCollectionAliasProperties(aliasName);
      if (aliasConf.size() == 0) {
        // no conf was found
        return;
      }
      TimeRoutedAlias alias = createTimeRouteAlias(aliasName, aliasConf);

      final int rows = Integer.valueOf(params.get("rows", "10"));
      if( rb.getResponseDocs().getNumFound() <= rows) {
        // rows was not reached, so there is no need to optimize this query.
        if (debug) {
          rb.getDebugInfo().add(COMPONENT_NAME, "limit was not reached, no filtering was required");
        }
        return;
      }

      List<Map.Entry<Instant, String>> sortedColls = alias.parseCollections(allAliases,
          () -> new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Alias must exist: " + alias.getAliasName()));

      final boolean isDescending = rb.getSortSpec().getSort().getSort()[0].getReverse();

      long docs = 0;
      List<List<String>> sortedShards = groupShardsBySortedCollection(rb.shards, sortedColls);
      Set<String> shardsWhiteList = new HashSet<>();
      Iterator<List<String>> CollectionIter = isDescending? sortedShards.iterator(): new ReverseListIterator(sortedShards); // get iterator by sort
      while (CollectionIter.hasNext()) {
        List<String> collectionShards = CollectionIter.next();
        for (String shardReq : collectionShards) {
          docs += rb.finished.stream()
              .map(x -> x.responses)
              .mapToLong(r -> r.stream()
                  .filter(x -> shardReq.equals(x.getShard()))
                  .mapToLong(shardResponse ->
                      ((SolrDocumentList) shardResponse.getSolrResponse().getResponse().get("response"))
                          .getNumFound()
                  ).sum()
              ).sum();
          shardsWhiteList.add(shardReq);
        }
        if (docs >= rows) break; // found enough docs, no need to check remaining collections
      }

      if (debug) {
        rb.getDebugInfo().add(COMPONENT_NAME, "dropping all requests but requests for shards: " +
            String.join(", ", shardsWhiteList)
        );
      }

      rb.finished
          .forEach(req ->
            req.responses = req.responses.stream()
          .filter(x -> shardsWhiteList.contains(x.getShard()))
              .collect(Collectors.toList())
      );
    }
  }

  @Override
  public String getDescription() {
    return "Routed Alias Query Optimizer";
  }

  private TimeRoutedAlias createTimeRouteAlias(String aliasName, Map<String, String> aliasConf) {
    return new TimeRoutedAlias(aliasName, aliasConf);
  }

  /**
   *
   * @param sortedCollections - list of collections in TRA sorted by date.
   * @return Collection of lists of shards. Top level list is sorted by collection date.
   */
  private List<List<String>> groupShardsBySortedCollection(String[] shards, List<Map.Entry<Instant, String>> sortedCollections) {
    return sortedCollections.stream()
        .map(Map.Entry::getValue)
        .map(x -> Arrays.stream(shards)
            .filter(y -> y.contains(x))
            .collect(Collectors.toCollection(LinkedList::new))
        )
        .collect(Collectors.toCollection(LinkedList::new));
  }
}

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
package org.apache.solr.search.join;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.request.SolrQueryRequest;

abstract class BlockJoinFacetComponentSupport extends SearchComponent {
  public static final String CHILD_FACET_FIELD_PARAMETER = "child.facet.field";
  public static final String NO_TO_PARENT_BJQ_MESSAGE = "Block join faceting is allowed with ToParentBlockJoinQuery only";
  public static final String COLLECTOR_CONTEXT_PARAM = "blockJoinFacetCollector";

  protected void validateQuery(Query query) {
    if (!(query instanceof ToParentBlockJoinQuery)) {
      if (query instanceof BooleanQuery) {
        List<BooleanClause> clauses = ((BooleanQuery) query).clauses();
        for (BooleanClause clause : clauses) {
          if (clause.getQuery() instanceof ToParentBlockJoinQuery) {
            return;
          }
        }
      }
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, NO_TO_PARENT_BJQ_MESSAGE);
    }
  }

  static String[] getChildFacetFields(SolrQueryRequest req) {
    return req.getParams().getParams(CHILD_FACET_FIELD_PARAMETER);
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (getChildFacetFields(rb.req) != null) {
      BlockJoinFacetAccsHolder blockJoinFacetCollector = (BlockJoinFacetAccsHolder) rb.req.getContext().get(COLLECTOR_CONTEXT_PARAM);
      assert blockJoinFacetCollector != null;
      NamedList output;
      if (isShard(rb)) {
        // distributed search, put results into own cell in order not to clash with facet component
        output = getChildFacetFields(rb.rsp.getValues(), true);
      } else {
        // normal process, put results into standard response
        output = getFacetFieldsList(rb);
      }
      mergeFacets(output, blockJoinFacetCollector.getFacets());
    }
  }

  private boolean isShard(ResponseBuilder rb) {
    return "true".equals(rb.req.getParams().get(ShardParams.IS_SHARD));
  }

  private NamedList getChildFacetFields(NamedList responseValues, boolean createIfAbsent) {
    return getNamedListFromList(responseValues, "child_facet_fields", createIfAbsent);
  }

  private void mergeFacets(NamedList childFacetFields, NamedList shardFacets) {
    if (shardFacets != null) {
      for (Map.Entry<String, NamedList<Integer>> nextShardFacet : (Iterable<Map.Entry<String, NamedList<Integer>>>) shardFacets) {
        String fieldName = nextShardFacet.getKey();
        NamedList<Integer> collectedFacet = (NamedList<Integer>) childFacetFields.get(fieldName);
        NamedList<Integer> shardFacet = nextShardFacet.getValue();
        if (collectedFacet == null) {
          childFacetFields.add(fieldName, shardFacet);
        } else {
          mergeFacetValues(collectedFacet, shardFacet);
        }
      }
    }
  }

  private void mergeFacetValues(NamedList<Integer> collectedFacetValue, NamedList<Integer> shardFacetValue) {
    for (Map.Entry<String, Integer> nextShardValue : shardFacetValue) {
      String facetValue = nextShardValue.getKey();
      Integer shardCount = nextShardValue.getValue();
      int indexOfCollectedValue = collectedFacetValue.indexOf(facetValue, 0);
      if (indexOfCollectedValue == -1) {
        collectedFacetValue.add(facetValue, shardCount);
      } else {
        int newCount = collectedFacetValue.getVal(indexOfCollectedValue) + shardCount;
        collectedFacetValue.setVal(indexOfCollectedValue, newCount);
      }
    }
  }

  private NamedList getNamedListFromList(NamedList parentList, String name, boolean createIfAbsent) {
    NamedList result = null;
    if (parentList != null) {
      result = (NamedList) parentList.get(name);
      if (result == null && createIfAbsent) {
        result = new NamedList();
        parentList.add(name, result);
      }
    }
    return result;
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
      NamedList collectedChildFacetFields = getChildFacetFields(rb.rsp.getValues(), true);
      List<ShardResponse> responses = sreq.responses;
      for (ShardResponse shardResponse : responses) {
        NamedList shardChildFacetFields = getChildFacetFields(shardResponse.getSolrResponse().getResponse(), false);
        mergeFacets(collectedChildFacetFields, shardChildFacetFields);
      }
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.stage != ResponseBuilder.STAGE_GET_FIELDS) return;
    NamedList childFacetFields = getChildFacetFields(rb.rsp.getValues(), true);
    NamedList facetFields = getFacetFieldsList(rb);
    for (Map.Entry<String, NamedList> childFacetField : (Iterable<Map.Entry<String, NamedList>>) childFacetFields) {
     facetFields.add(childFacetField.getKey(), childFacetField.getValue());
    }
    rb.rsp.getValues().remove("child_facet_fields");
  }

  private NamedList getFacetFieldsList(ResponseBuilder rb) {
    NamedList facetCounts = getNamedListFromList(rb.rsp.getValues(), "facet_counts", true);
    return getNamedListFromList(facetCounts, "facet_fields", true);
  }


  @Override
  public String getDescription() {
    return "BlockJoin facet component";
  }
}

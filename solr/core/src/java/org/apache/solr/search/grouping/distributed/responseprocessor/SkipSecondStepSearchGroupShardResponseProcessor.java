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
package org.apache.solr.search.grouping.distributed.responseprocessor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.grouping.GroupingSpecification;
import org.apache.solr.search.grouping.distributed.shardresultserializer.SearchGroupsResultTransformer;

public class SkipSecondStepSearchGroupShardResponseProcessor extends SearchGroupShardResponseProcessor {

  @Override
  protected SearchGroupsResultTransformer newSearchGroupsResultTransformer(SolrIndexSearcher solrIndexSearcher) {
    return new SearchGroupsResultTransformer.SkipSecondStepSearchResultResultTransformer(solrIndexSearcher);
  }

  @Override
  protected SearchGroupsContainer newSearchGroupsContainer(ResponseBuilder rb) {
    return new SkipSecondStepSearchGroupsContainer(rb.getGroupingSpec().getFields());
  }

  @Override
  public void process(ResponseBuilder rb, ShardRequest shardRequest) {
    super.process(rb, shardRequest);
    TopGroupsShardResponseProcessor.fillResultIds(rb);
  }

  private static class SkipSecondStepSearchGroupsContainer extends SearchGroupsContainer {

    private final Map<Object, String> docIdToShard = new HashMap<>();

    public SkipSecondStepSearchGroupsContainer(String[] fields) {
      super(fields);
    }

    @Override
    public void addSearchGroups(ShardResponse srsp, String field, Collection<SearchGroup<BytesRef>> searchGroups) {
      super.addSearchGroups(srsp, field, searchGroups);
      for (SearchGroup<BytesRef> searchGroup : searchGroups) {
        assert(srsp.getShard() != null);
        docIdToShard.put(searchGroup.topDocSolrId, srsp.getShard());
      }
    }

    @Override
    public void addMergedSearchGroups(ResponseBuilder rb, String groupField, Collection<SearchGroup<BytesRef>> mergedTopGroups ) {
      // TODO: add comment or javadoc re: why this method is overridden as a no-op
    }

    @Override
    public void addSearchGroupToShards(ResponseBuilder rb, String groupField, Collection<SearchGroup<BytesRef>> mergedTopGroups) {
      super.addSearchGroupToShards(rb, groupField, mergedTopGroups);

      final GroupDocs<BytesRef>[] groups = new GroupDocs[mergedTopGroups.size()];

      // This is the max score found in any document on any group
      float maxScore = Float.MIN_VALUE;
      int groupsIndex = 0;

      for (SearchGroup<BytesRef> group : mergedTopGroups) {
        maxScore = Math.max(maxScore, group.topDocScore);
        final String shard = docIdToShard.get(group.topDocSolrId);
        assert(shard != null);
        final ShardDoc sdoc = new ShardDoc();
        sdoc.score = group.topDocScore;
        sdoc.id = group.topDocSolrId;
        sdoc.shard = shard;

        groups[groupsIndex++] = new GroupDocs<>(
            Float.NaN,
            group.topDocScore,
            new TotalHits(1, TotalHits.Relation.EQUAL_TO), /* we don't know the actual number of hits in the group- we set it to 1 as we only keep track of the top doc */
            new ShardDoc[] { sdoc }, /* only top doc */
            group.groupValue,
            group.sortValues);
      }

      final GroupingSpecification groupingSpecification = rb.getGroupingSpec();

      final TopGroups<BytesRef> topMergedGroups = new TopGroups<BytesRef>(
          groupingSpecification.getGroupSortSpec().getSort().getSort(),
          groupingSpecification.getWithinGroupSortSpec().getSort().getSort(),
          0, /*Set totalHitCount to 0 as we can't computed it as is */
          0, /*Set totalGroupedHitCount to 0 as we can't computed it as is*/
          groups,
          groups.length > 0 ? maxScore : Float.NaN);
      rb.mergedTopGroups.put(groupField, topMergedGroups);
    }
  }

}

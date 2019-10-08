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
package org.apache.solr.search.grouping;

import java.util.Arrays;
import java.util.Collections;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.search.Grouping;
import org.apache.solr.search.SortSpec;

/**
 * Encapsulates the grouping options like fields group sort and more specified by clients.
 *
 * @lucene.experimental
 */
public class GroupingSpecification {

  private String[] fields = new String[]{};
  private String[] queries = new String[]{};
  private String[] functions = new String[]{};
  private SortSpec groupSortSpec;
  private SortSpec withinGroupSortSpec;
  private boolean includeGroupCount;
  private boolean main;
  private Grouping.Format responseFormat;
  private boolean needScore;
  private boolean truncateGroups;
  /* This is an optimization to skip the second grouping step when groupLimit is 1. The second
  * grouping step retrieves the top K documents for each group. This is not necessary when only one
  * document per group is required because in the first step every shard sends back the group score given
  * by its top document.
  */
  private boolean skipSecondGroupingStep;

  /**
   * Validates the current GropingSpecification.
   * It will throw a SolrException the grouping specification is not valid, otherwise
   * it will return without side effects.
   */
  public void validate() throws SolrException {
    if (skipSecondGroupingStep) {
      validateSkipSecondGroupingStep();
    }

    // when group.format=grouped then, validate group.offset
    // for group.main=true and group.format=simple, start value is used instead of group.offset
    if (!(main || responseFormat == Grouping.Format.simple) &&
       withinGroupSortSpec.getOffset() < 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'group.offset' parameter cannot be negative");
    }
  }

  private void validateSkipSecondGroupingStep() {
    // Only possible if we only want one doc per group
    final int limit =  withinGroupSortSpec.getCount();
    final int offset = withinGroupSortSpec.getOffset();
    if (limit != 1) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          GroupParams.GROUP_SKIP_DISTRIBUTED_SECOND + " does not support " +
          GroupParams.GROUP_LIMIT + " != 1 ("+GroupParams.GROUP_LIMIT+" is "+limit+")");
    }

    // group.func not supported
    if (functions.length > 0){
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              GroupParams.GROUP_SKIP_DISTRIBUTED_SECOND + " does not support "+ GroupParams.GROUP_FUNC);
    }
    // group.query not supported
    if (queries.length > 0){
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              GroupParams.GROUP_SKIP_DISTRIBUTED_SECOND + " does not support "+ GroupParams.GROUP_QUERY);
    }

    if (offset != 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          GroupParams.GROUP_SKIP_DISTRIBUTED_SECOND + " does not support " + GroupParams.GROUP_OFFSET + " != 0 (" +
              GroupParams.GROUP_OFFSET + " is "+offset + ")");
    }

    if (includeGroupCount) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          GroupParams.GROUP_SKIP_DISTRIBUTED_SECOND + " does not support " + GroupParams.GROUP_TOTAL_COUNT + " == true");
    }

    final SortField[] withinGroupSortFields = withinGroupSortSpec.getSort().getSort();
    final SortField[] groupSortFields = groupSortSpec.getSort().getSort();

    // Within group sort must be the same as group sort because if we skip second step no sorting within group will be done.
    // This checks if withinGroupSortFields is a prefix of groupSortFields
    if (Collections.indexOfSubList(Arrays.asList(groupSortFields), Arrays.asList(withinGroupSortFields)) != 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          GroupParams.GROUP_SKIP_DISTRIBUTED_SECOND + " does not allow the given within/global sort group configuration");
    }
  }

  public String[] getFields() {
    return fields;
  }

  public void setFields(String[] fields) {
    if (fields == null) {
      return;
    }

    this.fields = fields;
  }

  public String[] getQueries() {
    return queries;
  }

  public void setQueries(String[] queries) {
    if (queries == null) {
      return;
    }

    this.queries = queries;
  }

  public String[] getFunctions() {
    return functions;
  }

  public void setFunctions(String[] functions) {
    if (functions == null) {
      return;
    }

    this.functions = functions;
  }

  public boolean isIncludeGroupCount() {
    return includeGroupCount;
  }

  public void setIncludeGroupCount(boolean includeGroupCount) {
    this.includeGroupCount = includeGroupCount;
  }

  public boolean isMain() {
    return main;
  }

  public void setMain(boolean main) {
    this.main = main;
  }

  public Grouping.Format getResponseFormat() {
    return responseFormat;
  }

  public void setResponseFormat(Grouping.Format responseFormat) {
    this.responseFormat = responseFormat;
  }

  public boolean isNeedScore() {
    return needScore;
  }

  public void setNeedScore(boolean needScore) {
    this.needScore = needScore;
  }

  public boolean isTruncateGroups() {
    return truncateGroups;
  }

  public void setTruncateGroups(boolean truncateGroups) {
    this.truncateGroups = truncateGroups;
  }

  public SortSpec getGroupSortSpec() {
    return groupSortSpec;
  }

  public void setGroupSortSpec(SortSpec groupSortSpec) {
    this.groupSortSpec = groupSortSpec;
  }

  public SortSpec getWithinGroupSortSpec() {
    return withinGroupSortSpec;
  }

  public void setWithinGroupSortSpec(SortSpec withinGroupSortSpec) {
    this.withinGroupSortSpec = withinGroupSortSpec;
  }

  public boolean isSkipSecondGroupingStep() {
    return skipSecondGroupingStep;
  }

  public void setSkipSecondGroupingStep(boolean skipSecondGroupingStep) {
    this.skipSecondGroupingStep = skipSecondGroupingStep;
  }

}

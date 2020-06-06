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

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StatsParams;

import java.util.*;

/**
 * Models all of the information about stats needed for a single request
 *
 * @see StatsField
 */
class StatsInfo {

    private final ResponseBuilder rb;
    private final List<StatsField> statsFields = new ArrayList<>(7);
    private final Map<String, StatsValues> distribStatsValues = new LinkedHashMap<>();
    private final Map<String, StatsField> statsFieldMap = new LinkedHashMap<>();
    private final Map<String, List<StatsField>> tagToStatsFields = new LinkedHashMap<>();

    public StatsInfo(ResponseBuilder rb) {
        this.rb = rb;
        SolrParams params = rb.req.getParams();
        String[] statsParams = params.getParams(StatsParams.STATS_FIELD);
        if (null == statsParams) {
            // no stats.field params, nothing to parse.
            return;
        }

        for (String paramValue : statsParams) {
            StatsField current = new StatsField(rb, paramValue);
            statsFields.add(current);
            for (String tag : current.getTagList()) {
                List<StatsField> fieldList = tagToStatsFields.get(tag);
                if (fieldList == null) {
                    fieldList = new ArrayList<>();
                }
                fieldList.add(current);
                tagToStatsFields.put(tag, fieldList);
            }
            statsFieldMap.put(current.getOutputKey(), current);
            distribStatsValues.put(current.getOutputKey(),
                    StatsValuesFactory.createStatsValues(current));
        }
    }

    /**
     * Returns an immutable list of {@link StatsField} instances
     * modeling each of the {@link StatsParams#STATS_FIELD} params specified
     * as part of this request
     */
    public List<StatsField> getStatsFields() {
        return Collections.unmodifiableList(statsFields);
    }

    /**
     * Returns the {@link StatsField} associated with the specified (effective)
     * outputKey, or null if there was no {@link StatsParams#STATS_FIELD} param
     * that would corrispond with that key.
     */
    public StatsField getStatsField(String outputKey) {
        return statsFieldMap.get(outputKey);
    }

    /**
     * Return immutable list of {@link StatsField} instances by string tag local parameter.
     *
     * @param tag tag local parameter
     * @return list of stats fields
     */
    public List<StatsField> getStatsFieldsByTag(String tag) {
        List<StatsField> raw = tagToStatsFields.get(tag);
        if (null == raw) {
            return Collections.emptyList();
        } else {
            return Collections.unmodifiableList(raw);
        }
    }

    /**
     * Returns an immutable map of response key =&gt; {@link StatsValues}
     * instances for the current distributed request.
     * Depending on where we are in the process of handling this request,
     * these {@link StatsValues} instances may not be complete -- but they
     * will never be null.
     */
    public Map<String, StatsValues> getAggregateStatsValues() {
        return Collections.unmodifiableMap(distribStatsValues);
    }

}

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
package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

import java.util.List;
import java.util.Map;

import static org.apache.solr.client.solrj.request.beans.V2ApiConstants.CREATE_COLLECTION_KEY;

public class CreateAliasPayload implements ReflectMapWriter {
    @JsonProperty(required = true)
    public String name;

    @JsonProperty
    public List<String> collections;

    @JsonProperty
    public AliasRouter router;

    @JsonProperty
    public String tz;

    @JsonProperty(CREATE_COLLECTION_KEY)
    public Map<String, Object> createCollectionParams;

    @JsonProperty
    public String async;

    public static class AliasRouter implements ReflectMapWriter {
        @JsonProperty(required = true)
        public String name;

        @JsonProperty
        public String field;

        @JsonProperty
        public String start;

        @JsonProperty
        public String interval;

        @JsonProperty
        public Integer maxFutureMs;

        @JsonProperty
        public String preemptiveCreateMath;

        @JsonProperty
        public String autoDeleteAge;

        @JsonProperty
        public Integer maxCardinality;

        @JsonProperty
        public String mustMatch;

        @JsonProperty
        public List<Map<String, Object>> routerList;
    }
}



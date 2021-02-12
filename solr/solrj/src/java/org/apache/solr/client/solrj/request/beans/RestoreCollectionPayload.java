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

import java.util.Map;

import static org.apache.solr.client.solrj.request.beans.V2ApiConstants.CREATE_COLLECTION_KEY;

/**
 * V2 API POJO for the /v2/collections 'restore-collection' command.
 *
 * Analogous to the request parameters for v1 /admin/collections?action=RESTORE API.
 */
public class RestoreCollectionPayload implements ReflectMapWriter {

    @JsonProperty(required = true)
    public String collection;

    @JsonProperty(required = true)
    public String name;

    @JsonProperty
    public String location;

    @JsonProperty
    public String repository;

    @JsonProperty
    public Integer backupId;

    @JsonProperty(CREATE_COLLECTION_KEY)
    public Map<String, Object> createCollectionParams;

    @JsonProperty
    public String async;
}

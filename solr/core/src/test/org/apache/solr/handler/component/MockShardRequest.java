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

import java.util.ArrayList;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.mockito.Mockito;

public class MockShardRequest extends ShardRequest {

    public static MockShardRequest create() {
        MockShardRequest mockShardRequest = new MockShardRequest();
        mockShardRequest.responses = new ArrayList<>();
        return mockShardRequest;
    }

    public MockShardRequest withShardResponse(NamedList<Object> responseHeader, SolrDocumentList solrDocuments) {
        ShardResponse shardResponse = buildShardResponse(responseHeader, solrDocuments);
        responses.add(shardResponse);
        return this;
    }

    private ShardResponse buildShardResponse(NamedList<Object> responseHeader, SolrDocumentList solrDocuments) {
        SolrResponse solrResponse = Mockito.mock(SolrResponse.class);
        ShardResponse shardResponse = new ShardResponse();
        NamedList<Object> response = new NamedList<>();
        response.add("response", solrDocuments);
        shardResponse.setSolrResponse(solrResponse);
        response.add("responseHeader", responseHeader);
        Mockito.when(solrResponse.getResponse()).thenReturn(response);

        return shardResponse;
    }

}

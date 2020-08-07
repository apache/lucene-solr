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
package org.apache.solr.cluster.api;

import org.apache.solr.client.solrj.SolrRequest;

import java.util.Map;

/**Abstract out HTTP aspects of the request
 *
 */
public interface HttpRpc {
    HttpRpc withCallRouter(CallRouter callRouter);

    /**
     * Add a request param
     */
    HttpRpc addParam(String key, String val);

    HttpRpc addParam(String key, int val);

    HttpRpc addParam(String key, boolean val);

    HttpRpc addParam(String key, float val);

    HttpRpc addParam(String key, double val);

    /**
     * Add multiple request params
     */
    HttpRpc addParams(Map<String, String> params);

    /**
     * Add a request header
     */
    HttpRpc addHeader(String key, String val);

    /**
     * Consumer for the response data
     */
    HttpRpc withResponseConsumer(RpcFactory.ResponseConsumer sink);

    /**
     * Handle request headers if required
     */
    HttpRpc withHeaderConsumer(RpcFactory.HeaderConsumer headerConsumer);

    /**
     * Use Send a payload
     */
    HttpRpc withPayload(RpcFactory.InputSupplier payload);

    /**
     * Http method
     */
    HttpRpc withMethod(SolrRequest.METHOD method);

    /**
     * The uri. The semantics depends on
     * whether it is made to a node or replica
     * if it is a shard/replica the uri is the name of the handler (e.g /select , /update/json etc)
     * if it is node it is a full path (e.g: /admin/collections)
     */
    HttpRpc withV1Path(String uri);

    /**
     * The uri. The semantics depends on
     * whether it is made to a node or replica
     * if it is a shard/replica the uri is the name of the handler
     * if it is node it is a full path
     */
    HttpRpc withV2Path(String uri);


    /**
     * Invoke a synchronous request. The return object depends on the output of the
     * {@link RpcFactory.ResponseConsumer}
     */
    Object invoke() throws RpcFactory.RPCException;

}

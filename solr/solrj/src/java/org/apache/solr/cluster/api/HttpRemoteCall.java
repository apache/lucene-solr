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

/**
 * Abstract out HTTP aspects of a Solr request
 */
public interface HttpRemoteCall {
    HttpRemoteCall withPathSupplier(PathSupplier pathSupplier);

    /**
     * Add a request param
     */
    HttpRemoteCall addParam(String key, String val);

    HttpRemoteCall addParam(String key, int val);

    HttpRemoteCall addParam(String key, boolean val);

    HttpRemoteCall addParam(String key, float val);

    HttpRemoteCall addParam(String key, double val);

    /**
     * Add a request header
     */
    HttpRemoteCall addHeader(String key, String val);

    /**
     * Consumer for the response data
     */
    HttpRemoteCall withResponseConsumer(RemoteCallFactory.ResponseConsumer sink);

    /**
     * Handle request headers if required
     */
    HttpRemoteCall withHeaderConsumer(RemoteCallFactory.HeaderConsumer headerConsumer);

    /**
     * Use Send a payload
     */
    HttpRemoteCall withPayload(RemoteCallFactory.InputSupplier payload);

    /**
     * Http method
     */
    HttpRemoteCall withMethod(Method method);

    /**
     * The uri. The semantics depends on
     * whether it is made to a node or replica
     * if it is a shard/replica the uri is the name of the handler (e.g /select , /update/json etc)
     * if it is node it is a full path (e.g: /admin/collections)
     */
    HttpRemoteCall withPath(String handler, ApiType type);

    /**
     * Invoke a synchronous request. The return object depends on the output of the
     * {@link RemoteCallFactory.ResponseConsumer}
     */
    Object invoke() throws RemoteCallFactory.RemoteCallException;

    enum Method {
        GET, POST, DELETE,PUT
    }

}

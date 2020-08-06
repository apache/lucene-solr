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
package org.apache.solr.common.util;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.function.Function;

public interface RpcFactory {

    Rpc create();


     interface Rpc {
         /** A request is made to the leader of the shard
          *
          */
        Rpc toShardLeader(String collection, String shard);

         /** Make a request to any replica of the shard
          */
        Rpc toShard(String collection, String shard);

         /** Make a request to any replica of the shard of type
          */
        Rpc toShard(String collection, String shard, Replica.Type type);

         /**Identify the shard using the routeKey and send the request to the leader
          * replica
          */
        Rpc routeToShardLeader(String collection, String routeKey);

         /**Identify the shard using the route key and send the request to a given replica type
          */
        Rpc routeToShard(String collection, String routeKey, Replica.Type type);

         /**Identify the shard using the route key and send the request to a random replica
          */
         Rpc routeToShard(String collection, String routeKey);
         /**Make a request to a specific replica
          */
        Rpc toReplica(String collection, String shard, String replica);

         /**To any dolr vore  that may host this collection
          */
        Rpc toCollection(String collection);

         /**The request is to a specific node
          *
          */
        Rpc withTargetNode(String node);

         /** Add a request param
          */
        Rpc addParam(String key, String val);

         /**Add multiple request params
          */
        Rpc addParams(Map<String, String> params);

         /** Add a request header
          */
        Rpc addHeader(String key, String val);

         /**Consumer for the response data
          */
        Rpc withResponseConsumer(OutputConsumer sink);

         /** Use Send a payload
          */
        Rpc withPayload(InputSupplier payload);

         /**Http method
          */
        Rpc withHttpMethod(SolrRequest.METHOD method);

         /**The uri. The semantics depends on
          * whether it is made to a node or replica
          * if it is a shard/replica the uri is the name of the handler
          * if it is node it is a full path
          */
        Rpc withV1Uri(String uri);

         /**The uri. The semantics depends on
          * whether it is made to a node or replica
          * if it is a shard/replica the uri is the name of the handler
          * if it is node it is a full path
          */
        Rpc withV2Uri(String uri);


         /**Invoke a synchronous request. The return object depends on the output of the
          * {@link OutputConsumer}
          */
        Object invoke() throws RPCException;

    }

    interface OutputConsumer {
        Object accept(InputStream is) throws IOException;
    }

    /**Provide the payload stream
     *
     */
    interface InputSupplier {
        void write(OutputStream os) throws IOException;

        String getContentType();
    }


    class RPCException extends SolrException {

        public RPCException(ErrorCode code, String msg) {
            super(code, msg);
        }
    }
    /** Consumer of header data
     */
    interface HeaderConsumer {
        /**
         * read all required values from the header
         * @param status the HTTP status code
         * @return true to proceed to processing body. if false , ignore the body
         */
        boolean readHeader(int status, Function<String, String> headerProvider);
    }
}

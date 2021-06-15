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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Function;

/**A factory that creates any type of RPC calls in Solr
 * This is designed to create low level access to the RPC mechanism.
 * This is agnostic of Solr documents or other internal concepts of Solr
 * But it knows certain things
 * a) how to locate a Solr core/replica
 * b) basic HTTP access,
 * c) serialization/deserialization is the responsibility of the code that is making a request
 *
 */
public interface RemoteCallFactory {

    /**
     * Create a new router instance
     * This instance can be reused.
     */
    CallRouter createCallRouter();

    /**
     * Create a new {@link HttpRemoteCall}
     * This instance can be reused.
     */
    HttpRemoteCall createHttpRemoteCall();


    interface ResponseConsumer {
        /**
         * Allows this impl to add request params/http headers before the request is fired.
         * All values must be set before this method returns. Do not hold a reference to this
         *  {@link HttpRemoteCall} and set values later
         */
        default void setRemoteCall(HttpRemoteCall rpc){}

        /**Process the response.
         * Ensure that the whole stream is eaten up before this method returns
         * The stream will be closed after the method returns
         */
        Object accept(InputStream is) throws IOException;
    }

    /**Provide the payload stream
     *
     */
    interface InputSupplier {
        void write(OutputStream os) throws IOException;

        /**
         * Allows this impl to add request params/http headers before the request is fired.
         * All values must be set before this method returns. Do not hold a reference to this
         *  {@link HttpRemoteCall} and set values later
         */
        default void setRemoteCall(HttpRemoteCall rpc){}
    }


    class RemoteCallException extends SolrException {

        public RemoteCallException(ErrorCode code, String msg) {
            super(code, msg);
        }
    }
    /** Consumer of header data
     */
    interface HeaderConsumer {
        /**Allows this impl to add request params/http headers before the request is fired
         * All values must be set before this method returns. Do not hold a reference to this
         *  {@link HttpRemoteCall} and set values later
         */
        default void setRemoteCall(HttpRemoteCall rpc){}
        /**
         * read all required values from the header
         * @param status the HTTP status code
         * @return true to proceed to processing body. if false , ignore the body
         */
        boolean readHeader(int status, Function<String, String> headerProvider);
    }

    ResponseConsumer JAVABIN_CONSUMER = new ResponseConsumer() {
        @Override
        public void setRemoteCall(HttpRemoteCall rpc) {
            rpc.addParam(CommonParams.WT , CommonParams.JAVABIN);
        }

        @Override
        public Object accept(InputStream is) throws IOException {
            return Utils.JAVABINCONSUMER.accept(is);
        }
    };
    ResponseConsumer JSON_CONSUMER = new ResponseConsumer() {
        @Override
        public void setRemoteCall(HttpRemoteCall rpc) {
            rpc.addParam(CommonParams.WT , CommonParams.JSON);
        }

        @Override
        public Object accept(InputStream is) throws IOException {
            return Utils.JSONCONSUMER.accept(is);
        }
    };
}

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

package org.apache.solr.client.solrj.impl;

import java.util.Collections;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;

import static org.apache.solr.common.util.Utils.getObjectByPath;

public abstract class BaseHttpSolrClient extends SolrClient {

  /**
   * Subclass of SolrException that allows us to capture an arbitrary HTTP
   * status code that may have been returned by the remote server or a
   * proxy along the way.
   */
  public static class RemoteSolrException extends SolrException {
    /**
     * @param remoteHost the host the error was received from
     * @param code Arbitrary HTTP status code
     * @param msg Exception Message
     * @param th Throwable to wrap with this Exception
     */
    public RemoteSolrException(String remoteHost, int code, String msg, Throwable th) {
      super(code, "Error from server at " + remoteHost + ": " + msg, th);
    }
  }

  /**
   * This should be thrown when a server has an error in executing the request and
   * it sends a proper payload back to the client
   */
  @SuppressWarnings("serial")
  public static class RemoteExecutionException extends HttpSolrClient.RemoteSolrException {
    private NamedList meta;

    public RemoteExecutionException(String remoteHost, int code, String msg, NamedList meta) {
      super(remoteHost, code, msg, null);
      this.meta = meta;
    }


    @SuppressWarnings("deprecation")
    public static HttpSolrClient.RemoteExecutionException create(String host, NamedList errResponse) {
      Object errObj = errResponse.get("error");
      if (errObj != null) {
        Number code = (Number) getObjectByPath(errObj, true, Collections.singletonList("code"));
        String msg = (String) getObjectByPath(errObj, true, Collections.singletonList("msg"));
        return new HttpSolrClient.RemoteExecutionException(host, code == null ? ErrorCode.UNKNOWN.code : code.intValue(),
            msg == null ? "Unknown Error" : msg, errResponse);

      } else {
        throw new RuntimeException("No error");
      }

    }

    public NamedList getMetaData() {

      return meta;
    }
  }

}

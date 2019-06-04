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

import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;

public interface HttpListenerFactory {
  abstract class RequestResponseListener implements Request.BeginListener, Response.CompleteListener, Request.QueuedListener {

    /**
     * Callback method invoked when the request begins being processed in order to be sent.
     * This is the last opportunity to modify the request.
     * This method will NOT be ensured to be called on the same thread as the thread calling {@code Http2SolrClient} methods.
     *
     * @param request the request that begins being processed
     */
    @Override
    public void onBegin(Request request){}

    /**
     * Callback method invoked when the request is queued, waiting to be sent.
     * This method will be ensured to be called on the same thread as the thread calling {@code Http2SolrClient} methods.
     *
     * @param request the request being queued
     */
    @Override
    public void onQueued(Request request) {}

    @Override
    public void onComplete(Result result) {}
  }

  RequestResponseListener get();
}


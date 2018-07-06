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

package org.apache.solr.store.adls;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;

import java.io.IOException;

/**
 * provides a way to mock WebHDFS calls that ADLS is making.
 * Redirects them to a local directory under build/.
 */
public class WebServerBuilder {
  private MockWebServer server;

  public  WebServerBuilder(String path) {
    server = new MockWebServer();
    WebHdfsDispatcher dispatcher = new WebHdfsDispatcher(path);
    dispatcher.setFailFast(new MockResponse().setResponseCode(400));
    server.setDispatcher(dispatcher);
  }

  public  String getAccountFQDN() {
    return server.getHostName() + ":" + server.getPort();
  }

  public  String getAuthTokenEndPoint() {
    return "http://" + server.getHostName() + ":" + server.getPort();
  }

  public  void shutdown() throws IOException {
    server.shutdown();
  }

}

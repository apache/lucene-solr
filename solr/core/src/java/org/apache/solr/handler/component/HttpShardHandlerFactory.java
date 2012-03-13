/**
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

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.*;

/**
 * @author noble.paul@teamaol.com (noblep01)
 *         Date: 6/21/11
 *         Time: 12:14 PM
 */
public class HttpShardHandlerFactory extends ShardHandlerFactory implements PluginInfoInitialized {
  protected static Logger log = LoggerFactory.getLogger(HttpShardHandlerFactory.class);

  // We want an executor that doesn't take up any resources if
  // it's not used, so it could be created statically for
  // the distributed search component if desired.
  //
  // Consider CallerRuns policy and a lower max threads to throttle
  // requests at some point (or should we simply return failure?)
  Executor commExecutor = new ThreadPoolExecutor(
      0,
      Integer.MAX_VALUE,
      5, TimeUnit.SECONDS, // terminate idle threads after 5 sec
      new SynchronousQueue<Runnable>()  // directly hand off tasks
  );

  HttpClient client;
  Random r = new Random();
  int soTimeout = 0; //current default values
  int connectionTimeout = 0; //current default values
  int maxConnectionsPerHost = 20;
  int corePoolSize = 0;
  int maximumPoolSize = 10;
  int keepAliveTime = 5;
  int queueSize = 1;
  boolean accessPolicy = true;

  public String scheme = "http://"; //current default values

  // socket timeout measured in ms, closes a socket if read
  // takes longer than x ms to complete. throws
  // java.net.SocketTimeoutException: Read timed out exception
  static final String INIT_SO_TIMEOUT = "socketTimeout";

  // connection timeout measures in ms, closes a socket if connection
  // cannot be established within x ms. with a
  // java.net.SocketTimeoutException: Connection timed out
  static final String INIT_CONNECTION_TIMEOUT = "connTimeout";

  // URL scheme to be used in distributed search.
  static final String INIT_URL_SCHEME = "urlScheme";

  // Maximum connections allowed per host
  static final String INIT_MAX_CONNECTION_PER_HOST = "maxConnectionsPerHost";

  // The core size of the threadpool servicing requests
  static final String INIT_CORE_POOL_SIZE = "corePoolSize";

  // The maximum size of the threadpool servicing requests
  static final String INIT_MAX_POOL_SIZE = "maximumPoolSize";

  // The amount of time idle threads persist for in the queue, before being killed
  static final String MAX_THREAD_IDLE_TIME = "maxThreadIdleTime";

  // If the threadpool uses a backing queue, what is its maximum size (-1) to use direct handoff
  static final String INIT_SIZE_OF_QUEUE = "sizeOfQueue";

  // Configure if the threadpool favours fairness over throughput
  static final String INIT_FAIRNESS_POLICY = "fairnessPolicy";

  public ShardHandler getShardHandler() {
    return new HttpShardHandler(this);
  }

  public void init(PluginInfo info) {
    NamedList args = info.initArgs;
    this.soTimeout = getParameter(args, INIT_SO_TIMEOUT, 0);

    this.scheme = getParameter(args, INIT_URL_SCHEME, "http://");
    this.scheme = (this.scheme.endsWith("://")) ? this.scheme : this.scheme + "://";
    this.connectionTimeout = getParameter(args, INIT_CONNECTION_TIMEOUT, 0);
    this.maxConnectionsPerHost = getParameter(args, INIT_MAX_CONNECTION_PER_HOST, 20);
    this.corePoolSize = getParameter(args, INIT_CORE_POOL_SIZE, 0);
    this.maximumPoolSize = getParameter(args, INIT_MAX_POOL_SIZE, Integer.MAX_VALUE);
    this.keepAliveTime = getParameter(args, MAX_THREAD_IDLE_TIME, 5);
    this.queueSize = getParameter(args, INIT_SIZE_OF_QUEUE, -1);
    this.accessPolicy = getParameter(args, INIT_FAIRNESS_POLICY, false);

    this.commExecutor = new ThreadPoolExecutor(
        this.corePoolSize,
        this.maximumPoolSize,
        this.keepAliveTime, TimeUnit.SECONDS,
        (this.queueSize == -1) ?
            new SynchronousQueue<Runnable>(this.accessPolicy) :
            new ArrayBlockingQueue<Runnable>(this.queueSize, this.accessPolicy)
    );

    MultiThreadedHttpConnectionManager mgr = new MultiThreadedHttpConnectionManager();
    mgr.getParams().setDefaultMaxConnectionsPerHost(this.maxConnectionsPerHost);
    mgr.getParams().setMaxTotalConnections(10000);
    mgr.getParams().setConnectionTimeout(this.connectionTimeout);
    mgr.getParams().setSoTimeout(this.soTimeout);
    // mgr.getParams().setStaleCheckingEnabled(false);

    client = new HttpClient(mgr);

    // prevent retries  (note: this didn't work when set on mgr.. needed to be set on client)
    DefaultHttpMethodRetryHandler retryhandler = new DefaultHttpMethodRetryHandler(0, false);
    client.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, retryhandler);
  }

  private <T> T getParameter(NamedList initArgs, String configKey, T defaultValue) {
    T toReturn = defaultValue;
    if (initArgs != null) {
      T temp = (T) initArgs.get(configKey);
      toReturn = (temp != null) ? temp : defaultValue;
    }
    log.info("Setting {} to: {}", configKey, soTimeout);
    return toReturn;
  }
}

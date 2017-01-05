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
package org.apache.solr.rest;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.request.SolrRequestInfo;
import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.routing.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Restlet servlet handling /&lt;context&gt;/&lt;collection&gt;/schema/* URL paths
 */
public class SolrSchemaRestApi extends Application {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  /**
   * Returns reserved endpoints under /schema
   */
  public static Set<String> getReservedEndpoints() {
    Set<String> reservedEndpoints = new HashSet<>();
    return Collections.unmodifiableSet(reservedEndpoints);
  }

  private Router router;

  public SolrSchemaRestApi() {
    router = new Router(getContext());
  }

  @Override
  public void stop() throws Exception {
    if (null != router) {
      router.stop();
    }
  }

  /**
   * Bind URL paths to the appropriate ServerResource subclass. 
   */
  @Override
  public synchronized Restlet createInboundRoot() {

    log.info("createInboundRoot started for /schema");


    router.attachDefault(RestManager.ManagedEndpoint.class);
    
    // attach all the dynamically registered schema resources
    RestManager.getRestManager(SolrRequestInfo.getRequestInfo())
        .attachManagedResources(RestManager.SCHEMA_BASE_PATH, router);

    log.info("createInboundRoot complete for /schema");

    return router;
  }  
}

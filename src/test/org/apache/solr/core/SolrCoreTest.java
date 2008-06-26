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

package org.apache.solr.core;

import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.plugin.SolrCoreAware;

public class SolrCoreTest extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }

  public void testRequestHandlerRegistry() {
    // property values defined in build.xml
    SolrCore core = h.getCore();

    EmptyRequestHandler handler1 = new EmptyRequestHandler();
    EmptyRequestHandler handler2 = new EmptyRequestHandler();

    String path = "/this/is A path /that won't be registered!";
    SolrRequestHandler old = core.registerRequestHandler( path, handler1 );
    assertNull( old ); // should not be anything...
    assertEquals( core.getRequestHandlers().get( path ), handler1 );
    old = core.registerRequestHandler( path, handler2 );
    assertEquals( old, handler1 ); // should pop out the old one
    assertEquals( core.getRequestHandlers().get( path ), handler2 );
  }

  public void testClose() throws Exception {
    SolrCore core = h.getCore();

    ClosingRequestHandler handler1 = new ClosingRequestHandler();
    handler1.inform( core );

    String path = "/this/is A path /that won't be registered!";
    SolrRequestHandler old = core.registerRequestHandler( path, handler1 );
    assertNull( old ); // should not be anything...
    assertEquals( core.getRequestHandlers().get( path ), handler1 );
    core.close();
    assertTrue("Handler not closed", handler1.closed == true);
  }
}

class ClosingRequestHandler extends EmptyRequestHandler implements SolrCoreAware {
  boolean closed = false;

  public void inform(SolrCore core) {
    core.addCloseHook( new CloseHook() {
      public void close(SolrCore core) {
        closed = true;
      }
    });
  }
}

/**
 * An empty handler for testing
 */
class EmptyRequestHandler extends RequestHandlerBase
{
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // nothing!
  }

  @Override public String getDescription() { return null; }
  @Override public String getSource() { return null; }
  @Override public String getSourceId() { return null; }
  @Override public String getVersion() { return null; }
}

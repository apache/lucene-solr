package org.apache.solr.rest;
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

import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.BeforeClass;
import org.restlet.ext.servlet.ServerServlet;

import java.util.SortedMap;
import java.util.TreeMap;

abstract public class SchemaRestletTestBase extends RestTestBase {
  @BeforeClass
  public static void init() throws Exception {
    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<ServletHolder,String>();
    final ServletHolder schemaRestApi = new ServletHolder("SchemaRestApi", ServerServlet.class);
    schemaRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SchemaRestApi");
    extraServlets.put(schemaRestApi, "/schema/*");

    createJettyAndHarness(TEST_HOME(), "solrconfig.xml", "schema-rest.xml", "/solr", true, extraServlets);
  }
}

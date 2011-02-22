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

import org.apache.solr.util.AbstractSolrTestCase;

public class TestBadConfig extends AbstractSolrTestCase {

  @Override
  public String getSchemaFile() { return "schema.xml"; }
  @Override
  public String getSolrConfigFile() { return "bad_solrconfig.xml"; }

  @Override
  public void setUp() throws Exception {
    ignoreException("unset.sys.property");
    try {
      super.setUp();
      fail("Exception should have been thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("unset.sys.property"));
    } finally {
      resetExceptionIgnores();
    }
  }
    

  public void testNothing() {
    // Empty test case as the real test is that the initialization of the TestHarness fails
    assertTrue(true);
  }
}
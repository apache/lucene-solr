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
package org.apache.solr.core;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ShardHandlerFactory;

/**
 * Tests specifying a custom ShardHandlerFactory
 */
public class TestShardHandlerFactory extends SolrTestCaseJ4 {

  public void testXML() throws Exception {
    Path home = Paths.get(TEST_HOME());
    CoreContainer cc = CoreContainer.createAndLoad(home, home.resolve("solr-shardhandler.xml"));
    ShardHandlerFactory factory = cc.getShardHandlerFactory();
    assertTrue(factory instanceof MockShardHandlerFactory);
    @SuppressWarnings({"rawtypes"})
    NamedList args = ((MockShardHandlerFactory)factory).args;
    assertEquals("myMagicRequiredValue", args.get("myMagicRequiredParameter"));
    factory.close();
    cc.shutdown();
  }

}

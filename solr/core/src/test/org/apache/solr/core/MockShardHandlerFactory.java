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

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.util.plugin.PluginInfoInitialized;

/** a fake shardhandler factory that does nothing. */
public class MockShardHandlerFactory extends ShardHandlerFactory implements PluginInfoInitialized {
  NamedList args;
  
  @Override
  public void init(PluginInfo info) {
    args = info.initArgs;
  }
  
  @Override
  public ShardHandler getShardHandler() {
    return new ShardHandler() {
      @Override
      public void prepDistributed(ResponseBuilder rb) {}

      @Override
      public void submit(ShardRequest sreq, String shard,
          ModifiableSolrParams params) {}

      @Override
      public ShardResponse takeCompletedIncludingErrors() {
        return null;
      }

      @Override
      public ShardResponse takeCompletedOrError() {
        return null;
      }

      @Override
      public void cancelAll() {}

      @Override
      public ShardHandlerFactory getShardHandlerFactory() {
        return MockShardHandlerFactory.this;
      }
    };
  }

  @Override
  public void close() {}
}

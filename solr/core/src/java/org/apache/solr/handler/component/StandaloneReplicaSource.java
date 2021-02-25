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

package org.apache.solr.handler.component;

import java.util.Collections;
import java.util.List;

import org.apache.solr.common.util.StrUtils;

/**
 * A replica source for solr stand alone mode
 */
class StandaloneReplicaSource implements ReplicaSource {
  private List<String>[] replicas;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public StandaloneReplicaSource(Builder builder) {
    List<String> list = StrUtils.splitSmart(builder.shardsParam, ",", true);
    replicas = new List[list.size()];
    for (int i = 0; i < list.size(); i++) {
      replicas[i] = StrUtils.splitSmart(list.get(i), "|", true);
      // todo do we really not need to transform in non-cloud mode?!
      // builder.replicaListTransformer.transform(replicas[i]);
      builder.hostChecker.checkWhitelist(builder.shardsParam, replicas[i]);
    }
  }

  @Override
  public List<String> getSliceNames() {
    // there are no logical slice names in non-cloud
    return Collections.emptyList();
  }

  @Override
  public int getSliceCount() {
    return replicas.length;
  }

  @Override
  public List<String> getReplicasBySlice(int sliceNumber) {
    assert sliceNumber >= 0 && sliceNumber < replicas.length;
    return replicas[sliceNumber];
  }

  static class Builder {
    private String shardsParam;
    private HttpShardHandlerFactory.WhitelistHostChecker hostChecker;

    public Builder shards(String shardsParam) {
      this.shardsParam = shardsParam;
      return this;
    }

    public Builder whitelistHostChecker(HttpShardHandlerFactory.WhitelistHostChecker hostChecker) {
      this.hostChecker = hostChecker;
      return this;
    }

    public StandaloneReplicaSource build() {
      return new StandaloneReplicaSource(this);
    }
  }
}

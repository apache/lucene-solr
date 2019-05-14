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

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

/**
 *
 */
class AffinityReplicaListTransformerFactory implements ReplicaListTransformerFactory {
  private final String defaultDividendParam;
  private final String defaultHashParam;

  public AffinityReplicaListTransformerFactory() {
    this.defaultDividendParam = null;
    this.defaultHashParam = CommonParams.Q;
  }

  public AffinityReplicaListTransformerFactory(String defaultDividendParam, String defaultHashParam) {
    this.defaultDividendParam = defaultDividendParam;
    this.defaultHashParam = defaultHashParam == null ? CommonParams.Q : defaultHashParam;
  }

  public AffinityReplicaListTransformerFactory(NamedList<?> c) {
    this((String)c.get(ShardParams.ROUTING_DIVIDEND), (String)c.get(ShardParams.ROUTING_HASH));
  }

  @Override
  public ReplicaListTransformer getInstance(String configSpec, SolrQueryRequest request, ReplicaListTransformerFactory fallback) {
    ReplicaListTransformer rlt;
    if (configSpec == null) {
      rlt = AffinityReplicaListTransformer.getInstance(defaultDividendParam, defaultHashParam, request);
    } else {
      String[] parts = configSpec.split(":", 2);
      switch (parts[0]) {
        case ShardParams.ROUTING_DIVIDEND:
          rlt = AffinityReplicaListTransformer.getInstance(parts.length == 1 ? defaultDividendParam : parts[1], defaultHashParam, request);
          break;
        case ShardParams.ROUTING_HASH:
          rlt = AffinityReplicaListTransformer.getInstance(null, parts.length == 1 ? defaultHashParam : parts[1], request);
          break;
        default:
          throw new IllegalArgumentException("Invalid routing spec: \"" + configSpec + '"');
      }
    }
    return rlt != null ? rlt : fallback.getInstance(null, request, null);
  }

}

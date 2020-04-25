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
package org.apache.solr.client.solrj.routing;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * Factory for constructing an {@link AffinityReplicaListTransformer} that reorders replica routing
 * preferences deterministically, based on request parameters.
 *
 * Default names of params that contain the values by which routing is determined may be configured
 * at the time of {@link AffinityReplicaListTransformerFactory} construction, and may be
 * overridden by the config spec passed to {@link #getInstance(String, SolrParams, ReplicaListTransformerFactory)}
 *
 * If no defaultHashParam name is specified at time of factory construction, the routing dividend will
 * be derived by hashing the {@link String} value of the {@link CommonParams#Q} param.
 */
public class AffinityReplicaListTransformerFactory implements ReplicaListTransformerFactory {
  private final String defaultDividendParam;
  private final String defaultHashParam;

  public AffinityReplicaListTransformerFactory() {
    this.defaultDividendParam = null;
    this.defaultHashParam = CommonParams.Q;
  }

  public AffinityReplicaListTransformerFactory(String defaultDividendParam, String defaultHashParam) {
    this.defaultDividendParam = defaultDividendParam;
    this.defaultHashParam = defaultHashParam;
  }

  public AffinityReplicaListTransformerFactory(NamedList<?> c) {
    this((String)c.get(ShardParams.ROUTING_DIVIDEND), translateHashParam((String)c.get(ShardParams.ROUTING_HASH)));
  }

  /**
   * Null arg indicates no configuration, which should be translated to the default value {@link CommonParams#Q}.
   * Empty String is translated to null, allowing users to explicitly disable hash-based stable routing.
   *
   * @param hashParam configured hash param (null indicates unconfigured).
   * @return translated value to be used as default hash param in RLT.
   */
  private static String translateHashParam(String hashParam) {
    if (hashParam == null) {
      return CommonParams.Q;
    } else if (hashParam.isEmpty()) {
      return null;
    } else {
      return hashParam;
    }
  }

  @Override
  public ReplicaListTransformer getInstance(String configSpec, SolrParams requestParams, ReplicaListTransformerFactory fallback) {
    ReplicaListTransformer rlt;
    if (configSpec == null) {
      rlt = AffinityReplicaListTransformer.getInstance(defaultDividendParam, defaultHashParam, requestParams);
    } else {
      String[] parts = configSpec.split(":", 2);
      switch (parts[0]) {
        case ShardParams.ROUTING_DIVIDEND:
          rlt = AffinityReplicaListTransformer.getInstance(parts.length == 1 ? defaultDividendParam : parts[1], defaultHashParam, requestParams);
          break;
        case ShardParams.ROUTING_HASH:
          rlt = AffinityReplicaListTransformer.getInstance(null, parts.length == 1 ? defaultHashParam : parts[1], requestParams);
          break;
        default:
          throw new IllegalArgumentException("Invalid routing spec: \"" + configSpec + '"');
      }
    }
    return rlt != null ? rlt : fallback.getInstance(null, requestParams, null);
  }

}

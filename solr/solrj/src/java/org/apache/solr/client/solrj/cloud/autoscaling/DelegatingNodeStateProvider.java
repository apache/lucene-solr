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

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.NodeStateProvider;

/**
 * Base class for overriding some behavior of {@link NodeStateProvider}.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class DelegatingNodeStateProvider implements NodeStateProvider {
  private final NodeStateProvider delegate;

  public DelegatingNodeStateProvider(NodeStateProvider delegate) {
    this.delegate = delegate;
  }

  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
    return delegate.getNodeValues(node, tags);
  }

  @Override
  public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
    return delegate.getReplicaInfo(node, keys);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public boolean isClosed() {
    return delegate.isClosed();
  }
}

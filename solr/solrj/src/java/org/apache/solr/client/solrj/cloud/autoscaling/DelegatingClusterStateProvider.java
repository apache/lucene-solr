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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;

/**
 * Base class for overriding some behavior of {@link ClusterStateProvider}
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class DelegatingClusterStateProvider implements ClusterStateProvider {
  protected ClusterStateProvider delegate;

  public DelegatingClusterStateProvider(ClusterStateProvider delegate) {
    this.delegate = delegate;
  }

  @Override
  public ClusterState.CollectionRef getState(String collection) {
    if (delegate != null) {
      return delegate.getState(collection);
    } else {
      return null;
    }
  }

  @Override
  public Set<String> getLiveNodes() {
    if (delegate != null) {
      return delegate.getLiveNodes();
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public List<String> resolveAlias(String alias) {
    if (delegate != null) {
      return delegate.resolveAlias(alias);
    } else {
      return Collections.singletonList(alias);
    }
  }

  @Override
  public Map<String, String> getAliasProperties(String alias) {
    if (delegate != null) {
      return delegate.getAliasProperties(alias);
    } else {
      return Collections.emptyMap();
    }
  }

  @Override
  public String resolveSimpleAlias(String alias) throws IllegalArgumentException {
    if (delegate != null) {
      return delegate.resolveSimpleAlias(alias);
    } else {
      return alias;
    }
  }

  @Override
  public ClusterState getClusterState() throws IOException {
    if (delegate != null) {
      return delegate.getClusterState();
    } else {
      return null;
    }
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    if (delegate != null) {
      return delegate.getClusterProperties();
    } else {
      return Collections.emptyMap();
    }
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    if (delegate != null) {
      return delegate.getPolicyNameByCollection(coll);
    } else {
      return null;
    }
  }

  @Override
  public DocCollection getCollection(String name) throws IOException {
    ClusterState cs = getClusterState();
    return cs == null ? null : cs.getCollectionOrNull(name);
  }

  @Override
  public void connect() {
    if (delegate != null) {
      delegate.connect();
    }
  }

  @Override
  public void close() throws IOException {
    if (delegate != null) {
      delegate.close();
    }
  }
}

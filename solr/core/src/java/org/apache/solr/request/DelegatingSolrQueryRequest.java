/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.request;

import java.security.Principal;
import java.util.List;
import java.util.Map;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.servlet.HttpSolrCall;
import org.apache.solr.util.RTimerTree;

/**
 * A {@link SolrQueryRequest} implementation that defers to a delegate in all cases.
 *
 * <p>Used primarily in cases where developers want to customize one or more SolrQueryRequest
 * methods while deferring the remainder to an existing instances.
 */
public class DelegatingSolrQueryRequest implements SolrQueryRequest {
  private final SolrQueryRequest delegate;

  public DelegatingSolrQueryRequest(SolrQueryRequest delegate) {
    this.delegate = delegate;
  }

  @Override
  public SolrParams getParams() {
    return delegate.getParams();
  }

  @Override
  public void setParams(SolrParams params) {
    delegate.setParams(params);
  }

  @Override
  public Iterable<ContentStream> getContentStreams() {
    return delegate.getContentStreams();
  }

  @Override
  public SolrParams getOriginalParams() {
    return delegate.getOriginalParams();
  }

  @Override
  public Map<Object, Object> getContext() {
    return delegate.getContext();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public long getStartTime() {
    return delegate.getStartTime();
  }

  @Override
  public RTimerTree getRequestTimer() {
    return delegate.getRequestTimer();
  }

  @Override
  public SolrIndexSearcher getSearcher() {
    return delegate.getSearcher();
  }

  @Override
  public SolrCore getCore() {
    return delegate.getCore();
  }

  @Override
  public IndexSchema getSchema() {
    return delegate.getSchema();
  }

  @Override
  public void updateSchemaToLatest() {
    delegate.updateSchemaToLatest();
  }

  @Override
  public String getParamString() {
    return delegate.getParamString();
  }

  @Override
  public Map<String, Object> getJSON() {
    return delegate.getJSON();
  }

  @Override
  public void setJSON(Map<String, Object> json) {
    delegate.setJSON(json);
  }

  @Override
  public Principal getUserPrincipal() {
    return delegate.getUserPrincipal();
  }

  @Override
  public String getPath() {
    return delegate.getPath();
  }

  @Override
  public Map<String, String> getPathTemplateValues() {
    return delegate.getPathTemplateValues();
  }

  @Override
  public List<CommandOperation> getCommands(boolean validateInput) {
    return delegate.getCommands(validateInput);
  }

  @Override
  public String getHttpMethod() {
    return delegate.getHttpMethod();
  }

  @Override
  public HttpSolrCall getHttpSolrCall() {
    return delegate.getHttpSolrCall();
  }

  @Override
  public CoreContainer getCoreContainer() {
    return delegate.getCoreContainer();
  }

  @Override
  public CloudDescriptor getCloudDescriptor() {
    return delegate.getCloudDescriptor();
  }
}

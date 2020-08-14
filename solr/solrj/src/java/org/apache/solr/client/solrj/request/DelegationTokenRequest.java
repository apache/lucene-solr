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

package org.apache.solr.client.solrj.request;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

/**
 * Class for making Solr delegation token requests.
 *
 * @since Solr 6.2
 */
public abstract class DelegationTokenRequest
    <Q extends DelegationTokenRequest<Q,R>, R extends DelegationTokenResponse>
    extends SolrRequest<R> {

  protected static final String OP_KEY = "op";
  protected static final String TOKEN_KEY = "token";

  public DelegationTokenRequest(METHOD m) {
    // path doesn't really matter -- the filter will respond to any path.
    // setting the path to admin/collections lets us pass through CloudSolrServer
    // without having to specify a collection (that may not even exist yet).
    super(m, "/admin/collections");
  }

  protected abstract Q getThis();

  @Override
  protected abstract R createResponse(SolrClient client);

  public static class Get extends DelegationTokenRequest<Get, DelegationTokenResponse.Get> {
    protected String renewer;

    public Get() {
      this(null);
    }

    public Get(String renewer) {
      super(METHOD.GET);
      this.renewer = renewer;
      setResponseParser(new DelegationTokenResponse.JsonMapResponseParser());
      setQueryParams(new TreeSet<String>(Arrays.asList(OP_KEY)));
    }

    @Override
    protected Get getThis() {
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(OP_KEY, "GETDELEGATIONTOKEN");
      if (renewer != null) params.set("renewer", renewer);
      return params;
    }

    @Override
    public DelegationTokenResponse.Get createResponse(SolrClient client) { return new DelegationTokenResponse.Get(); }
  }

  public static class Renew extends DelegationTokenRequest<Renew, DelegationTokenResponse.Renew> {
    protected String token;

    @Override
    protected Renew getThis() {
      return this;
    }

    public Renew(String token) {
      super(METHOD.PUT);
      this.token = token;
      setResponseParser(new DelegationTokenResponse.JsonMapResponseParser());
      setQueryParams(new TreeSet<String>(Arrays.asList(OP_KEY, TOKEN_KEY)));
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(OP_KEY, "RENEWDELEGATIONTOKEN");
      params.set(TOKEN_KEY, token);
      return params;
    }

    @Override
    public DelegationTokenResponse.Renew createResponse(SolrClient client) { return new DelegationTokenResponse.Renew(); }
  }

  public static class Cancel extends DelegationTokenRequest<Cancel, DelegationTokenResponse.Cancel> {
    protected String token;

    public Cancel(String token) {
      super(METHOD.PUT);
      this.token = token;
      setResponseParser(new NoOpResponseParser());
      Set<String> queryParams = new TreeSet<String>();
      setQueryParams(new TreeSet<String>(Arrays.asList(OP_KEY, TOKEN_KEY)));
    }

    @Override
    protected Cancel getThis() {
      return this;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(OP_KEY, "CANCELDELEGATIONTOKEN");
      params.set(TOKEN_KEY, token);
      return params;
    }

    @Override
    public DelegationTokenResponse.Cancel createResponse(SolrClient client) { return new DelegationTokenResponse.Cancel(); }
  }
}

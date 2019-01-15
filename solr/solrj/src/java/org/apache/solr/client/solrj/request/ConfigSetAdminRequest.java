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

import java.util.Map;
import java.util.Properties;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.params.ConfigSetParams.ConfigSetAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * This class is experimental and subject to change.
 *
 * @since solr 5.4
 */
public abstract class ConfigSetAdminRequest
      <Q extends ConfigSetAdminRequest<Q,R>, R extends ConfigSetAdminResponse>
      extends SolrRequest<R> {

  protected ConfigSetAction action = null;

  protected ConfigSetAdminRequest setAction(ConfigSetAction action) {
    this.action = action;
    return this;
  }

  public ConfigSetAdminRequest() {
    super(METHOD.GET, "/admin/configs");
  }

  public ConfigSetAdminRequest(String path) {
    super (METHOD.GET, path);
  }

  protected abstract Q getThis();

  @Override
  public SolrParams getParams() {
    if (action == null) {
      throw new RuntimeException( "no action specified!" );
    }
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(ConfigSetParams.ACTION, action.toString());
    return params;
  }


  @Override
  protected abstract R createResponse(SolrClient client);

  protected abstract static class ConfigSetSpecificAdminRequest
       <T extends ConfigSetAdminRequest<T,ConfigSetAdminResponse>>
       extends ConfigSetAdminRequest<T,ConfigSetAdminResponse> {
    protected String configSetName = null;

    public final T setConfigSetName(String configSetName) {
      this.configSetName = configSetName;
      return getThis();
    }

    public final String getConfigSetName() {
      return configSetName;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (configSetName == null) {
        throw new RuntimeException( "no ConfigSet specified!" );
      }
      params.set(NAME, configSetName);
      return params;
    }

    @Override
    protected ConfigSetAdminResponse createResponse(SolrClient client) {
      return new ConfigSetAdminResponse();
    }
  }

  // CREATE request
  public static class Create extends ConfigSetSpecificAdminRequest<Create> {
    protected static String PROPERTY_PREFIX = "configSetProp";
    protected String baseConfigSetName;
    protected Properties properties;

    public Create() {
      action = ConfigSetAction.CREATE;
    }

    @Override
    protected Create getThis() {
      return this;
    }

    public final Create setBaseConfigSetName(String baseConfigSetName) {
      this.baseConfigSetName = baseConfigSetName;
      return getThis();
    }

    public final String getBaseConfigSetName() {
      return baseConfigSetName;
    }

    public final Create setNewConfigSetProperties(Properties properties) {
      this.properties = properties;
      return getThis();
    }

    public final Properties getNewConfigSetProperties() {
      return properties;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (baseConfigSetName != null) {
        params.set("baseConfigSet", baseConfigSetName);
      }
      if (properties != null) {
        for (Map.Entry entry : properties.entrySet()) {
          params.set(PROPERTY_PREFIX + "." + entry.getKey().toString(),
              entry.getValue().toString());
        }
      }
      return params;
    }
  }

  // DELETE request
  public static class Delete extends ConfigSetSpecificAdminRequest<Delete> {
    public Delete() {
      action = ConfigSetAction.DELETE;
    }

    @Override
    protected Delete getThis() {
      return this;
    }
  }

  // LIST request
  public static class List extends ConfigSetAdminRequest<List, ConfigSetAdminResponse.List> {
    public List() {
      action = ConfigSetAction.LIST;
    }

    @Override
    protected List getThis() {
      return this;
    }

    @Override
    protected ConfigSetAdminResponse.List createResponse(SolrClient client) {
      return new ConfigSetAdminResponse.List();
    }
  }
}

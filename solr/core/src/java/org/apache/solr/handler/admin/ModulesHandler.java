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
package org.apache.solr.handler.admin;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.bundle.PluginBundles;
import ro.fortsoft.pf4j.PluginWrapper;
import ro.fortsoft.pf4j.update.PluginInfo;
import ro.fortsoft.pf4j.update.UpdateRepository;

import static org.apache.solr.common.SolrException.ErrorCode.INVALID_STATE;

/**
 * @since solr 7.0
 */
public class ModulesHandler extends RequestHandlerBase
{
  private PluginBundles pluginBundles;

  public void initializeModules(PluginBundles pluginBundles) {
    this.pluginBundles = pluginBundles;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception
  {
    SolrParams params = req.getParams();
    if (pluginBundles == null) {
      throw new SolrException(INVALID_STATE, "Not initialized");
    }
    pluginBundles.getUpdateManager().refresh();
    pluginBundles.getPluginManager().loadPlugins();
    pluginBundles.getPluginManager().startPlugins();
    rsp.add( "modules", getModulesInfo(pluginBundles));
    rsp.setHttpCaching(false);
  }

  private SimpleOrderedMap<Object> getModulesInfo(PluginBundles pluginBundles)
  {
    SimpleOrderedMap<Object> map = new SimpleOrderedMap<>();
    List<Object> repositories = new ArrayList<>();
    List<Object> installed = new ArrayList<>();
    List<Object> available = new ArrayList<>();
    List<Object> updates = new ArrayList<>();
    map.add("repositories", repositories);
    map.add("installed", installed);
    map.add("available", available);
    map.add("updates", updates);

    for (UpdateRepository r : pluginBundles.getUpdateManager().getRepositories()) {
      SimpleOrderedMap<Object> repo = new SimpleOrderedMap<>();
      repo.add("id", r.getId());
      repo.add("location", r.getUrl());
      repositories.add(repo);
    }

    for (PluginWrapper p : pluginBundles.listInstalled()) {
      SimpleOrderedMap<Object> desc = new SimpleOrderedMap<>();
      desc.add("id", p.getPluginId());
      desc.add("path", p.getPluginPath().toString());
      desc.add("version", p.getDescriptor().getVersion().toString());
      desc.add("description", p.getDescriptor().getPluginDescription());
      desc.add("state", p.getPluginState().toString());
      installed.add(desc);
    }

    for (PluginInfo p : pluginBundles.query(null)) {
      available.add(pluginInfoToMap(p));
    }

    for (PluginInfo p : pluginBundles.getUpdateManager().getUpdates()) {
      SimpleOrderedMap<Object> pi = pluginInfoToMap(p);
      if (pluginBundles.getPluginManager().getPlugin(p.id) != null) {
        pi.add("installedVersion", pluginBundles.getPluginManager().getPlugin(p.id).getDescriptor().getVersion().toString());
      }
      updates.add(pi);
    }
    return map;
  }

  private SimpleOrderedMap<Object> pluginInfoToMap(PluginInfo p) {
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();
    info.add("id", p.id);
    info.add("name", p.name);
    info.add("description", p.description);
    info.add("projectUrl", p.projectUrl);
    info.add("provider", p.provider);
    info.add("version", p.getLastRelease(pluginBundles.getPluginManager().getSystemVersion()).version);
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    info.add("date", df.format(p.getLastRelease(pluginBundles.getPluginManager().getSystemVersion()).date));
    info.add("url", p.getLastRelease(pluginBundles.getPluginManager().getSystemVersion()).url);
    return info;
  }


  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Registry";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

}

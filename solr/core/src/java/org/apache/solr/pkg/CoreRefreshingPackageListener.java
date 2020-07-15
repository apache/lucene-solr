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

package org.apache.solr.pkg;

import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;

/**A utility class that loads classes from packages and reloads core if any of those packages are updated
 *
 */
public class CoreRefreshingPackageListener implements PackageListeners.Listener {
  private final SolrCore solrCore;
  private final PluginInfo info;
  private final PackageLoader.Package.Version version;

  public CoreRefreshingPackageListener(SolrCore solrCore, PluginInfo info, PackageLoader.Package.Version version) {
    this.solrCore = solrCore;
    this.info = info;
    this.version = version;
  }

  @Override
  public String packageName() {
    return info.cName.pkg;
  }

  public SolrResourceLoader getLoader() {
    return version.getLoader();
  }
  @Override
  public PluginInfo pluginInfo() {
    return info;
  }

  @Override
  public void changed(PackageLoader.Package pkg, Ctx ctx) {
    PackageLoader.Package.Version version = pkg.getLatest(solrCore.getSolrConfig().maxPackageVersion(info.cName.pkg));
    if(version != this.version) {
      ctx.runLater(CoreRefreshingPackageListener.class.getSimpleName()+"/"+ SolrCore.class.getName(),
              () -> solrCore.getCoreContainer().reload(CoreRefreshingPackageListener.class.getName() , solrCore.uniqueId));
    }
  }

  @Override
  public PackageLoader.Package.Version getPackageVersion() {
    return version;
  }

  public static <T> T createInst(SolrResourceLoader srl, PluginInfo info, Class<T> type) {
    return srl.newInstance(info, type,true);
    }


}

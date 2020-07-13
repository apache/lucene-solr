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

import java.util.function.Function;

import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;

/**A utility class that loads classes from packages and reloads core if any of those packages are updated
 *
 */
public class CoreRefreshingClassLoader implements PackageListeners.Listener {
  private final SolrCore solrCore;
  private final PluginInfo info;
  private final PackageLoader.Package.Version version;

  public CoreRefreshingClassLoader(SolrCore solrCore, PluginInfo info, PackageLoader.Package.Version version) {
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
      ctx.runLater(SolrCore.class.getName(), () -> solrCore.getCoreContainer().reload(CoreRefreshingClassLoader.class.getName() , solrCore.uniqueId));
    }
  }

  @Override
  public PackageLoader.Package.Version getPackageVersion() {
    return version;
  }

  /**
   * Load a class using an appropriate {@link SolrResourceLoader} depending of the package of that class
   */
  public static <T> Class<? extends T> findClass(SolrResourceLoader srl, PluginInfo info, Class<T>  type) {
    if(info.cName.pkg == null) return srl.findClass(info.className, type);
    return _get(srl, info,
            (Function<PackageLoader.Package.Version, Class<? extends T>>) ver -> ver.getLoader().findClass(info.cName.className, type));

  }

  private static  <T> T _get(SolrResourceLoader srl, PluginInfo info, Function<PackageLoader.Package.Version, T> fun) {
    PluginInfo.ClassName cName = info.cName;
    SolrCore core = srl.getCore();
    PackageLoader.Package.Version latest = srl.getCoreContainer().getPackageLoader().getPackage(cName.pkg)
            .getLatest(core.getSolrConfig().maxPackageVersion(cName.pkg));
    T result = fun.apply(latest);
    if(srl.getCore() !=null) {
      srl.getCore().getPackageListeners().addListener(new CoreRefreshingClassLoader(core, info, latest));
    }
    return result;
  }

  public static <T> T createInst(SolrResourceLoader srl, PluginInfo info, Class<T> type) {
    if(info.cName.pkg == null) {
        return srl.newInstance(info.cName.className == null?
                type.getName():
                info.cName.className ,
                type);
    }
    return _get(srl, info, version -> version.getLoader().newInstance(info.cName.className, type));
  }

}

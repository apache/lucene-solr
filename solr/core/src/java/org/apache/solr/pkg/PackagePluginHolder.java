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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Map;

import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackagePluginHolder<T> extends PluginBag.PluginHolder<T> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String LATEST = "$LATEST";

  private final SolrCore core;
  private final SolrConfig.SolrPluginInfo pluginMeta;
  private PackageLoader.Package.Version pkgVersion;
  private PluginInfo info;


  public PackagePluginHolder(PluginInfo info, SolrCore core, SolrConfig.SolrPluginInfo pluginMeta) {
    super(info);
    this.core = core;
    this.pluginMeta = pluginMeta;
    this.info = info;

    reload(core.getCoreContainer().getPackageLoader().getPackage(info.pkgName));
    core.getPackageListeners().addListener(new PackageListeners.Listener() {
      @Override
      public String packageName() {
        return info.pkgName;
      }

      @Override
      public Map<String, PackageAPI.PkgVersion> packageDetails() {
        return Collections.singletonMap(info.cName.original, pkgVersion.getPkgVersion());
      }

      @Override
      public void changed(PackageLoader.Package pkg, Ctx ctx) {
        reload(pkg);

      }

      @Override
      public MapWriter getPackageVersion(PluginInfo.ClassName cName) {
        return pkgVersion == null ? null : ew -> pkgVersion.writeMap(ew);
      }

    });
  }

  public static <T> PluginBag.PluginHolder<T> createHolder(T inst,  Class<T> type) {
    SolrConfig.SolrPluginInfo plugin = SolrConfig.classVsSolrPluginInfo.get(type.getName());
    PluginInfo info = new PluginInfo(plugin.tag, Collections.singletonMap("class", inst.getClass().getName()));
    return new PluginBag.PluginHolder<T>(info,inst);
  }

  public static <T> PluginBag.PluginHolder<T> createHolder(PluginInfo info, SolrCore core, Class<T> type, String msg) {
    if(info.cName.pkg == null) {
      return new PluginBag.PluginHolder<T>(info, core.createInitInstance(info, type,msg, null));
    } else {
      return new PackagePluginHolder<T>(info, core, SolrConfig.classVsSolrPluginInfo.get(type.getName()));
    }
  }

  private synchronized void reload(PackageLoader.Package pkg) {
    String lessThan = core.getSolrConfig().maxPackageVersion(info.pkgName);
    PackageLoader.Package.Version newest = pkg.getLatest(lessThan);
    if (newest == null) {
      log.error("No latest version available for package : {}", pkg.name());
      return;
    }
    if (lessThan != null) {
      PackageLoader.Package.Version pkgLatest = pkg.getLatest();
      if (pkgLatest != newest) {
        if (log.isInfoEnabled()) {
          log.info("Using version :{}. latest is {},  params.json has config {} : {}", newest.getVersion(), pkgLatest.getVersion(), pkg.name(), lessThan);
        }
      }
    }

    if (pkgVersion != null) {
      if (newest == pkgVersion) {
        //I'm already using the latest classloader in the package. nothing to do
        return;
      }
    }

    if (log.isInfoEnabled()) {
      log.info("loading plugin: {} -> {} using  package {}:{}",
              pluginInfo.type, pluginInfo.name, pkg.name(), newest.getVersion());
    }

    initNewInstance(newest);
    pkgVersion = newest;

  }

  @SuppressWarnings({"unchecked"})
  protected Object initNewInstance(PackageLoader.Package.Version newest) {
    Object instance = SolrCore.createInstance(pluginInfo.className,
            pluginMeta.clazz, pluginMeta.getCleanTag(), core, newest.getLoader());
    PluginBag.initInstance(instance, pluginInfo);
    handleAwareCallbacks(newest.getLoader(), instance);
    T old = inst;
    inst = (T) instance;
    if (old instanceof AutoCloseable) {
      AutoCloseable closeable = (AutoCloseable) old;
      try {
        closeable.close();
      } catch (Exception e) {
        log.error("error closing plugin", e);
      }
    }
    return inst;
  }

  private void handleAwareCallbacks(SolrResourceLoader loader, Object instance) {
    if (instance instanceof SolrCoreAware) {
      SolrCoreAware coreAware = (SolrCoreAware) instance;
      if (!core.getResourceLoader().addToCoreAware(coreAware)) {
        coreAware.inform(core);
      }
    }
    if (instance instanceof ResourceLoaderAware) {
      SolrResourceLoader.assertAwareCompatibility(ResourceLoaderAware.class, instance);
      try {
        ((ResourceLoaderAware) instance).inform(loader);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
    if (instance instanceof SolrInfoBean) {
      core.getResourceLoader().addToInfoBeans(instance);
    }
  }

}
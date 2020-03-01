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

import java.lang.invoke.MethodHandles;

import org.apache.solr.core.PluginBag;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.RequestParams;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
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
      public PluginInfo pluginInfo() {
        return info;
      }

      @Override
      public void changed(PackageLoader.Package pkg, PackageListeners.Ctx ctx) {
        reload(pkg);

      }

      @Override
      public PackageAPI.PkgVersion getPackageVersion() {
        return pkgVersion.getVersionInfo();
      }

    });
  }

  private static String maxAllowedVersion(RequestParams requestParams, String pkgName) {
    RequestParams.ParamSet p = requestParams.getParams(PackageListeners.PACKAGE_VERSIONS);
    if (p == null) {
      return null;
    }
    Object o = p.get().get(pkgName);
    if (o == null || LATEST.equals(o)) return null;
    return o.toString();
  }


  private synchronized void reload(PackageLoader.Package pkg) {
    PackageLoader.Package.Version rightVersion = getRightVersion(pkg, core.getSolrConfig().getRequestParams());
    if (pkgVersion != null) {
      if (rightVersion == pkgVersion) {
        //I'm already using the latest classloader in the package. nothing to do
        return ;
      }
    }
    if (rightVersion == null) return;

    log.info("loading plugin: {} -> {} using  package {}:{}",
        pluginInfo.type, pluginInfo.name, pkg.name(), rightVersion.getVersion());

    initNewInstance(rightVersion);
    pkgVersion = rightVersion;

  }

  public static PackageLoader.Package.Version getRightVersion(PackageLoader.Package pkg, RequestParams requestParams) {
    String lessThan = maxAllowedVersion(requestParams, pkg.name);
    PackageLoader.Package.Version newest = pkg.getLatest(lessThan);
    if (newest == null) {
      log.error("No latest version available for package : {}", pkg.name());
      return null;
    }
    if (lessThan != null) {
      PackageLoader.Package.Version pkgLatest = pkg.getLatest();
      if (pkgLatest != newest) {
        log.info("Using version :{}. latest is {},  params.json has config {} : {}", newest.getVersion(), pkgLatest.getVersion(), pkg.name(), lessThan);
      }
    }


    return newest;
  }

  protected void initNewInstance(PackageLoader.Package.Version newest) {
    Object instance = SolrCore.createInstance(pluginInfo.className,
        pluginMeta.clazz, pluginMeta.getCleanTag(), core, newest.getLoader());
    PluginBag.initInstance(instance, pluginInfo);
    if (instance instanceof SolrCoreAware) {
      core.getResourceLoader().registerSolrCoreAware((SolrCoreAware) instance);
    }
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
  }

}
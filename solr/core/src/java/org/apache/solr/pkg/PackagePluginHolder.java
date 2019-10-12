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
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackagePluginHolder<T> extends PluginBag.PluginHolder<T> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrCore core;
  private final SolrConfig.SolrPluginInfo pluginMeta;
  private PackageLoader.Package aPackage;
  private PackageLoader.Package.Version pkgVersion;


  public PackagePluginHolder(PluginInfo info, SolrCore core, SolrConfig.SolrPluginInfo pluginMeta) {
    super(info);
    this.core = core;
    this.pluginMeta = pluginMeta;

    reload(aPackage = core.getCoreContainer().getPackageLoader().getPackage(info.pkgName));
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
      public void changed(PackageLoader.Package pkg) {
        reload(pkg);

      }

      @Override
      public PackageLoader.Package.Version getPackageVersion() {
        return pkgVersion;
      }

    });
  }


  private synchronized void reload(PackageLoader.Package pkg) {
    if(pkgVersion != null && aPackage.getLatest() == pkgVersion ) return;

    if (inst != null) log.info("reloading plugin {} ", pluginInfo.name);
    PackageLoader.Package.Version newest = pkg.getLatest();
    if(newest == null) return;
    Object instance = SolrCore.createInstance(pluginInfo.className,
        pluginMeta.clazz, pluginMeta.getCleanTag(), core, newest.getLoader());
    PluginBag.initInstance(instance, pluginInfo);
    T old = inst;
    inst = (T) instance;
    pkgVersion = newest;
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
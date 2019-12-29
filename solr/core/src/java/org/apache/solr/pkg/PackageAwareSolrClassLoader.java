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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrClassLoader;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.plugin.SolrCoreAware;

import static java.util.Collections.singletonMap;

/**
 * This class implements a {@link SolrClassLoader} that can  identify the correct packages
 * and load classes from that. This also listens to any changes to the relevant packages and
 * invoke a callback if anything is modified
 */
public class PackageAwareSolrClassLoader implements SolrClassLoader {
  final SolrCore core;
  final SolrResourceLoader loader;
  private Map<String, PackageAPI.PkgVersion> classNameVsPkg = new HashMap<>();

  private final List<PackageListeners.Listener> listeners = new ArrayList<>();
  private final Runnable reloadRunnable;


  /**
   *
   * @param core The core where this belong to
   * @param runnable run a task if something is modified, say reload schema or reload core, refresh cache or something else
   */
  public PackageAwareSolrClassLoader(SolrCore core,  Runnable runnable) {
    this.core = core;
    this.loader = core.getResourceLoader();
    this.reloadRunnable = runnable;
  }

  SolrCore getCore() {
    return core;
  }

  @Override
  public InputStream openResource(String resource) throws IOException {
    return loader.openResource(resource);
  }

  @Override
  public <T> T newInstance(String cname, Class<T> expectedType) {
    return loadWithRightPackageLoader(cname, expectedType,
        (pkgloader, name) -> pkgloader.newInstance(name, expectedType));
  }

  @Override
  public <T> T newInstance(String cname, Class<T> expectedType, String... subpackages) {
    return loadWithRightPackageLoader(cname, expectedType,
        (pkgloader, name) -> pkgloader.newInstance(name, expectedType, subpackages));
  }

  private <T> T loadWithRightPackageLoader(String cname, Class expectedType, BiFunction<SolrClassLoader, String, T> fun) {
    PluginInfo.ParsedClassName parsedClassName = new PluginInfo.ParsedClassName(cname);
    if (parsedClassName.pkg == null) {
      return  fun.apply(loader, parsedClassName.klas);
    } else {
      PackageLoader.Package pkg = core.getCoreContainer().getPackageLoader().getPackage(parsedClassName.pkg);
      PackageLoader.Package.Version ver = PackagePluginHolder.getRightVersion(pkg, core);
      T result = fun.apply(ver.getLoader(), parsedClassName.klas);
      if (result instanceof SolrCoreAware) {
        loader.registerSolrCoreAware((SolrCoreAware) result);
      }
      classNameVsPkg.put(cname, ver.getVersionInfo());
      PackageListeners.Listener listener = new PackageListener(expectedType, cname, parsedClassName);
      listeners.add(listener);
      core.getPackageListeners().addListener(listener);
      return result;
    }
  }

  @Override
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
    return loadWithRightPackageLoader(cname, expectedType, (BiFunction<SolrClassLoader, String ,Class<? extends T>>) (loader, name) -> loader.findClass(name, expectedType));
  }

  @Override
  public <T> T newInstance(String cName, Class<T> expectedType, String[] subPackages, Class[] params, Object[] args) {
    return loadWithRightPackageLoader(cName, expectedType, (pkgloader, name) -> pkgloader.newInstance(name, expectedType, subPackages, params, args));
  }



  @Override
  public void close() throws IOException {
    for (PackageListeners.Listener l : listeners) {
      core.getPackageListeners().removeListener(l);
    }
  }

  private class PackageListener implements PackageListeners.Listener {

    private final String cname;
    private final PluginInfo.ParsedClassName parsedClassName;
    PluginInfo info;

    public PackageListener(Class expectedType, String cname, PluginInfo.ParsedClassName parsedClassName) {
      this.cname = cname;
      this.parsedClassName = parsedClassName;
      info = new PluginInfo(expectedType.getSimpleName(), singletonMap("class", cname));
    }

    @Override
    public String packageName() {
      return parsedClassName.pkg;
    }

    @Override
    public PluginInfo pluginInfo() {
      return info;
    }

    @Override
    public void changed(PackageLoader.Package pkg, PackageListeners.Ctx ctx) {
      PackageLoader.Package.Version rightVersion = PackagePluginHolder.getRightVersion(pkg, core);
      if (rightVersion == null ) return;
      PackageAPI.PkgVersion v = classNameVsPkg.get(parsedClassName.toString());
      if(Objects.equals(v.version ,rightVersion.getVersionInfo().version)) return; //nothing has changed no need to reload
      Runnable old = ctx.getPostProcessor(PackageAwareSolrClassLoader.class.getName());// just want to do one refresh for every package laod
      if (old == null) ctx.addPostProcessor(PackageAwareSolrClassLoader.class.getName(), reloadRunnable);
    }

    @Override
    public PackageAPI.PkgVersion getPackageVersion() {
      return classNameVsPkg.get(cname);
    }
  }
}

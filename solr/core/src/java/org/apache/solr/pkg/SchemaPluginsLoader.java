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

import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.solr.common.MapWriter;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrClassLoader;
import org.apache.solr.core.SolrResourceLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class SchemaPluginsLoader implements SolrClassLoader {
    final CoreContainer coreContainer;
    final SolrResourceLoader loader;
    final Function<String, String> pkgVersionSupplier;
    private Map<String ,PackageAPI.PkgVersion> packageVersions;
    private Map<String, String> classNameVsPkg;
    private final Runnable onReload;

    public SchemaPluginsLoader(CoreContainer coreContainer,
                               SolrResourceLoader loader,
                               Function<String, String> pkgVersionSupplier,
                               Runnable onReload) {
        this.coreContainer = coreContainer;
        this.loader = loader;
        this.pkgVersionSupplier = pkgVersionSupplier;
        this.onReload = () -> {
            SchemaPluginsLoader.this.packageVersions = new HashMap<>();
            classNameVsPkg = new HashMap<>();
            onReload.run();
        };
    }


    @Override
    public <T> T newInstance(String cname, Class<T> expectedType, String... subpackages) {
        PluginInfo.ClassName cName = new PluginInfo.ClassName(cname);
        if(cName.pkg == null){
            return loader.newInstance(cname, expectedType, subpackages);
        } else {
            PackageLoader.Package.Version version = findPkgVersion(cName);
            return applyResourceLoaderAware(version, version.getLoader().newInstance(cName.className, expectedType, subpackages));

        }
    }

    private PackageLoader.Package.Version findPkgVersion(PluginInfo.ClassName cName) {
        PackageLoader.Package.Version theVersion = coreContainer.getPackageLoader().getPackage(cName.pkg).getLatest(pkgVersionSupplier.apply(cName.pkg));
        if(packageVersions == null) packageVersions = new HashMap<>();
        packageVersions.put(cName.pkg, theVersion.getPkgVersion());
        if(classNameVsPkg == null) classNameVsPkg = new HashMap<>();
        classNameVsPkg.put(cName.toString(),cName.pkg);
        return theVersion;
    }
    public MapWriter getVersionInfo(String className) {
        if(packageVersions == null || classNameVsPkg == null) return null;
        PackageAPI.PkgVersion p = packageVersions.get(classNameVsPkg.get(className));
        return p == null? null: p::writeMap;
    }


    private <T> T applyResourceLoaderAware(PackageLoader.Package.Version version, T t) {
        if (t instanceof ResourceLoaderAware) {
            try {
                ((ResourceLoaderAware) t).inform(version.getLoader());
                return t;
            } catch (IOException e) {
                throw new RuntimeException(e);
                //RODO handle exception

            }
        }
        return t;
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public <T> T newInstance(String cname, Class<T> expectedType, String[] subPackages, Class[] params, Object[] args) {
        PluginInfo.ClassName cName = new PluginInfo.ClassName(cname);
        if (cName.pkg == null) {
            return loader.newInstance(cname, expectedType, subPackages, params, args);
        } else {
            PackageLoader.Package.Version version = findPkgVersion(cName);
            return applyResourceLoaderAware(version, version.getLoader().newInstance(cName.className, expectedType, subPackages, params, args));
        }
    }

    @Override
    public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
        PluginInfo.ClassName cName = new PluginInfo.ClassName(cname);
        if (cName.pkg == null) {
            return loader.findClass(cname, expectedType);
        } else {
            PackageLoader.Package.Version version = findPkgVersion(cName);
            return version.getLoader().findClass(cName.className, expectedType);

        }
    }

    public final PackageListeners.Listener listener = new PackageListeners.Listener() {
        @Override
        public String packageName() {
            return null;
        }

        @Override
        public PluginInfo pluginInfo() {
            return null;
        }

        @Override
        public void changed(PackageLoader.Package pkg, Ctx ctx) {
            if(packageVersions == null) return;
            PackageAPI.PkgVersion  currVer = packageVersions.get(pkg.name);
            if(currVer == null) {
                //not watching this
                return;
            }
            String latestSupportedVersion = pkgVersionSupplier.apply(pkg.name);
            if(latestSupportedVersion == null) {
                //no specific version configured. use the latest
                latestSupportedVersion = pkg.getLatest().getVersion();
            }
            if(Objects.equals(currVer.version, latestSupportedVersion)) {
                //no need to update
                return;
            }
            ctx.runLater(SchemaPluginsLoader.class.getName(), onReload);
        }

        @Override
        public PackageLoader.Package.Version getPackageVersion() {
            return null;
        }
    };


}

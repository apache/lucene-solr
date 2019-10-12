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
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackageLoader {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer coreContainer;
  private final Map<String, Package> packageClassLoaders = new ConcurrentHashMap<>();

  private PackageAPI.Packages myCopy;

  private PackageAPI packageAPI;


  public PackageLoader(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    packageAPI = new PackageAPI(coreContainer, this);
    myCopy = packageAPI.pkgs;

  }

  public PackageAPI getPackageAPI() {
    return packageAPI;
  }

  public Package getPackage(String key) {
    return packageClassLoaders.get(key);
  }

  public Map<String, Package> getPackages() {
    return Collections.EMPTY_MAP;
  }

  public void refreshPackageConf() {
    log.info("{} updated to version {}", ZkStateReader.SOLR_PKGS_PATH, packageAPI.pkgs.znodeVersion);

    List<Package> updated = new ArrayList<>();
    Map<String, List<PackageAPI.PkgVersion>> modified = getModified(myCopy, packageAPI.pkgs);
    for (Map.Entry<String, List<PackageAPI.PkgVersion>> e : modified.entrySet()) {
      if (e.getValue() != null) {
        Package p = packageClassLoaders.get(e.getKey());
        if (e.getValue() != null && p == null) {
          packageClassLoaders.put(e.getKey(), p = new Package(e.getKey()));
        }
        p.updateVersions(e.getValue());
        updated.add(p);
      } else {
        Package p = packageClassLoaders.remove(e.getKey());
        if (p != null) {
          //other classes are holding to a reference to this objecec
          // they should know that this is removed
          p.markDeleted();
        }
      }
    }
    for (SolrCore core : coreContainer.getCores()) {
      core.getPackageListeners().packagesUpdated(updated);

    }
  }

  public Map<String, List<PackageAPI.PkgVersion>> getModified(PackageAPI.Packages old, PackageAPI.Packages newPkgs) {
    Map<String, List<PackageAPI.PkgVersion>> changed = new HashMap<>();
    for (Map.Entry<String, List<PackageAPI.PkgVersion>> e : newPkgs.packages.entrySet()) {
      List<PackageAPI.PkgVersion> versions = old.packages.get(e.getKey());
      if (versions != null) {
        if (!Objects.equals(e.getValue(), versions)) {
          changed.put(e.getKey(), e.getValue());
        }
      } else {
        changed.put(e.getKey(), e.getValue());
      }
    }
    //some packages are deleted altogether
    for (String s : old.packages.keySet()) {
      if (!newPkgs.packages.keySet().contains(s)) {
        changed.put(s, null);
      }
    }

    return changed;

  }

  public SolrResourceLoader getResourceLoader(String pkg, String version) {
    return null;
  }


  public class Package {
    final String name;
    final Map<String, Version> myVersions = new ConcurrentHashMap<>();
    private List<String> sortedVersions = new CopyOnWriteArrayList<>();
    String latest;
    private boolean deleted;


    public Package(String name) {
      this.name = name;
    }

    public boolean isDeleted() {
      return deleted;
    }


    private synchronized void updateVersions(List<PackageAPI.PkgVersion> modified) {
      for (PackageAPI.PkgVersion v : modified) {
        Version version = myVersions.get(v.version);
        if (version == null) {
          myVersions.put(v.version, new Version(this, v));
          sortedVersions.add(v.version);
        }
      }

      Set<String> newVersions = new HashSet<>();
      for (PackageAPI.PkgVersion v : modified) {
        newVersions.add(v.version);
      }
      for (String s : new HashSet<>(myVersions.keySet())) {
        if (!newVersions.contains(s)) {
          sortedVersions.remove(s);
          myVersions.remove(s);
        }
      }

      sortedVersions.sort(String::compareTo);
      if (sortedVersions.size() > 0) {
        latest = sortedVersions.get(sortedVersions.size() - 1);
      } else {
        latest = null;
      }

    }


    public Version getLatest() {
      return latest == null ? null : myVersions.get(latest);
    }

    public Version getLatest(String lessThan) {
      String latest = null;
      for (String v : (ArrayList<String>) new ArrayList(sortedVersions)) {
        if (v.compareTo(lessThan) < 1) {
          latest = v;
        } else break;

      }


      return latest == null ? null : myVersions.get(latest);
    }

    public String name() {
      return name;
    }

    private void markDeleted() {
      deleted = true;
      myVersions.clear();
      sortedVersions.clear();
      latest = null;

    }

    public class Version implements MapWriter {
      private final Package parent;
      private SolrResourceLoader loader;

      private final PackageAPI.PkgVersion version;

      @Override
      public void writeMap(EntryWriter ew) throws IOException {
        ew.put("package", parent.name());
        version.writeMap(ew);
      }

      public Version(Package parent, PackageAPI.PkgVersion v) {
        this.parent = parent;
        this.version = v;
        List<Path> paths = new ArrayList<>();
        for (String file : version.files) {
          paths.add(coreContainer.getPackageStoreAPI().getPackageStore().getRealpath(file));
        }

        try {
          loader = new SolrResourceLoader(
              "PACKAGE_LOADER:"+ parent.name()+ ":"+ version,
              paths,
              coreContainer.getResourceLoader().getInstancePath(),
              coreContainer.getResourceLoader().getClassLoader());
        } catch (MalformedURLException e) {
          log.error("Could not load classloader ", e);
        }
      }

      public String getVersion() {
        return version.version;
      }

      public Collection getFiles() {
        return Collections.unmodifiableList(version.files);
      }

      public SolrResourceLoader getLoader() {
        return loader;

      }

    }
  }


}

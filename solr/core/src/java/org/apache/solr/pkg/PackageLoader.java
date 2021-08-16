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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

import static org.apache.lucene.util.IOUtils.closeWhileHandlingException;

/**
 * The class that holds a mapping of various packages and classloaders
 */
public class PackageLoader implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String LATEST = "$LATEST";


  private final CoreContainer coreContainer;
  private final Map<String, Package> packageClassLoaders = new ConcurrentHashMap<>();

  private PackageAPI.Packages myCopy =  new PackageAPI.Packages();

  private PackageAPI packageAPI;


  public Optional<Package.Version> getPackageVersion(String pkg, String version) {
    Package p = packageClassLoaders.get(pkg);
    if(p == null) return Optional.empty();
    return Optional.ofNullable(p.getVersion(version));
  }

  public PackageLoader(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    packageAPI = new PackageAPI(coreContainer, this);
    refreshPackageConf();

  }

  public PackageAPI getPackageAPI() {
    return packageAPI;
  }

  public Package getPackage(String key) {
    return packageClassLoaders.get(key);
  }

  @SuppressWarnings({"unchecked"})
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
          //other classes are holding to a reference to this object
          // they should know that this is removed
          p.markDeleted();
          closeWhileHandlingException(p);
        }
      }
    }
    for (SolrCore core : coreContainer.getCores()) {
      core.getPackageListeners().packagesUpdated(updated);
    }
    myCopy = packageAPI.pkgs;
  }

  public Map<String, List<PackageAPI.PkgVersion>> getModified(PackageAPI.Packages old, PackageAPI.Packages newPkgs) {
    Map<String, List<PackageAPI.PkgVersion>> changed = new HashMap<>();
    for (Map.Entry<String, List<PackageAPI.PkgVersion>> e : newPkgs.packages.entrySet()) {
      List<PackageAPI.PkgVersion> versions = old.packages.get(e.getKey());
      if (versions != null) {
        if (!Objects.equals(e.getValue(), versions)) {
          if (log.isInfoEnabled()) {
            log.info("Package {} is modified ", e.getKey());
          }
          changed.put(e.getKey(), e.getValue());
        }
      } else {
        if (log.isInfoEnabled()) {
          log.info("A new package: {} introduced", e.getKey());
        }
        changed.put(e.getKey(), e.getValue());
      }
    }
    //some packages are deleted altogether
    for (String s : old.packages.keySet()) {
      if (!newPkgs.packages.keySet().contains(s)) {
        log.info("Package: {} is removed althogether", s);
        changed.put(s, null);
      }
    }

    return changed;

  }

  public void notifyListeners(String pkg) {
    Package p = packageClassLoaders.get(pkg);
    if (p != null) {
      List<Package> l = Collections.singletonList(p);
      for (SolrCore core : coreContainer.getCores()) {
        core.getPackageListeners().packagesUpdated(l);
      }
    }
  }

  /**
   * represents a package definition in the packages.json
   */
  public class Package implements Closeable {
    final String name;
    final Map<String, Version> myVersions = new ConcurrentHashMap<>();
    private List<String> sortedVersions = new CopyOnWriteArrayList<>();
    String latest;
    private boolean deleted;


    Package(String name) {
      this.name = name;
    }

    public boolean isDeleted() {
      return deleted;
    }

    public Set<String> allVersions() {
      return myVersions.keySet();
    }


    private synchronized void updateVersions(List<PackageAPI.PkgVersion> modified) {
      for (PackageAPI.PkgVersion v : modified) {
        Version version = myVersions.get(v.version);
        if (version == null) {
          log.info("A new version: {} added for package: {} with artifacts {}", v.version, this.name, v.files);
          Version ver = null;
          try {
            ver = new Version(this, v);
          } catch (Exception e) {
            log.error("package could not be loaded {}", ver, e);
            continue;
          }
          myVersions.put(v.version, ver);
          sortedVersions.add(v.version);
        }
      }

      Set<String> newVersions = new HashSet<>();
      for (PackageAPI.PkgVersion v : modified) {
        newVersions.add(v.version);
      }
      for (String s : new HashSet<>(myVersions.keySet())) {
        if (!newVersions.contains(s)) {
          log.info("version: {} is removed from package: {}", s, this.name);
          sortedVersions.remove(s);
          Version removed = myVersions.remove(s);
          if (removed != null) {
            closeWhileHandlingException(removed);
          }
        }
      }

      sortedVersions.sort(String::compareTo);
      if (sortedVersions.size() > 0) {
        String latest = sortedVersions.get(sortedVersions.size() - 1);
        if (!latest.equals(this.latest)) {
          log.info("version: {} is the new latest in package: {}", latest, this.name);
        }
        this.latest = latest;
      } else {
        log.error("latest version:  null");
        latest = null;
      }

    }


    public Version getLatest() {
      return latest == null ? null : myVersions.get(latest);
    }

    public Version getVersion(String version) {
      if(version == null) return getLatest();
      return myVersions.get(version);
    }

    public Version getLatest(String lessThan) {
      if (lessThan == null) {
        return getLatest();
      }
      String latest = findBiggest(lessThan, new ArrayList<>(sortedVersions));
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

    @Override
    public void close() throws IOException {
      for (Version v : myVersions.values()) v.close();
    }

    public class Version implements MapWriter, Closeable {
      private final Package parent;
      private SolrResourceLoader loader;

      private final PackageAPI.PkgVersion version;

      @Override
      public void writeMap(EntryWriter ew) throws IOException {
        ew.put("package", parent.name());
        version.writeMap(ew);
      }

      Version(Package parent, PackageAPI.PkgVersion v) {
        this.parent = parent;
        this.version = v;
        List<Path> paths = new ArrayList<>();

        List<String> errs = new ArrayList<>();
        coreContainer.getPackageStoreAPI().validateFiles(version.files, true, s -> errs.add(s));
        if(!errs.isEmpty()) {
          throw new RuntimeException("Cannot load package: " +errs);
        }
        for (String file : version.files) {
          paths.add(coreContainer.getPackageStoreAPI().getPackageStore().getRealpath(file));
        }

        loader = new PackageResourceLoader(
            "PACKAGE_LOADER: " + parent.name() + ":" + version,
            paths,
            Paths.get(coreContainer.getSolrHome()),
            coreContainer.getResourceLoader().getClassLoader());
      }

      public String getVersion() {
        return version.version;
      }
      public PackageAPI.PkgVersion getPkgVersion(){
        return version.copy();
      }

      @SuppressWarnings({"rawtypes"})
      public Collection getFiles() {
        return Collections.unmodifiableList(version.files);
      }

      public SolrResourceLoader getLoader() {
        return loader;
      }

      @Override
      public void close() throws IOException {
        if (loader != null) {
          closeWhileHandlingException(loader);
        }
      }

      @Override
      public String toString() {
        return jsonStr();
      }
    }
  }
  static class PackageResourceLoader extends SolrResourceLoader {
    List<Path> paths;


    PackageResourceLoader(String name, List<Path> classpath, Path instanceDir, ClassLoader parent) {
      super(name, classpath, instanceDir, parent);
      this.paths = classpath;
    }

    @Override
    public <T> boolean addToCoreAware(T obj) {
      //do not do anything
      //this class is not aware of a SolrCore and it is totally not tied to
      // the lifecycle of SolrCore. So, this returns 'false' & it should be
      // taken care of by the caller
      return false;
    }

    @Override
    public <T> boolean addToResourceLoaderAware(T obj) {
      // do not do anything
      // this should be invoked only after the init() is invoked.
      // The caller should take care of that
      return false;
    }

    @Override
    public  <T> void addToInfoBeans(T obj) {
      //do not do anything. It should be handled externally
    }

    @Override
    public InputStream openResource(String resource) {
      return getClassLoader().getResourceAsStream(resource);
    }
  }

  @SuppressWarnings("CompareToZero") // TODO either document why or fix this
  private static String findBiggest(String lessThan, List<String> sortedList) {
    String latest = null;
    for (String v : sortedList) {
      if (v.compareTo(lessThan) < 1) {
        latest = v;
      } else break;
    }
    return latest;
  }

  @Override
  public void close()  {
    for (Package p : packageClassLoaders.values()) closeWhileHandlingException(p);
  }
}

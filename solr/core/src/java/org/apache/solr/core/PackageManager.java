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

package org.apache.solr.core;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterPropertiesListener;
import org.apache.solr.common.util.Base64;
import org.apache.solr.util.CryptoKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.PACKAGES;
import static org.apache.solr.common.params.CommonParams.VERSION;
import static org.apache.solr.core.RuntimeLib.SHA256;

/**
 * This class listens to changes to packages and it also keeps a
 * registry of the resource loader instances and the metadata of the package
 * The resource loader is supposed to be in sync with the data in Zookeeper
 * and it will always have one and only one instance of the resource loader
 * per package in a given node. These resource loaders are shared across
 * all components in a Solr node
 * <p>
 * when packages are created/updated new resource loaders are created and if there are
 * listeners, they are notified. They can in turn choose to discard old instances of plugins
 * loaded from old resource loaders and create new instances if required.
 * <p>
 * All the resource loaders are loaded from blobs that exist in the {@link FsBlobStore}
 */
public class PackageManager implements ClusterPropertiesListener {
  public static final boolean enablePackage = Boolean.parseBoolean(System.getProperty("enable.package", "false"));
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final CoreContainer coreContainer;

  private Map<String, PackageResourceLoader> pkgs = new HashMap<>();

  final PackageListeners listenerRegistry = new PackageListeners();

  int myVersion = -1;


  public int getZNodeVersion(String pkg) {
    PackageResourceLoader p = pkgs.get(pkg);
    return p == null ? -1 : p.packageInfo.znodeVersion;
  }


  public PackageInfo getPackageInfo(String pkg) {
    PackageResourceLoader p = pkgs.get(pkg);
    return p == null ? null : p.packageInfo;
  }


  public static class PackageInfo implements MapWriter {
    public final String name;
    public final String version;
    public final List<Blob> blobs;
    public final int znodeVersion;
    public List<String> oldBlob;
    public final String manifest;

    public PackageInfo(Map m, int znodeVersion) {
      name = (String) m.get(NAME);
      version = (String) m.get(VERSION);
      manifest = (String) m.get("manifest");
      this.znodeVersion = znodeVersion;
      Object o = m.get("blob");
      if (o instanceof Map) {
        Map map = (Map) o;
        this.blobs = ImmutableList.of(new Blob(map));
      } else {
        throw new RuntimeException("Invalid type for attribute blob");
      }
    }

    public List<String> validate(CoreContainer coreContainer) throws Exception {
      List<String> errors = new ArrayList<>();
      if (!enablePackage) {
        errors.add("node not started with -Denable.package=true");
        return errors;
      }
      Map<String, byte[]> keys = CloudUtil.getTrustedKeys(
          coreContainer.getZkController().getZkClient(), "exe");
      if (keys.isEmpty()) {
        errors.add("No public keys in ZK : /keys/exe");
        return errors;
      }
      CryptoKeys cryptoKeys = new CryptoKeys(keys);
      for (Blob blob : blobs) {
        if (!blob.verifyJar(cryptoKeys, coreContainer)) {
          errors.add("Invalid signature for blob : " + blob.sha256);
        }
      }
      return errors;

    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put("name", name);
      ew.put("version", version);
      ew.put("manifest", manifest);
      ew.putIfNotNull("blobs.old", oldBlob);
      if (blobs.size() == 1) {
        ew.put("blob", blobs.get(0));
      } else {
        ew.put("blobs", blobs);
      }
    }


    @Override
    public boolean equals(Object obj) {
      if (obj instanceof PackageInfo) {
        PackageInfo that = (PackageInfo) obj;
        if (!Objects.equals(this.version, that.version)) return false;
        if (this.blobs.size() == that.blobs.size()) {
          for (int i = 0; i < blobs.size(); i++) {
            if (!Objects.equals(blobs.get(i), that.blobs.get(i))) {
              return false;
            }
          }
        } else {
          return false;
        }
      } else {
        return false;
      }
      return true;
    }

    PackageResourceLoader createPackage(PackageManager packageManager) {
      return new PackageResourceLoader(packageManager, this);
    }

    public static class Blob implements MapWriter {
      public final String sha256;
      public final String sig;
      public final String name;

      public Blob(Object o) {
        if (o instanceof Map) {
          Map m = (Map) o;
          this.sha256 = (String) m.get(SHA256);
          this.sig = (String) m.get("sig");
          this.name = (String) m.get(NAME);
        } else {
          throw new RuntimeException("blob should be a Object Type");
        }
      }

      @Override
      public void writeMap(EntryWriter ew) throws IOException {
        ew.put(SHA256, sha256);
        ew.put("sig", sig);
        ew.putIfNotNull(NAME, name);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof Blob) {
          Blob that = (Blob) obj;
          return Objects.equals(this.sha256, that.sha256) && Objects.equals(this.sig, that.sig);
        } else {
          return false;
        }
      }

      public boolean verifyJar(CryptoKeys cryptoKeys, CoreContainer coreContainer) throws IOException {
        boolean[] result = new boolean[]{false};
        for (Map.Entry<String, PublicKey> e : cryptoKeys.keys.entrySet()) {
          coreContainer.getBlobStore().readBlob(sha256, is -> {
            try {
              if (CryptoKeys.verify(e.getValue(), Base64.base64ToByteArray(sig), is)) result[0] = true;
            } catch (Exception ex) {
              log.error("Unexpected error in verifying jar", ex);
            }
          });
        }
        return result[0];

      }
    }

  }

  public static class PackageResourceLoader extends SolrResourceLoader implements MapWriter {
    final PackageInfo packageInfo;

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      packageInfo.writeMap(ew);
    }

    PackageResourceLoader(PackageManager packageManager, PackageInfo packageInfo) {
      super(packageManager.coreContainer.getResourceLoader().getInstancePath(),
          packageManager.coreContainer.getResourceLoader().classLoader);
      this.packageInfo = packageInfo;
      List<URL> blobURLs = new ArrayList<>(packageInfo.blobs.size());
      for (PackageInfo.Blob blob : packageInfo.blobs) {
        try {
          if (!packageManager.coreContainer.getBlobStore().fetchBlobToFS(blob.sha256)) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Blob not available " + blob.sha256);
          }
          blobURLs.add(new File(packageManager.coreContainer.getBlobStore().getBlobsPath().toFile(), blob.sha256).toURI().toURL());
        } catch (MalformedURLException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
      }
      addToClassLoader(blobURLs);
    }
  }

  PackageManager(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }


  public <T> T newInstance(String cName, Class<T> expectedType, String pkg) {
    PackageResourceLoader p = pkgs.get(pkg);
    if (p == null) {
      return coreContainer.getResourceLoader().newInstance(cName, expectedType);
    } else {
      return p.newInstance(cName, expectedType);
    }
  }

  @Override
  public boolean onChange(Map<String, Object> properties) {
    log.debug("clusterprops.json changed , version {}", coreContainer.getZkController().getZkStateReader().getClusterPropsVersion());
    int v = coreContainer.getZkController().getZkStateReader().getClusterPropsVersion();
    List<PackageInfo> touchedPackages = updatePackages(properties, v);

    if (!touchedPackages.isEmpty()) {
      Collection<SolrCore> cores = coreContainer.getCores();

      log.info(" {} cores being notified of updated packages  : {}",cores.size() ,touchedPackages.stream().map(p -> p.name).collect(Collectors.toList()) );
      for (SolrCore core : cores) {
        core.getListenerRegistry().packagesUpdated(touchedPackages);
      }
      listenerRegistry.packagesUpdated(touchedPackages);

    }
    coreContainer.getContainerRequestHandlers().updateReqHandlers(properties);
    myVersion = v;
    return false;
  }


  private List<PackageInfo> updatePackages(Map<String, Object> properties, int ver) {
    Map m = (Map) properties.getOrDefault(PACKAGES, Collections.emptyMap());
    if (pkgs.isEmpty() && m.isEmpty()) return Collections.emptyList();
    Map<String, PackageInfo> reloadPackages = new HashMap<>();
    m.forEach((k, v) -> {
      if (v instanceof Map) {
        PackageInfo info = new PackageInfo((Map) v, ver);
        PackageResourceLoader pkg = pkgs.get(k);
        if (pkg == null || !pkg.packageInfo.equals(info)) {
          reloadPackages.put(info.name, info);
        }
      }
    });
    pkgs.forEach((name, aPackage) -> {
      if (!m.containsKey(name)) reloadPackages.put(name, null);
    });

    if (!reloadPackages.isEmpty()) {
      List<PackageInfo> touchedPackages = new ArrayList<>();
      Map<String, PackageResourceLoader> newPkgs = new HashMap<>(pkgs);
      reloadPackages.forEach((s, pkgInfo) -> {
        if (pkgInfo == null) {
          newPkgs.remove(s);
        } else {
          newPkgs.put(s, pkgInfo.createPackage(PackageManager.this));
          touchedPackages.add(pkgInfo);
        }
      });
      this.pkgs = newPkgs;
      return touchedPackages;

    }
    return Collections.emptyList();
  }

  public ResourceLoader getResourceLoader(String pkg) {
    PackageResourceLoader loader = pkgs.get(pkg);
    return loader == null ? coreContainer.getResourceLoader() : loader;
  }

}

package org.apache.solr.packagemanager;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.packagemanager.SolrPluginInfo.SolrPluginRelease;
import org.apache.solr.packagemanager.pf4j.CompoundVerifier;
import org.apache.solr.packagemanager.pf4j.DefaultUpdateRepository;
import org.apache.solr.packagemanager.pf4j.FileDownloader;
import org.apache.solr.packagemanager.pf4j.FileVerifier;
import org.apache.solr.packagemanager.pf4j.LenientDateTypeAdapter;
import org.apache.solr.packagemanager.pf4j.SimpleFileDownloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

public class SolrUpdateRepository {
  private static final Logger log = LoggerFactory.getLogger(DefaultUpdateRepository.class);

  private String id;
  private URL url;

  private String pluginsJsonFileName;

  public String getPluginsJsonFileName() {
    if (pluginsJsonFileName == null) {
      pluginsJsonFileName = "plugins.json";
    }

    return pluginsJsonFileName;
  }

  public void refresh() {
    packages = null;
  }


  public FileDownloader getFileDownloader() {
      return new SimpleFileDownloader();
  }

  /**
   * Gets a file verifier to execute on the downloaded file for it to be claimed valid.
   * May be a CompoundVerifier in order to chain several verifiers.
   * @return list of {@link FileVerifier}s
   */
  public FileVerifier getFileVerfier() {
      return new CompoundVerifier();
  }

  public String getId() {
      return id;
  }

  public URL getUrl() {
      return url;
  }

  @Expose(serialize = false, deserialize = true) 
  private Map<String, SolrPluginInfo> packages;

  public SolrUpdateRepository(String id, URL url) {
    //this.id = id;
    //this.url = url;
  }

  public Map<String, SolrPluginInfo> getPlugins() {
    if (packages == null) {
      initPlugins();
    }

    return packages;
  }

  public SolrPluginInfo getPlugin(String id) {
    return getPlugins().get(id);
  }

  private void initPlugins() {
    Reader pluginsJsonReader;
    try {
      URL pluginsUrl = new URL(getUrl(), getPluginsJsonFileName());
      log.debug("Read plugins of '{}' repository from '{}'", getId(), pluginsUrl);
      pluginsJsonReader = new InputStreamReader(pluginsUrl.openStream());
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      packages = Collections.emptyMap();
      return;
    }

    Gson gson = new GsonBuilder().
        registerTypeAdapter(Date.class, new LenientDateTypeAdapter()).create();
    SolrPluginInfo[] items = gson.fromJson(pluginsJsonReader, SolrPluginInfo[].class);
    packages = new HashMap<>(items.length);
    for (SolrPluginInfo p : items) {
      for (SolrPluginRelease r : p.versions) {
        try {
          r.url = new URL(getUrl(), r.url).toString();
          if (r.date.getTime() == 0) {
            log.warn("Illegal release date when parsing {}@{}, setting to epoch", p.id, r.version);
          }
        } catch (MalformedURLException e) {
          log.warn("Skipping release {} of plugin {} due to failure to build valid absolute URL. Url was {}{}", r.version, p.id, getUrl(), r.url);
        }
      }
      p.setRepositoryId(getId());
      packages.put(p.id, p);
      
      System.out.println("****\n"+p+"\n*******");
    }
    log.debug("Found {} plugins in repository '{}'", packages.size(), getId());
  }
}

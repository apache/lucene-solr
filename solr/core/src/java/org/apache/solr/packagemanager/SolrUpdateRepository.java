package org.apache.solr.packagemanager;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.packagemanager.SolrPluginInfo.SolrPluginRelease;
import org.pf4j.update.DefaultUpdateRepository;
import org.pf4j.update.PluginInfo;
import org.pf4j.update.PluginInfo.PluginRelease;
import org.pf4j.update.util.LenientDateTypeAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

public class SolrUpdateRepository extends DefaultUpdateRepository {
  private static final Logger log = LoggerFactory.getLogger(DefaultUpdateRepository.class);

  //private String id;
  //private URL url;

  @Expose(serialize = false, deserialize = true) 
  private Map<String, PluginInfo> packages;

  public SolrUpdateRepository(String id, URL url) {
    super(id, url);
    //this.id = id;
    //this.url = url;
  }

  @Override
  public Map<String, PluginInfo> getPlugins() {
    if (packages == null) {
      initPlugins();
    }

    return packages;
  }

  @Override
  public PluginInfo getPlugin(String id) {
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
      p.releases = new ArrayList<>(); // nocommit
      for (SolrPluginRelease r: p.versions) {
        p.releases.add(r);
      }

      for (PluginRelease r : p.releases) {
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

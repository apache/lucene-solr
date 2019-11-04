package org.apache.solr.packagemanager;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.packagemanager.SolrPackage.SolrPackageRelease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SolrPackageRepository {
  private static final Logger log = LoggerFactory.getLogger(SolrPackageRepository.class);

  @JsonProperty("id")
  public String id;
  @JsonProperty("url")
  public String url;

  public SolrPackageRepository() {
  }//nocommit wtf?

  public SolrPackageRepository(String id, String url) {
    this.id = id;
    this.url = url;
  }

  public void refresh() {
    packages = null;
  }

  @JsonIgnore
  private Map<String, SolrPackage> packages;

  public Map<String, SolrPackage> getPackages() {
    if (packages == null) {
      initPackages();
    }

    return packages;
  }

  public SolrPackage getPackage(String id) {
    return getPackages().get(id);
  }

  public boolean hasPackage(String packageName) {
    return getPackages().containsKey(packageName);
  }

  public Path download(URL url) throws SolrException, IOException {
    Path tmpDirectory = Files.createTempDirectory("solr-packages");
    tmpDirectory.toFile().deleteOnExit();
    String fileName = url.getPath().substring(url.getPath().lastIndexOf('/') + 1);
    Path destination = tmpDirectory.resolve(fileName);

    switch (url.getProtocol()) {
      case "http":
      case "https":
      case "ftp":
        FileUtils.copyURLToFile(url, destination.toFile());
        break;
      case "file":
        try {
          FileUtils.copyFile(new File(url.toURI()), destination.toFile());
        } catch (URISyntaxException e) {
          throw new SolrException(ErrorCode.INVALID_STATE, e);
        }
        break;
      default:
        throw new SolrException(ErrorCode.BAD_REQUEST, "URL protocol " + url.getProtocol() + " not supported");
    }
    
    return destination;
  }

  private void initPackages() {
    Reader pluginsJsonReader;
    try {
      URL pluginsUrl = new URL(new URL(url), "manifest.json"); //nocommit hardcoded
      log.debug("Read plugins of '{}' repository from '{}'", id, pluginsUrl);
      pluginsJsonReader = new InputStreamReader(pluginsUrl.openStream());
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      packages = Collections.emptyMap();
      return;
    }

    ObjectMapper mapper = new ObjectMapper();
    SolrPackage items[];
    try {
      items = mapper.readValue(pluginsJsonReader, SolrPackage[].class);
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
    packages = new HashMap<>(items.length);
    for (SolrPackage p : items) {
      for (SolrPackageRelease r : p.versions) {
        try {
          r.url = new URL(new URL(url), r.url).toString();
          if (r.date.getTime() == 0) {
            log.warn("Illegal release date when parsing {}@{}, setting to epoch", p.id, r.version);
          }
        } catch (MalformedURLException e) {
          log.warn("Skipping release {} of plugin {} due to failure to build valid absolute URL. Url was {}{}", r.version, p.id, url, r.url);
        }
      }
      p.setRepositoryId(id);
      packages.put(p.id, p);

      System.out.println("****\n"+p+"\n*******");
    }
    log.debug("Found {} plugins in repository '{}'", packages.size(), id);
  }
}

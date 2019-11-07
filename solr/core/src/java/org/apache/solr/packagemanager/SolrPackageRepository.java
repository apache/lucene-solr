package org.apache.solr.packagemanager;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.packagemanager.SolrPackage.SolrPackageRelease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SolrPackageRepository {
  private static final Logger log = LoggerFactory.getLogger(SolrPackageRepository.class);

  @JsonProperty("name")
  public String name;
  @JsonProperty("url")
  public String url;

  public SolrPackageRepository() {
  }//nocommit wtf?

  public SolrPackageRepository(String repositoryName, String url) {
    this.name = repositoryName;
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

  public SolrPackage getPackage(String packageName) {
    return getPackages().get(packageName);
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
    try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
      SolrPackage[] items = PackageUtils.getJson(client, url + "/repository.json", SolrPackage[].class);

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
        p.setRepository(name);
        packages.put(p.id, p);
      }
    } catch (IOException ex) {
      throw new SolrException(ErrorCode.INVALID_STATE, ex);
    }
    log.debug("Found {} packages in repository '{}'", packages.size(), name);
  }
}

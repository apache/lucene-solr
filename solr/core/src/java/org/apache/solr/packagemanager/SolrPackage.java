package org.apache.solr.packagemanager;


import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes a package (along with all released versions) as it appears in a repository.
 */
public class SolrPackage implements Serializable, Comparable<SolrPackage> {

  public String id;
  public String description;
  public List<SolrPackageRelease> versions;

  private String repository;

  public static class SolrPackageRelease {

    public String version;
    public Date date;
    public String url;

    public String sha512sum;
    public String sig;

    public Manifest manifest;
    @Override
    public String toString() {
      return "SolrPackageRelease{" +
          "version='" + version + '\'' +
          ", date=" + date +
          ", url='" + url + '\'' +
          ", sig='" + sig + '\'' +
          ", min='" + manifest.minSolrVersion + '\'' +
          ", max='" + manifest.maxSolrVersion + '\'' +
          ", dependencies='" + manifest.dependencies + '\'' +
          ", plugins='" + manifest.plugins + '\'' +
          ", paramDefaults='" + manifest.parameterDefaults + '\'' +
          ", sha512sum='" + sha512sum + '\'' +
          '}';
    }
  }

  public static class Manifest {
    @JsonProperty("min-solr-version")
    public String minSolrVersion;
    @JsonProperty("max-solr-version")
    public String maxSolrVersion;

    public List<String> dependencies;
    public List<Plugin> plugins;
    @JsonProperty("parameter-defaults")
    public Map<String, String> parameterDefaults;
  }

  public static class Plugin {
    public String id;
    @JsonProperty("setup-command")
    public String setupCommand;

    @JsonProperty("update-command")
    public String updateCommand;

    @JsonProperty("uninstall-command")
    public String uninstallCommand;

    @JsonProperty("verify-command")
    public Command verifyCommand;

    @Override
    public String toString() {
      return id + ": {setup: "+setupCommand+", update: "+updateCommand+", uninstall: "+uninstallCommand+", verify: "+verifyCommand+"}";
    }
  }

  @Override
  public int compareTo(SolrPackage o) {
    return id.compareTo(o.id);
  }

  public String getRepository() {
    return repository;
  }

  public void setRepository(String repository) {
    this.repository = repository;
  }

  public static class Command {
    public String path;
    public String method;
    public Map payload;
    public String condition;
    public String expected;
    
    @Override
      public String toString() {
        return method + " " + path + ", Payload: "+ payload+", Condition: "+condition+", expected: "+expected;
      }
  }
}


package org.apache.solr.packagemanager;


import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.ReflectMapWriter;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes a package (along with all released versions) as it appears in a repository.
 */
public class SolrPackage implements Comparable<SolrPackage> {

  public String name;
  public String description;
  public List<SolrPackageRelease> versions;

  private String repository;

  public static class SolrPackageRelease implements ReflectMapWriter {
    public String version;
    public Date date;
    public List<Artifact> artifacts;
    public Manifest manifest;

    @Override
    public String toString() {
      return jsonStr();
    }
  }

  public static class Artifact {
    public String url;
    public String sig;
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

  public static class Plugin implements ReflectMapWriter {
    public String name;
    @JsonProperty("setup-command")
    public Command setupCommand;

    @JsonProperty("uninstall-command")
    public Command uninstallCommand;

    @JsonProperty("verify-command")
    public Command verifyCommand;

    @Override
    public String toString() {
      return jsonStr();
    }
  }

  @Override
  public int compareTo(SolrPackage o) {
    return name.compareTo(o.name);
  }

  public String getRepository() {
    return repository;
  }

  public void setRepository(String repository) {
    this.repository = repository;
  }

  public static class Command implements ReflectMapWriter {
    public String path;
    public String method;
    public Map<String, Object> payload;
    public String condition;
    public String expected;
    
    @Override
      public String toString() {
        return jsonStr();
      }
  }
}


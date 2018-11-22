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
package org.apache.lucene.validation;

import org.apache.ivy.Ivy;
import org.apache.ivy.core.LogOptions;
import org.apache.ivy.core.report.ResolveReport;
import org.apache.ivy.core.resolve.ResolveOptions;
import org.apache.ivy.core.settings.IvySettings;
import org.apache.ivy.plugins.conflict.NoConflictManager;
import org.apache.lucene.dependencies.InterpolatedProperties;
import org.apache.lucene.validation.ivyde.IvyNodeElement;
import org.apache.lucene.validation.ivyde.IvyNodeElementAdapter;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.Resource;
import org.apache.tools.ant.types.ResourceCollection;
import org.apache.tools.ant.types.resources.FileResource;
import org.apache.tools.ant.types.resources.Resources;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An Ant task to verify that the '/org/name' keys in ivy-versions.properties
 * are sorted lexically and are neither duplicates nor orphans, and that all
 * dependencies in all ivy.xml files use rev="${/org/name}" format.
 */
public class LibVersionsCheckTask extends Task {

  private static final String IVY_XML_FILENAME = "ivy.xml";
  private static final Pattern COORDINATE_KEY_PATTERN = Pattern.compile("(/([^/ \t\f]+)/([^=:/ \t\f]+))");
  private static final Pattern BLANK_OR_COMMENT_LINE_PATTERN = Pattern.compile("[ \t\f]*(?:[#!].*)?");
  private static final Pattern TRAILING_BACKSLASH_PATTERN = Pattern.compile("[^\\\\]*(\\\\+)$");
  private static final Pattern LEADING_WHITESPACE_PATTERN = Pattern.compile("[ \t\f]+(.*)");
  private static final Pattern WHITESPACE_GOODSTUFF_WHITESPACE_BACKSLASH_PATTERN
      = Pattern.compile("[ \t\f]*(.*?)(?:(?<!\\\\)[ \t\f]*)?\\\\");
  private static final Pattern TRAILING_WHITESPACE_BACKSLASH_PATTERN
      = Pattern.compile("(.*?)(?:(?<!\\\\)[ \t\f]*)?\\\\");
  private static final Pattern MODULE_NAME_PATTERN = Pattern.compile("\\smodule\\s*=\\s*[\"']([^\"']+)[\"']");
  private static final Pattern MODULE_DIRECTORY_PATTERN 
      = Pattern.compile(".*[/\\\\]((?:lucene|solr)[/\\\\].*)[/\\\\].*");
  private Ivy ivy;

  /**
   * All ivy.xml files to check.
   */
  private Resources ivyXmlResources = new Resources();

  /**
   * Centralized Ivy versions properties file: ivy-versions.properties
   */
  private File centralizedVersionsFile;

  /**
   * Centralized Ivy ignore conflicts file: ivy-ignore-conflicts.properties
   */
  private File ignoreConflictsFile;

  /**
   * Ivy settings file: top-level-ivy-settings.xml
   */
  private File topLevelIvySettingsFile;

  /**
   * Location of common build dir: lucene/build/
   */
  private File commonBuildDir;

  /**
   * Location of ivy cache resolution directory.
   */
  private File ivyResolutionCacheDir;
  
  /**
   * Artifact lock strategy that Ivy should use.
   */
  private String ivyLockStrategy;
  
  /**
   * A logging level associated with verbose logging.
   */
  private int verboseLevel = Project.MSG_VERBOSE;
  
  /**
   * All /org/name keys found in ivy-versions.properties,
   * mapped to info about direct dependence and what would
   * be conflicting indirect dependencies if Lucene/Solr
   * were to use transitive dependencies.
   */
  private Map<String,Dependency> directDependencies = new LinkedHashMap<>();

  /**
   * All /org/name keys found in ivy-ignore-conflicts.properties,
   * mapped to the set of indirect dependency versions that will
   * be ignored, i.e. not trigger a conflict.
   */
  private Map<String,HashSet<String>> ignoreConflictVersions = new HashMap<>();

  private static class Dependency {
    String org;
    String name;
    String directVersion;
    String latestVersion;
    boolean directlyReferenced = false;
    LinkedHashMap<IvyNodeElement,Set<String>> conflictLocations = new LinkedHashMap<>(); // dependency path -> moduleNames
    
    Dependency(String org, String name, String directVersion) {
      this.org = org;
      this.name = name;
      this.directVersion = directVersion;
    }
  }
  
  /**
   * Adds a set of ivy.xml resources to check.
   */
  public void add(ResourceCollection rc) {
    ivyXmlResources.add(rc);
  }

  public void setVerbose(boolean verbose) {
    verboseLevel = (verbose ? Project.MSG_INFO : Project.MSG_VERBOSE);
  }

  public void setCentralizedVersionsFile(File file) {
    centralizedVersionsFile = file;
  }

  public void setTopLevelIvySettingsFile(File file) {
    topLevelIvySettingsFile = file;
  }

  public void setIvyResolutionCacheDir(File dir) {
    ivyResolutionCacheDir = dir;
  }
  
  public void setIvyLockStrategy(String strategy) {
    this.ivyLockStrategy = strategy;
  }

  public void setCommonBuildDir(File file) {
    commonBuildDir = file;
  }
  
  public void setIgnoreConflictsFile(File file) {
    ignoreConflictsFile = file;
  }

  /**
   * Execute the task.
   */
  @Override
  public void execute() throws BuildException {
    log("Starting scan.", verboseLevel);
    long start = System.currentTimeMillis();

    setupIvy();

    int numErrors = 0;
    if ( ! verifySortedCoordinatesPropertiesFile(centralizedVersionsFile)) {
      ++numErrors;
    }
    if ( ! verifySortedCoordinatesPropertiesFile(ignoreConflictsFile)) {
      ++numErrors;
    }
    collectDirectDependencies();
    if ( ! collectVersionConflictsToIgnore()) {
      ++numErrors;
    }

    int numChecked = 0;

    @SuppressWarnings("unchecked")
    Iterator<Resource> iter = (Iterator<Resource>)ivyXmlResources.iterator();
    while (iter.hasNext()) {
      final Resource resource = iter.next();
      if ( ! resource.isExists()) {
        throw new BuildException("Resource does not exist: " + resource.getName());
      }
      if ( ! (resource instanceof FileResource)) {
        throw new BuildException("Only filesystem resources are supported: " 
            + resource.getName() + ", was: " + resource.getClass().getName());
      }

      File ivyXmlFile = ((FileResource)resource).getFile();
      try {
        if ( ! checkIvyXmlFile(ivyXmlFile)) {
          ++numErrors;
        }
        if ( ! resolveTransitively(ivyXmlFile)) {
          ++numErrors;
        }
        if ( ! findLatestConflictVersions()) {
          ++numErrors;
        }
      } catch (Exception e) {
        throw new BuildException("Exception reading file " + ivyXmlFile.getPath() + " - " + e.toString(), e);
      }
      ++numChecked;
    }

    log("Checking for orphans in " + centralizedVersionsFile.getName(), verboseLevel);
    for (Map.Entry<String,Dependency> entry : directDependencies.entrySet()) {
      String coordinateKey = entry.getKey();
      if ( ! entry.getValue().directlyReferenced) {
        log("ORPHAN coordinate key '" + coordinateKey + "' in " + centralizedVersionsFile.getName()
            + " is not found in any " + IVY_XML_FILENAME + " file.",
            Project.MSG_ERR);
        ++numErrors;
      }
    }

    int numConflicts = emitConflicts();

    int messageLevel = numErrors > 0 ? Project.MSG_ERR : Project.MSG_INFO;
    log("Checked that " + centralizedVersionsFile.getName() + " and " + ignoreConflictsFile.getName()
        + " have lexically sorted '/org/name' keys and no duplicates or orphans.",
        messageLevel);
    log("Scanned " + numChecked + " " + IVY_XML_FILENAME + " files for rev=\"${/org/name}\" format.",
        messageLevel);
    log("Found " + numConflicts + " indirect dependency version conflicts.");
    log(String.format(Locale.ROOT, "Completed in %.2fs., %d error(s).",
                      (System.currentTimeMillis() - start) / 1000.0, numErrors),
        messageLevel);

    if (numConflicts > 0 || numErrors > 0) {
      throw new BuildException("Lib versions check failed. Check the logs.");
    }
  }

  private boolean findLatestConflictVersions() {
    boolean success = true;
    StringBuilder latestIvyXml = new StringBuilder();
    latestIvyXml.append("<ivy-module version=\"2.0\">\n");
    latestIvyXml.append("  <info organisation=\"org.apache.lucene\" module=\"core-tools-find-latest-revision\"/>\n");
    latestIvyXml.append("  <configurations>\n");
    latestIvyXml.append("    <conf name=\"default\" transitive=\"false\"/>\n");
    latestIvyXml.append("  </configurations>\n");
    latestIvyXml.append("  <dependencies>\n");
    for (Map.Entry<String, Dependency> directDependency : directDependencies.entrySet()) {
      Dependency dependency = directDependency.getValue();
      if (dependency.conflictLocations.entrySet().isEmpty()) {
        continue;
      }
      latestIvyXml.append("    <dependency org=\"");
      latestIvyXml.append(dependency.org);
      latestIvyXml.append("\" name=\"");
      latestIvyXml.append(dependency.name);
      latestIvyXml.append("\" rev=\"latest.release\" conf=\"default->*\"/>\n");
    }
    latestIvyXml.append("  </dependencies>\n");
    latestIvyXml.append("</ivy-module>\n");
    File buildDir = new File(commonBuildDir, "ivy-transitive-resolve");
    if ( ! buildDir.exists() && ! buildDir.mkdirs()) {
      throw new BuildException("Could not create temp directory " + buildDir.getPath());
    }
    File findLatestIvyXmlFile = new File(buildDir, "find.latest.conflicts.ivy.xml");
    try {
      try (Writer writer = new OutputStreamWriter(new FileOutputStream(findLatestIvyXmlFile), StandardCharsets.UTF_8)) {
        writer.write(latestIvyXml.toString());
      }
      ResolveOptions options = new ResolveOptions();
      options.setDownload(false);           // Download only module descriptors, not artifacts
      options.setTransitive(false);         // Resolve only direct dependencies
      options.setUseCacheOnly(false);       // Download the internet!
      options.setOutputReport(false);       // Don't print to the console
      options.setLog(LogOptions.LOG_QUIET); // Don't log to the console
      options.setConfs(new String[] {"*"}); // Resolve all configurations
      ResolveReport resolveReport = ivy.resolve(findLatestIvyXmlFile.toURI().toURL(), options);
      IvyNodeElement root = IvyNodeElementAdapter.adapt(resolveReport);
      for (IvyNodeElement element : root.getDependencies()) {
        String coordinate = "/" + element.getOrganization() + "/" + element.getName();
        Dependency dependency = directDependencies.get(coordinate);
        if (null == dependency) {
          log("ERROR: the following coordinate key does not appear in "
              + centralizedVersionsFile.getName() + ": " + coordinate, Project.MSG_ERR);
          success = false;
        } else {
          dependency.latestVersion = element.getRevision();
        }
      }
    } catch (IOException e) {
      log("Exception writing to " + findLatestIvyXmlFile.getPath() + ": " + e.toString(), Project.MSG_ERR);
      success = false;
    } catch (ParseException e) {
      log("Exception parsing filename " + findLatestIvyXmlFile.getPath() + ": " + e.toString(), Project.MSG_ERR);
      success = false;
    }
    return success;
  }

  /**
   * Collects indirect dependency version conflicts to ignore 
   * in ivy-ignore-conflicts.properties, and also checks for orphans
   * (coordinates not included in ivy-versions.properties).
   * 
   * Returns true if no orphans are found.
   */
  private boolean collectVersionConflictsToIgnore() {
    log("Checking for orphans in " + ignoreConflictsFile.getName(), verboseLevel);
    boolean orphansFound = false;
    InterpolatedProperties properties = new InterpolatedProperties();
    try (InputStream inputStream = new FileInputStream(ignoreConflictsFile);
         Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      properties.load(reader);
    } catch (IOException e) {
      throw new BuildException("Exception reading " + ignoreConflictsFile + ": " + e.toString(), e);
    }
    for (Object obj : properties.keySet()) {
      String coordinate = (String)obj;
      if (COORDINATE_KEY_PATTERN.matcher(coordinate).matches()) {
        if ( ! directDependencies.containsKey(coordinate)) {
          orphansFound = true;
          log("ORPHAN coordinate key '" + coordinate + "' in " + ignoreConflictsFile.getName()
                  + " is not found in " + centralizedVersionsFile.getName(),
              Project.MSG_ERR);
        } else {
          String versionsToIgnore = properties.getProperty(coordinate);
          List<String> ignore = Arrays.asList(versionsToIgnore.trim().split("\\s*,\\s*|\\s+"));
          ignoreConflictVersions.put(coordinate, new HashSet<>(ignore));
        }
      }
    }
    return ! orphansFound;
  }

  private void collectDirectDependencies() {
    InterpolatedProperties properties = new InterpolatedProperties();
    try (InputStream inputStream = new FileInputStream(centralizedVersionsFile);
         Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      properties.load(reader);
    } catch (IOException e) {
      throw new BuildException("Exception reading " + centralizedVersionsFile + ": " + e.toString(), e);
    }
    for (Object obj : properties.keySet()) {
      String coordinate = (String)obj;
      Matcher matcher = COORDINATE_KEY_PATTERN.matcher(coordinate);
      if (matcher.matches()) {
        String org = matcher.group(2);
        String name = matcher.group(3);
        String directVersion = properties.getProperty(coordinate);
        Dependency dependency = new Dependency(org, name, directVersion);
        directDependencies.put(coordinate, dependency);
      }
    }
  }

  /**
   * Transitively resolves all dependencies in the given ivy.xml file,
   * looking for indirect dependencies with versions that conflict
   * with those of direct dependencies.  Dependency conflict when a
   * direct dependency's version is older than that of an indirect
   * dependency with the same /org/name.
   * 
   * Returns true if no version conflicts are found and no resolution
   * errors occurred, false otherwise.
   */
  private boolean resolveTransitively(File ivyXmlFile) {
    boolean success = true;

    ResolveOptions options = new ResolveOptions();
    options.setDownload(false);           // Download only module descriptors, not artifacts
    options.setTransitive(true);          // Resolve transitively, if not already specified in the ivy.xml file
    options.setUseCacheOnly(false);       // Download the internet!
    options.setOutputReport(false);       // Don't print to the console
    options.setLog(LogOptions.LOG_QUIET); // Don't log to the console
    options.setConfs(new String[] {"*"}); // Resolve all configurations

    // Rewrite the ivy.xml, replacing all 'transitive="false"' with 'transitive="true"'
    // The Ivy API is file-based, so we have to write the result to the filesystem.
    String moduleName = "unknown";
    String ivyXmlContent = xmlToString(ivyXmlFile);
    Matcher matcher = MODULE_NAME_PATTERN.matcher(ivyXmlContent);
    if (matcher.find()) {
      moduleName = matcher.group(1);
    }
    ivyXmlContent = ivyXmlContent.replaceAll("\\btransitive\\s*=\\s*[\"']false[\"']", "transitive=\"true\"");
    File transitiveIvyXmlFile = null;
    try {
      File buildDir = new File(commonBuildDir, "ivy-transitive-resolve");
      if ( ! buildDir.exists() && ! buildDir.mkdirs()) {
        throw new BuildException("Could not create temp directory " + buildDir.getPath());
      }
      matcher = MODULE_DIRECTORY_PATTERN.matcher(ivyXmlFile.getCanonicalPath());
      if ( ! matcher.matches()) {
        throw new BuildException("Unknown ivy.xml module directory: " + ivyXmlFile.getCanonicalPath());
      }
      String moduleDirPrefix = matcher.group(1).replaceAll("[/\\\\]", ".");
      transitiveIvyXmlFile = new File(buildDir, "transitive." + moduleDirPrefix + ".ivy.xml");
      try (Writer writer = new OutputStreamWriter(new FileOutputStream(transitiveIvyXmlFile), StandardCharsets.UTF_8)) {
        writer.write(ivyXmlContent);
      }
      ResolveReport resolveReport = ivy.resolve(transitiveIvyXmlFile.toURI().toURL(), options);
      IvyNodeElement root = IvyNodeElementAdapter.adapt(resolveReport);
      for (IvyNodeElement directDependency : root.getDependencies()) {
        String coordinate = "/" + directDependency.getOrganization() + "/" + directDependency.getName();
        Dependency dependency = directDependencies.get(coordinate);
        if (null == dependency) {
          log("ERROR: the following coordinate key does not appear in " 
              + centralizedVersionsFile.getName() + ": " + coordinate);
          success = false;
        } else {
          dependency.directlyReferenced = true;
          if (collectConflicts(directDependency, directDependency, moduleName)) {
            success = false;
          }
        }
      }
    } catch (ParseException | IOException e) {
      if (null != transitiveIvyXmlFile) {
        log("Exception reading " + transitiveIvyXmlFile.getPath() + ": " + e.toString());
      }
      success = false;
    }
    return success;
  }

  /**
   * Recursively finds indirect dependencies that have a version conflict with a direct dependency.
   * Returns true if one or more conflicts are found, false otherwise
   */
  private boolean collectConflicts(IvyNodeElement root, IvyNodeElement parent, String moduleName) {
    boolean conflicts = false;
    for (IvyNodeElement child : parent.getDependencies()) {
      String coordinate = "/" + child.getOrganization() + "/" + child.getName();
      Dependency dependency = directDependencies.get(coordinate);
      if (null != dependency) { // Ignore this indirect dependency if it's not also a direct dependency
        String indirectVersion = child.getRevision();
        if (isConflict(coordinate, dependency.directVersion, indirectVersion)) {
          conflicts = true;
          Set<String> moduleNames = dependency.conflictLocations.get(root);
          if (null == moduleNames) {
            moduleNames = new HashSet<>();
            dependency.conflictLocations.put(root, moduleNames);
          }
          moduleNames.add(moduleName);
        }
        conflicts |= collectConflicts(root, child, moduleName);
      }
    }
    return conflicts;
  }

  /**
   * Copy-pasted from Ivy's 
   * org.apache.ivy.plugins.latest.LatestRevisionStrategy
   * with minor modifications
   */
  private static final Map<String,Integer> SPECIAL_MEANINGS;
  static {
    SPECIAL_MEANINGS = new HashMap<>();
    SPECIAL_MEANINGS.put("dev", -1);
    SPECIAL_MEANINGS.put("rc", 1);
    SPECIAL_MEANINGS.put("final", 2);
  }

  /**
   * Copy-pasted from Ivy's 
   * org.apache.ivy.plugins.latest.LatestRevisionStrategy.MridComparator
   * with minor modifications
   */
  private static class LatestVersionComparator implements Comparator<String> {
    @Override
    public int compare(String rev1, String rev2) {
      rev1 = rev1.replaceAll("([a-zA-Z])(\\d)", "$1.$2");
      rev1 = rev1.replaceAll("(\\d)([a-zA-Z])", "$1.$2");
      rev2 = rev2.replaceAll("([a-zA-Z])(\\d)", "$1.$2");
      rev2 = rev2.replaceAll("(\\d)([a-zA-Z])", "$1.$2");

      String[] parts1 = rev1.split("[-._+]");
      String[] parts2 = rev2.split("[-._+]");

      int i = 0;
      for (; i < parts1.length && i < parts2.length; i++) {
        if (parts1[i].equals(parts2[i])) {
          continue;
        }
        boolean is1Number = isNumber(parts1[i]);
        boolean is2Number = isNumber(parts2[i]);
        if (is1Number && !is2Number) {
          return 1;
        }
        if (is2Number && !is1Number) {
          return -1;
        }
        if (is1Number && is2Number) {
          return Long.valueOf(parts1[i]).compareTo(Long.valueOf(parts2[i]));
        }
        // both are strings, we compare them taking into account special meaning
        Integer sm1 = SPECIAL_MEANINGS.get(parts1[i].toLowerCase(Locale.ROOT));
        Integer sm2 = SPECIAL_MEANINGS.get(parts2[i].toLowerCase(Locale.ROOT));
        if (sm1 != null) {
          sm2 = sm2 == null ? 0 : sm2;
          return sm1.compareTo(sm2);
        }
        if (sm2 != null) {
          return Integer.valueOf(0).compareTo(sm2);
        }
        return parts1[i].compareTo(parts2[i]);
      }
      if (i < parts1.length) {
        return isNumber(parts1[i]) ? 1 : -1;
      }
      if (i < parts2.length) {
        return isNumber(parts2[i]) ? -1 : 1;
      }
      return 0;
    }

    private static final Pattern IS_NUMBER = Pattern.compile("\\d+");
    private static boolean isNumber(String str) {
      return IS_NUMBER.matcher(str).matches();
    }
  }
  private static LatestVersionComparator LATEST_VERSION_COMPARATOR = new LatestVersionComparator();

  /**
   * Returns true if directVersion is less than indirectVersion, and 
   * coordinate=indirectVersion is not present in ivy-ignore-conflicts.properties. 
   */
  private boolean isConflict(String coordinate, String directVersion, String indirectVersion) {
    boolean isConflict = LATEST_VERSION_COMPARATOR.compare(directVersion, indirectVersion) < 0;
    if (isConflict) {
      Set<String> ignoredVersions = ignoreConflictVersions.get(coordinate);
      if (null != ignoredVersions && ignoredVersions.contains(indirectVersion)) {
        isConflict = false;
      }
    }
    return isConflict;
  }

  /**
   * Returns the number of direct dependencies in conflict with indirect
   * dependencies.
   */
  private int emitConflicts() {
    int conflicts = 0;
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String,Dependency> directDependency : directDependencies.entrySet()) {
      String coordinate = directDependency.getKey();
      Set<Map.Entry<IvyNodeElement,Set<String>>> entrySet
          = directDependency.getValue().conflictLocations.entrySet();
      if (entrySet.isEmpty()) {
        continue;
      }
      ++conflicts;
      Map.Entry<IvyNodeElement,Set<String>> first = entrySet.iterator().next();
      int notPrinted = entrySet.size() - 1;
      builder.append("VERSION CONFLICT: transitive dependency in module(s) ");
      boolean isFirst = true;
      for (String moduleName : first.getValue()) {
        if (isFirst) {
          isFirst = false;
        } else {
          builder.append(", ");
        }
        builder.append(moduleName);
      }
      builder.append(":\n");
      IvyNodeElement element = first.getKey();
      builder.append('/').append(element.getOrganization()).append('/').append(element.getName())
             .append('=').append(element.getRevision()).append('\n');
      emitConflict(builder, coordinate, first.getKey(), 1);
        
      if (notPrinted > 0) {
        builder.append("... and ").append(notPrinted).append(" more\n");
      }
      builder.append("\n");
    }
    if (builder.length() > 0) {
      log(builder.toString());
    }
    return conflicts;
  }
  
  private boolean emitConflict(StringBuilder builder, String conflictCoordinate, IvyNodeElement parent, int depth) {
    for (IvyNodeElement child : parent.getDependencies()) {
      String indirectCoordinate = "/" + child.getOrganization() + "/" + child.getName();
      if (conflictCoordinate.equals(indirectCoordinate)) {
        Dependency dependency = directDependencies.get(conflictCoordinate);
        String directVersion = dependency.directVersion;
        if (isConflict(conflictCoordinate, directVersion, child.getRevision())) {
          for (int i = 0 ; i < depth - 1 ; ++i) {
            builder.append("    ");
          }
          builder.append("+-- ");
          builder.append(indirectCoordinate).append("=").append(child.getRevision());
          builder.append(" <<< Conflict (direct=").append(directVersion);
          builder.append(", latest=").append(dependency.latestVersion).append(")\n");
          return true;
        }
      } else if (hasConflicts(conflictCoordinate, child)) {
        for (int i = 0 ; i < depth -1 ; ++i) {
          builder.append("    ");
        }
        builder.append("+-- ");
        builder.append(indirectCoordinate).append("=").append(child.getRevision()).append("\n");
        if (emitConflict(builder, conflictCoordinate, child, depth + 1)) {
          return true;
        }
      }
    }
    return false;
  }
  
  private boolean hasConflicts(String conflictCoordinate, IvyNodeElement parent) {
    // the element itself will never be in conflict, since its coordinate is different
    for (IvyNodeElement child : parent.getDependencies()) {
      String indirectCoordinate = "/" + child.getOrganization() + "/" + child.getName();
      if (conflictCoordinate.equals(indirectCoordinate)) {
        Dependency dependency = directDependencies.get(conflictCoordinate);
        if (isConflict(conflictCoordinate, dependency.directVersion, child.getRevision())) {
          return true;
        }
      } else if (hasConflicts(conflictCoordinate, child)) {
        return true;
      }
    }
    return false;
  }

  private String xmlToString(File ivyXmlFile) {
    StringWriter writer = new StringWriter();
    try {
      StreamSource inputSource = new StreamSource(new FileInputStream(ivyXmlFile.getPath()));
      Transformer serializer = TransformerFactory.newInstance().newTransformer();
      serializer.transform(inputSource, new StreamResult(writer));
    } catch (TransformerException | IOException e) {
      throw new BuildException("Exception reading " + ivyXmlFile.getPath() + ": " + e.toString(), e);
    }
    return writer.toString();
  }

  private void setupIvy() {
    IvySettings ivySettings = new IvySettings();
    try {
      ivySettings.setVariable("common.build.dir", commonBuildDir.getAbsolutePath());
      ivySettings.setVariable("ivy.exclude.types", "source|javadoc");
      ivySettings.setVariable("ivy.resolution-cache.dir", ivyResolutionCacheDir.getAbsolutePath());
      ivySettings.setVariable("ivy.lock-strategy", ivyLockStrategy);
      ivySettings.setVariable("ivysettings.xml", getProject().getProperty("ivysettings.xml")); // nested settings file
      ivySettings.setBaseDir(commonBuildDir);
      ivySettings.setDefaultConflictManager(new NoConflictManager());
      ivy = Ivy.newInstance(ivySettings);
      ivy.configure(topLevelIvySettingsFile);
    } catch (Exception e) {
      throw new BuildException("Exception reading " + topLevelIvySettingsFile.getPath() + ": " + e.toString(), e);
    }
  }

  /**
   * Returns true if the "/org/name" coordinate keys in the given
   * properties file are lexically sorted and are not duplicates.
   */
  private boolean verifySortedCoordinatesPropertiesFile(File coordinatePropertiesFile) {
    log("Checking for lexically sorted non-duplicated '/org/name' keys in: " + coordinatePropertiesFile, verboseLevel);
    boolean success = true;
    String line = null;
    String currentKey = null;
    String previousKey = null;
    try (InputStream stream = new FileInputStream(coordinatePropertiesFile);
         Reader reader = new InputStreamReader(stream, StandardCharsets.ISO_8859_1);
         BufferedReader bufferedReader = new BufferedReader(reader)) {
      while (null != (line = readLogicalPropertiesLine(bufferedReader))) {
        final Matcher keyMatcher = COORDINATE_KEY_PATTERN.matcher(line);
        if ( ! keyMatcher.lookingAt()) {
          continue; // Ignore keys that don't look like "/org/name"
        }
        currentKey = keyMatcher.group(1);
        if (null != previousKey) {
          int comparison = currentKey.compareTo(previousKey);
          if (0 == comparison) {
            log("DUPLICATE coordinate key '" + currentKey + "' in " + coordinatePropertiesFile.getName(),
                Project.MSG_ERR);
            success = false;
          } else if (comparison < 0) {
            log("OUT-OF-ORDER coordinate key '" + currentKey + "' in " + coordinatePropertiesFile.getName(),
                Project.MSG_ERR);
            success = false;
          }
        }
        previousKey = currentKey;
      }
    } catch (IOException e) {
      throw new BuildException("Exception reading " + coordinatePropertiesFile.getPath() + ": " + e.toString(), e);
    }
    return success;
  }

  /**
   * Builds up logical {@link java.util.Properties} lines, composed of one non-blank,
   * non-comment initial line, either:
   * 
   * 1. without a non-escaped trailing slash; or
   * 2. with a non-escaped trailing slash, followed by
   *    zero or more lines with a non-escaped trailing slash, followed by
   *    one or more lines without a non-escaped trailing slash
   *
   * All leading non-escaped whitespace and trailing non-escaped whitespace +
   * non-escaped slash are trimmed from each line before concatenating.
   * 
   * After composing the logical line, escaped characters are un-escaped.
   * 
   * null is returned if there are no lines left to read. 
   */
  private String readLogicalPropertiesLine(BufferedReader reader) throws IOException {
    final StringBuilder logicalLine = new StringBuilder();
    String line;
    do {
      line = reader.readLine();
      if (null == line) { 
        return null;
      }
    } while (BLANK_OR_COMMENT_LINE_PATTERN.matcher(line).matches());

    Matcher backslashMatcher = TRAILING_BACKSLASH_PATTERN.matcher(line); 
    // Check for a non-escaped backslash
    if (backslashMatcher.find() && 1 == (backslashMatcher.group(1).length() % 2)) {
      final Matcher firstLineMatcher = TRAILING_WHITESPACE_BACKSLASH_PATTERN.matcher(line);
      if (firstLineMatcher.matches()) {
        logicalLine.append(firstLineMatcher.group(1)); // trim trailing backslash and any preceding whitespace
      }
      line = reader.readLine();
      while (null != line
             && (backslashMatcher = TRAILING_BACKSLASH_PATTERN.matcher(line)).find()
             && 1 == (backslashMatcher.group(1).length() % 2)) {
        // Trim leading whitespace, the trailing backslash and any preceding whitespace
        final Matcher goodStuffMatcher = WHITESPACE_GOODSTUFF_WHITESPACE_BACKSLASH_PATTERN.matcher(line);
        if (goodStuffMatcher.matches()) {
          logicalLine.append(goodStuffMatcher.group(1));
        }
        line = reader.readLine();
      }
      if (null != line) {
        // line can't have a non-escaped trailing backslash
        final Matcher leadingWhitespaceMatcher = LEADING_WHITESPACE_PATTERN.matcher(line);
        if (leadingWhitespaceMatcher.matches()) {
          line = leadingWhitespaceMatcher.group(1); // trim leading whitespace
        }
        logicalLine.append(line);
      }
    } else {
      logicalLine.append(line);
    }
    // trim non-escaped leading whitespace
    final Matcher leadingWhitespaceMatcher = LEADING_WHITESPACE_PATTERN.matcher(logicalLine);
    final CharSequence leadingWhitespaceStripped = leadingWhitespaceMatcher.matches()
                                                 ? leadingWhitespaceMatcher.group(1)
                                                 : logicalLine;

    // unescape all chars in the logical line
    StringBuilder output = new StringBuilder();
    final int numChars = leadingWhitespaceStripped.length();
    for (int pos = 0 ; pos < numChars - 1 ; ++pos) {
      char ch = leadingWhitespaceStripped.charAt(pos);
      if (ch == '\\') {
        ch = leadingWhitespaceStripped.charAt(++pos);
      }
      output.append(ch);
    }
    if (numChars > 0) {
      output.append(leadingWhitespaceStripped.charAt(numChars - 1));
    }

    return output.toString();
  }

  /**
   * Check a single ivy.xml file for dependencies' versions in rev="${/org/name}"
   * format.  Returns false if problems are found, true otherwise.
   */
  private boolean checkIvyXmlFile(File ivyXmlFile)
      throws ParserConfigurationException, SAXException, IOException {
    log("Scanning: " + ivyXmlFile.getPath(), verboseLevel);
    XMLReader xmlReader = XMLReaderFactory.createXMLReader();
    DependencyRevChecker revChecker = new DependencyRevChecker(ivyXmlFile); 
    xmlReader.setContentHandler(revChecker);
    xmlReader.setErrorHandler(revChecker);
    xmlReader.parse(new InputSource(ivyXmlFile.getAbsolutePath()));
    return ! revChecker.fail;
  }

  private class DependencyRevChecker extends DefaultHandler {
    private final File ivyXmlFile;
    private final Stack<String> tags = new Stack<>();
    
    public boolean fail = false;

    public DependencyRevChecker(File ivyXmlFile) {
      this.ivyXmlFile = ivyXmlFile;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
      if (localName.equals("dependency") && insideDependenciesTag()) {
        String org = attributes.getValue("org");
        boolean foundAllAttributes = true;
        if (null == org) {
          log("MISSING 'org' attribute on <dependency> in " + ivyXmlFile.getPath(), Project.MSG_ERR);
          fail = true;
          foundAllAttributes = false;
        }
        String name = attributes.getValue("name");
        if (null == name) {
          log("MISSING 'name' attribute on <dependency> in " + ivyXmlFile.getPath(), Project.MSG_ERR);
          fail = true;
          foundAllAttributes = false;
        }
        String rev = attributes.getValue("rev");
        if (null == rev) {
          log("MISSING 'rev' attribute on <dependency> in " + ivyXmlFile.getPath(), Project.MSG_ERR);
          fail = true;
          foundAllAttributes = false;
        }
        if (foundAllAttributes) {
          String coordinateKey = "/" + org + '/' + name;
          String expectedRev = "${" + coordinateKey + '}';
          if ( ! rev.equals(expectedRev)) {
            log("BAD <dependency> 'rev' attribute value '" + rev + "' - expected '" + expectedRev + "'"
                + " in " + ivyXmlFile.getPath(), Project.MSG_ERR);
            fail = true;
          }
          if ( ! directDependencies.containsKey(coordinateKey)) {
            log("MISSING key '" + coordinateKey + "' in " + centralizedVersionsFile.getPath(), Project.MSG_ERR);
            fail = true;
          }
        }
      }
      tags.push(localName);
    }

    @Override
    public void endElement (String uri, String localName, String qName) throws SAXException {
      tags.pop();
    }

    private boolean insideDependenciesTag() {
      return tags.size() == 2 && tags.get(0).equals("ivy-module") && tags.get(1).equals("dependencies");
    }
  }
}

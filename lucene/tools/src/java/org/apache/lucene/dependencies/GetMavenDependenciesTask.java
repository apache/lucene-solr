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
package org.apache.lucene.dependencies;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.Resource;
import org.apache.tools.ant.types.ResourceCollection;
import org.apache.tools.ant.types.resources.FileResource;
import org.apache.tools.ant.types.resources.Resources;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

/**
 * An Ant task to generate a properties file containing maven dependency
 * declarations, used to filter the maven POMs when copying them to
 * maven-build/ via 'ant get-maven-poms', or to lucene/build/poms/
 * via the '-filter-maven-poms' target, which is called from the
 * 'generate-maven-artifacts' target.
 */
public class GetMavenDependenciesTask extends Task {
  private static final Pattern PROPERTY_PREFIX_FROM_IVY_XML_FILE_PATTERN = Pattern.compile
      ("[/\\\\](lucene|solr)[/\\\\](?:(?:contrib|(analysis)|(example)|(server))[/\\\\])?([^/\\\\]+)[/\\\\]ivy\\.xml");
  private static final Pattern COORDINATE_KEY_PATTERN = Pattern.compile("/([^/]+)/([^/]+)");
  private static final Pattern MODULE_DEPENDENCIES_COORDINATE_KEY_PATTERN
      = Pattern.compile("(.*?)(\\.test)?\\.dependencies");
  // lucene/build/core/classes/java
  private static final Pattern COMPILATION_OUTPUT_DIRECTORY_PATTERN 
      = Pattern.compile("(lucene|solr)/build/(?:contrib/)?(.*)/classes/(?:java|test)");
  private static final String UNWANTED_INTERNAL_DEPENDENCIES
      = "/(?:test-)?lib/|test-framework/classes/java|/test-files|/resources";
  private static final Pattern SHARED_EXTERNAL_DEPENDENCIES_PATTERN
      = Pattern.compile("((?:solr|lucene)/(?!test-framework).*)/((?:test-)?)lib/");

  private static final String DEPENDENCY_MANAGEMENT_PROPERTY = "lucene.solr.dependency.management";
  private static final String IVY_USER_DIR_PROPERTY = "ivy.default.ivy.user.dir";
  private static final Properties allProperties = new Properties();
  private static final Set<String> modulesWithSeparateCompileAndTestPOMs = new HashSet<>();

  private static final Set<String> globalOptionalExternalDependencies = new HashSet<>();
  private static final Map<String,Set<String>> perModuleOptionalExternalDependencies = new HashMap<>();
  private static final Set<String> modulesWithTransitiveDependencies = new HashSet<>();
  static {
    // Add modules here that have split compile and test POMs
    // - they need compile-scope deps to also be test-scope deps.
    modulesWithSeparateCompileAndTestPOMs.addAll
        (Arrays.asList("lucene-core", "lucene-codecs", "solr-core", "solr-solrj"));

    // Add external dependencies here that should be optional for all modules
    // (i.e., not invoke Maven's transitive dependency mechanism).
    // Format is "groupId:artifactId"
    globalOptionalExternalDependencies.addAll(Arrays.asList
        ("org.slf4j:jul-to-slf4j", "org.slf4j:slf4j-log4j12"));

    // Add modules here that should NOT have their dependencies
    // excluded in the grandparent POM's dependencyManagement section,
    // thus enabling their dependencies to be transitive.
    modulesWithTransitiveDependencies.addAll(Arrays.asList("lucene-test-framework"));
  }

  private final XPath xpath = XPathFactory.newInstance().newXPath();
  private final SortedMap<String,SortedSet<String>> internalCompileScopeDependencies
      = new TreeMap<>();
  private final Set<String> nonJarDependencies = new HashSet<>();
  private final Map<String,Set<String>> dependencyClassifiers = new HashMap<>();
  private final Map<String,Set<String>> interModuleExternalCompileScopeDependencies = new HashMap<>();
  private final Map<String,Set<String>> interModuleExternalTestScopeDependencies = new HashMap<>();
  private final Map<String,SortedSet<ExternalDependency>> allExternalDependencies
     = new HashMap<>();
  private final DocumentBuilder documentBuilder;
  private File ivyCacheDir;
  private Pattern internalJarPattern;
  private Map<String,String> ivyModuleInfo;


  /**
   * All ivy.xml files to get external dependencies from.
   */
  private Resources ivyXmlResources = new Resources();

  /**
   * Centralized Ivy versions properties file
   */
  private File centralizedVersionsFile;

  /**
   * Module dependencies properties file, generated by task -append-module-dependencies-properties.
   */
  private File moduleDependenciesPropertiesFile;

  /**
   * Where all properties are written, to be used to filter POM templates when copying them.
   */
  private File mavenDependenciesFiltersFile;

  /**
   * A logging level associated with verbose logging.
   */
  private int verboseLevel = Project.MSG_VERBOSE;

  /**
   * Adds a set of ivy.xml resources to check.
   */
  public void add(ResourceCollection rc) {
    ivyXmlResources.add(rc);
  }

  public void setVerbose(boolean verbose) {
    verboseLevel = (verbose ? Project.MSG_VERBOSE : Project.MSG_INFO);
  }

  public void setCentralizedVersionsFile(File file) {
    centralizedVersionsFile = file;
  }

  public void setModuleDependenciesPropertiesFile(File file) {
    moduleDependenciesPropertiesFile = file;
  }
  
  public void setMavenDependenciesFiltersFile(File file) {
    mavenDependenciesFiltersFile = file;
  }

  public GetMavenDependenciesTask() {
    try {
      documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new BuildException(e);
    }
  }
  
  /**
   * Collect dependency information from Ant build.xml and ivy.xml files
   * and from ivy-versions.properties, then write out an Ant filters file
   * to be used when copying POMs.
   */
  @Override
  public void execute() throws BuildException {
    // Local:   lucene/build/analysis/common/lucene-analyzers-common-5.0-SNAPSHOT.jar
    // Jenkins: lucene/build/analysis/common/lucene-analyzers-common-5.0-2013-10-31_18-52-24.jar
    // Also support any custom version, which won't necessarily conform to any predefined pattern.
    internalJarPattern = Pattern.compile(".*(lucene|solr)([^/]*?)-"
        + Pattern.quote(getProject().getProperty("version")) + "\\.jar");

    ivyModuleInfo = getIvyModuleInfo(ivyXmlResources, documentBuilder, xpath);

    setInternalDependencyProperties();            // side-effect: all modules' internal deps are recorded
    setExternalDependencyProperties();            // side-effect: all modules' external deps are recorded
    setGrandparentDependencyManagementProperty(); // uses deps recorded in above two methods
    writeFiltersFile();
  }

  /**
   * Write out an Ant filters file to be used when copying POMs.
   */
  private void writeFiltersFile() {
    Writer writer = null;
    try {
      FileOutputStream outputStream = new FileOutputStream(mavenDependenciesFiltersFile);
      writer = new OutputStreamWriter(outputStream, StandardCharsets.ISO_8859_1);
      allProperties.store(writer, null);
    } catch (FileNotFoundException e) {
      throw new BuildException("Can't find file: '" + mavenDependenciesFiltersFile.getPath() + "'", e);
    } catch (IOException e) {
      throw new BuildException("Exception writing out '" + mavenDependenciesFiltersFile.getPath() + "'", e);
    } finally {
      if (null != writer) {
        try {
          writer.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
  }

  /**
   * Visits all ivy.xml files and collects module and organisation attributes into a map.
   */
  private static Map<String,String> getIvyModuleInfo(Resources ivyXmlResources,
      DocumentBuilder documentBuilder, XPath xpath) {
    Map<String,String> ivyInfoModuleToOrganisation = new HashMap<String,String>();
    traverseIvyXmlResources(ivyXmlResources, new Consumer<File>() {
      @Override
      public void accept(File f) {
        try {
          Document document = documentBuilder.parse(f);
          {
            String infoPath = "/ivy-module/info";
            NodeList infos = (NodeList)xpath.evaluate(infoPath, document, XPathConstants.NODESET);
            for (int infoNum = 0 ; infoNum < infos.getLength() ; ++infoNum) {
              Element infoElement = (Element)infos.item(infoNum);
              String infoOrg = infoElement.getAttribute("organisation");
              String infoOrgSuffix = infoOrg.substring(infoOrg.lastIndexOf('.')+1);
              String infoModule = infoElement.getAttribute("module");
              String module = infoOrgSuffix+"-"+infoModule;
              ivyInfoModuleToOrganisation.put(module, infoOrg);
            }
          }
        } catch (XPathExpressionException | IOException | SAXException e) {
          throw new RuntimeException(e);
        }
      }
    });
    return ivyInfoModuleToOrganisation;
  }

  /**
   * Collects external dependencies from each ivy.xml file and sets
   * external dependency properties to be inserted into modules' POMs. 
   */
  private void setExternalDependencyProperties() {
    traverseIvyXmlResources(ivyXmlResources, new Consumer<File>() {
      @Override
      public void accept(File f) {
        try {
        collectExternalDependenciesFromIvyXmlFile(f);
        } catch (XPathExpressionException | IOException | SAXException e) {
          throw new RuntimeException(e);
        }
      }
    });
    addSharedExternalDependencies();
    setExternalDependencyXmlProperties();
  }

  private static void traverseIvyXmlResources(Resources ivyXmlResources, Consumer<File> ivyXmlFileConsumer) {
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
        ivyXmlFileConsumer.accept(ivyXmlFile);
      } catch (BuildException e) {
        throw e;
      } catch (Exception e) {
        throw new BuildException("Exception reading file " + ivyXmlFile.getPath() + ": " + e, e);
      }
    }
  }

  /**
   * For each module that includes other modules' external dependencies via
   * including all files under their ".../lib/" dirs in their (test.)classpath,
   * add the other modules' dependencies to its set of external dependencies. 
   */
  private void addSharedExternalDependencies() {
    // Delay adding shared compile-scope dependencies until after all have been processed,
    // so dependency sharing is limited to a depth of one.
    Map<String,SortedSet<ExternalDependency>> sharedDependencies = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : interModuleExternalCompileScopeDependencies.entrySet()) {
      TreeSet<ExternalDependency> deps = new TreeSet<>();
      sharedDependencies.put(entry.getKey(), deps);
      Set<String> moduleDependencies = entry.getValue();
      if (null != moduleDependencies) {
        for (String otherArtifactId : moduleDependencies) {
          SortedSet<ExternalDependency> otherExtDeps = allExternalDependencies.get(otherArtifactId); 
          if (null != otherExtDeps) {
            for (ExternalDependency otherDep : otherExtDeps) {
              if ( ! otherDep.isTestDependency) {
                deps.add(otherDep);
              }
            }
          }
        }
      }
    }
    for (Map.Entry<String, Set<String>> entry : interModuleExternalTestScopeDependencies.entrySet()) {
      String module = entry.getKey();
      SortedSet<ExternalDependency> deps = sharedDependencies.get(module);
      if (null == deps) {
        deps = new TreeSet<>();
        sharedDependencies.put(module, deps);
      }
      Set<String> moduleDependencies = entry.getValue();
      if (null != moduleDependencies) {
        for (String otherArtifactId : moduleDependencies) {
          int testScopePos = otherArtifactId.indexOf(":test");
          boolean isTestScope = false;
          if (-1 != testScopePos) {
            otherArtifactId = otherArtifactId.substring(0, testScopePos);
            isTestScope = true;
          }
          SortedSet<ExternalDependency> otherExtDeps = allExternalDependencies.get(otherArtifactId);
          if (null != otherExtDeps) {
            for (ExternalDependency otherDep : otherExtDeps) {
              if (otherDep.isTestDependency == isTestScope) {
                if (  ! deps.contains(otherDep)
                   && (  null == allExternalDependencies.get(module)
                      || ! allExternalDependencies.get(module).contains(otherDep))) {
                  // Add test-scope clone only if it's not already a compile-scope dependency. 
                  ExternalDependency otherDepTestScope = new ExternalDependency
                      (otherDep.groupId, otherDep.artifactId, otherDep.classifier, true, otherDep.isOptional);
                  deps.add(otherDepTestScope);
                }
              }
            }
          }
        }
      }
    }
    for (Map.Entry<String, SortedSet<ExternalDependency>> entry : sharedDependencies.entrySet()) {
      String module = entry.getKey();
      SortedSet<ExternalDependency> deps = allExternalDependencies.get(module);
      if (null == deps) {
        deps = new TreeSet<>();
        allExternalDependencies.put(module, deps);
      }
      for (ExternalDependency dep : entry.getValue()) {
        String dependencyCoordinate = dep.groupId + ":" + dep.artifactId;
        if (globalOptionalExternalDependencies.contains(dependencyCoordinate)
            || (perModuleOptionalExternalDependencies.containsKey(module)
                && perModuleOptionalExternalDependencies.get(module).contains(dependencyCoordinate))) {
          // make a copy of the dep and set optional=true
          dep = new ExternalDependency(dep.groupId, dep.artifactId, dep.classifier, dep.isTestDependency, true);
        }
        deps.add(dep);
      }
    }
  }

  /**
   * For each module, sets a compile-scope and a test-scope property
   * with values that contain the appropriate &lt;dependency&gt;
   * snippets.
   */
  private void setExternalDependencyXmlProperties() {
    for (String module : internalCompileScopeDependencies.keySet()) { // get full module list
      StringBuilder compileScopeBuilder = new StringBuilder();
      StringBuilder testScopeBuilder = new StringBuilder();
      SortedSet<ExternalDependency> extDeps = allExternalDependencies.get(module);
      if (null != extDeps) {
        for (ExternalDependency dep : extDeps) {
          StringBuilder builder = dep.isTestDependency ? testScopeBuilder : compileScopeBuilder;
          appendDependencyXml(builder, dep.groupId, dep.artifactId, "    ", null, 
                              dep.isTestDependency, dep.isOptional, dep.classifier, null);
          // Test POMs for solrj, solr-core, lucene-codecs and lucene-core modules
          // need to include all compile-scope dependencies as test-scope dependencies
          // since we've turned off transitive dependency resolution.
          if ( ! dep.isTestDependency && modulesWithSeparateCompileAndTestPOMs.contains(module)) {
            appendDependencyXml(testScopeBuilder, dep.groupId, dep.artifactId, "    ", null,
                                true, dep.isOptional, dep.classifier, null);
          }
        }
      }
      if (compileScopeBuilder.length() > 0) {
        compileScopeBuilder.setLength(compileScopeBuilder.length() - 1); // drop trailing newline
      }
      if (testScopeBuilder.length() > 0) {
        testScopeBuilder.setLength(testScopeBuilder.length() - 1); // drop trailing newline
      }
      allProperties.setProperty(module + ".external.dependencies", compileScopeBuilder.toString());
      allProperties.setProperty(module + ".external.test.dependencies", testScopeBuilder.toString());
    }
  }

  /**
   * Sets the property to be inserted into the grandparent POM's 
   * &lt;dependencyManagement&gt; section.
   */
  private void setGrandparentDependencyManagementProperty() {
    StringBuilder builder = new StringBuilder();
    appendAllInternalDependencies(builder);
    Map<String,String> versionsMap = new HashMap<>();
    appendAllExternalDependencies(builder, versionsMap);
    builder.setLength(builder.length() - 1); // drop trailing newline
    allProperties.setProperty(DEPENDENCY_MANAGEMENT_PROPERTY, builder.toString());
    for (Map.Entry<String,String> entry : versionsMap.entrySet()) {
      allProperties.setProperty(entry.getKey(), entry.getValue());
    }
  }

  /**
   * For each artifact in the project, append a dependency with version
   * ${project.version} to the grandparent POM's &lt;dependencyManagement&gt;
   * section.  An &lt;exclusion&gt; is added for each of the artifact's
   * dependencies.
   */
  private void appendAllInternalDependencies(StringBuilder builder) {
    for (Map.Entry<String, SortedSet<String>> entry : internalCompileScopeDependencies.entrySet()) {
      String artifactId = entry.getKey();
      List<String> exclusions = new ArrayList<>(entry.getValue());
      SortedSet<ExternalDependency> extDeps = allExternalDependencies.get(artifactId);
      if (null != extDeps) {
        for (ExternalDependency externalDependency : extDeps) {
          if ( ! externalDependency.isTestDependency && ! externalDependency.isOptional) {
            exclusions.add(externalDependency.groupId + ':' + externalDependency.artifactId);
          }
        }
      }
      String groupId = ivyModuleInfo.get(artifactId);
      appendDependencyXml(builder, groupId, artifactId, "      ", "${project.version}", false, false, null, exclusions);
    }
  }

  /**
   * Sets the ivyCacheDir field, to either the ${ivy.default.ivy.user.dir} 
   * property, or if that's not set, to the default ~/.ivy2/.
   */
  private File getIvyCacheDir() {
    String ivyUserDirName = getProject().getUserProperty(IVY_USER_DIR_PROPERTY);
    if (null == ivyUserDirName) {
      ivyUserDirName = getProject().getProperty(IVY_USER_DIR_PROPERTY);
      if (null == ivyUserDirName) {
        ivyUserDirName = System.getProperty("user.home") + System.getProperty("file.separator") + ".ivy2";
      }
    }
    File ivyUserDir = new File(ivyUserDirName);
    if ( ! ivyUserDir.exists()) {
      throw new BuildException("Ivy user dir does not exist: '" + ivyUserDir.getPath() + "'");
    }
    File dir = new File(ivyUserDir, "cache");
    if ( ! dir.exists()) {
      throw new BuildException("Ivy cache dir does not exist: '" + ivyCacheDir.getPath() + "'");
    }
    return dir;
  }

  /**
   * Append each dependency listed in the centralized Ivy versions file
   * to the grandparent POM's &lt;dependencyManagement&gt; section.  
   * An &lt;exclusion&gt; is added for each of the artifact's dependencies,
   * which are collected from the artifact's ivy.xml from the Ivy cache.
   * 
   * Also add a version property for each dependency.
   */
  private void appendAllExternalDependencies(StringBuilder dependenciesBuilder, Map<String,String> versionsMap) {
    log("Loading centralized ivy versions from: " + centralizedVersionsFile, verboseLevel);
    ivyCacheDir = getIvyCacheDir();
    Properties versions = new InterpolatedProperties();
    try (InputStream inputStream = new FileInputStream(centralizedVersionsFile);
         Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      versions.load(reader);
    } catch (IOException e) {
      throw new BuildException("Exception reading centralized versions file " + centralizedVersionsFile.getPath(), e);
    } 
    SortedSet<Map.Entry<?,?>> sortedEntries = new TreeSet<>(new Comparator<Map.Entry<?,?>>() {
      @Override public int compare(Map.Entry<?,?> o1, Map.Entry<?,?> o2) {
        return ((String)o1.getKey()).compareTo((String)o2.getKey());
      }
    });
    sortedEntries.addAll(versions.entrySet());
    for (Map.Entry<?,?> entry : sortedEntries) {
      String key = (String)entry.getKey();
      Matcher matcher = COORDINATE_KEY_PATTERN.matcher(key);
      if (matcher.lookingAt()) {
        String groupId = matcher.group(1);
        String artifactId = matcher.group(2);
        String coordinate = groupId + ':' + artifactId;
        String version = (String)entry.getValue();
        versionsMap.put(coordinate + ".version", version);
        if ( ! nonJarDependencies.contains(coordinate)) {
          Set<String> classifiers = dependencyClassifiers.get(coordinate);
          if (null != classifiers) {
            for (String classifier : classifiers) {
              Collection<String> exclusions = getTransitiveDependenciesFromIvyCache(groupId, artifactId, version);
              appendDependencyXml
                  (dependenciesBuilder, groupId, artifactId, "      ", version, false, false, classifier, exclusions);
            }
          }
        }
      }
    }
  }

  /**
   * Collect transitive compile-scope dependencies for the given artifact's
   * ivy.xml from the Ivy cache, using the default ivy pattern 
   * "[organisation]/[module]/ivy-[revision].xml".  See 
   * <a href="http://ant.apache.org/ivy/history/latest-milestone/settings/caches.html"
   * >the Ivy cache documentation</a>.
   */
  private Collection<String> getTransitiveDependenciesFromIvyCache
  (String groupId, String artifactId, String version) {
    SortedSet<String> transitiveDependencies = new TreeSet<>();
    //                                      E.g. ~/.ivy2/cache/xerces/xercesImpl/ivy-2.9.1.xml
    File ivyXmlFile = new File(new File(new File(ivyCacheDir, groupId), artifactId), "ivy-" + version + ".xml");
    if ( ! ivyXmlFile.exists()) {
      throw new BuildException("File not found: " + ivyXmlFile.getPath());
    }
    try {
      Document document = documentBuilder.parse(ivyXmlFile);
      String dependencyPath = "/ivy-module/dependencies/dependency"
                            + "[   not(starts-with(@conf,'test->'))"
                            + "and not(starts-with(@conf,'provided->'))"
                            + "and not(starts-with(@conf,'optional->'))]";
      NodeList dependencies = (NodeList)xpath.evaluate(dependencyPath, document, XPathConstants.NODESET);
      for (int i = 0 ; i < dependencies.getLength() ; ++i) {
        Element dependency = (Element)dependencies.item(i);
        transitiveDependencies.add(dependency.getAttribute("org") + ':' + dependency.getAttribute("name"));
      }
    } catch (Exception e) {
      throw new BuildException( "Exception collecting transitive dependencies for " 
                              + groupId + ':' + artifactId + ':' + version + " from "
                              + ivyXmlFile.getAbsolutePath(), e);
    }
    return transitiveDependencies;
  }

  /**
   * Sets the internal dependencies compile and test properties to be inserted 
   * into modules' POMs.
   * 
   * Also collects shared external dependencies, 
   * e.g. solr-core wants all of solrj's external dependencies 
   */
  private void  setInternalDependencyProperties() {
    log("Loading module dependencies from: " + moduleDependenciesPropertiesFile, verboseLevel);
    Properties moduleDependencies = new Properties();
    try (InputStream inputStream = new FileInputStream(moduleDependenciesPropertiesFile);
         Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      moduleDependencies.load(reader);
    } catch (FileNotFoundException e) {
      throw new BuildException("Properties file does not exist: " + moduleDependenciesPropertiesFile.getPath());
    } catch (IOException e) {
      throw new BuildException("Exception reading properties file " + moduleDependenciesPropertiesFile.getPath(), e);
    }
    Map<String,SortedSet<String>> testScopeDependencies = new HashMap<>();
    Map<String, String> testScopePropertyKeys = new HashMap<>();
    for (Map.Entry<?,?> entry : moduleDependencies.entrySet()) {
      String newPropertyKey = (String)entry.getKey();
      StringBuilder newPropertyValue = new StringBuilder();
      String value = (String)entry.getValue();
      Matcher matcher = MODULE_DEPENDENCIES_COORDINATE_KEY_PATTERN.matcher(newPropertyKey);
      if ( ! matcher.matches()) {
        throw new BuildException("Malformed module dependencies property key: '" + newPropertyKey + "'");
      }
      String antProjectName = matcher.group(1);
      boolean isTest = null != matcher.group(2);
      String artifactName = antProjectToArtifactName(antProjectName);
      newPropertyKey = artifactName + (isTest ? ".internal.test" : ".internal") + ".dependencies"; // Add ".internal"
      if (isTest) {
        testScopePropertyKeys.put(artifactName, newPropertyKey);
      }
      if (null == value || value.isEmpty()) {
        allProperties.setProperty(newPropertyKey, "");
        Map<String,SortedSet<String>> scopedDependencies
            = isTest ? testScopeDependencies : internalCompileScopeDependencies;
        scopedDependencies.put(artifactName, new TreeSet<String>());
      } else {
        // Lucene analysis modules' build dirs do not include hyphens, but Solr contribs' build dirs do
        String origModuleDir = antProjectName.replace("analyzers-", "analysis/");
        // Exclude the module's own build output, in addition to UNWANTED_INTERNAL_DEPENDENCIES
        Pattern unwantedInternalDependencies = Pattern.compile
            ("(?:lucene/build/|solr/build/(?:contrib/)?)" + origModuleDir + "/" // require dir separator 
             + "|" + UNWANTED_INTERNAL_DEPENDENCIES);
        SortedSet<String> sortedDeps = new TreeSet<>();
        for (String dependency : value.split(",")) {
          matcher = SHARED_EXTERNAL_DEPENDENCIES_PATTERN.matcher(dependency);
          if (matcher.find()) {
            String otherArtifactName = matcher.group(1);
            boolean isTestScope = null != matcher.group(2) && matcher.group(2).length() > 0;
            otherArtifactName = otherArtifactName.replace('/', '-');
            otherArtifactName = otherArtifactName.replace("lucene-analysis", "lucene-analyzers");
            otherArtifactName = otherArtifactName.replace("solr-contrib-solr-", "solr-");
            otherArtifactName = otherArtifactName.replace("solr-contrib-", "solr-");
            if ( ! otherArtifactName.equals(artifactName)) {
              Map<String,Set<String>> sharedDeps
                  = isTest ? interModuleExternalTestScopeDependencies : interModuleExternalCompileScopeDependencies;
              Set<String> sharedSet = sharedDeps.get(artifactName);
              if (null == sharedSet) {
                sharedSet = new HashSet<>();
                sharedDeps.put(artifactName, sharedSet);
              }
              if (isTestScope) {
                otherArtifactName += ":test";
              }
              sharedSet.add(otherArtifactName);
            }
          }
          matcher = unwantedInternalDependencies.matcher(dependency);
          if (matcher.find()) {
            continue;  // skip external (/(test-)lib/), and non-jar and unwanted (self) internal deps
          }
          String artifactId = dependencyToArtifactId(newPropertyKey, dependency);
          String groupId = ivyModuleInfo.get(artifactId);
          String coordinate = groupId + ':' + artifactId;
          sortedDeps.add(coordinate);
        }
        if (isTest) {  // Don't set test-scope properties until all compile-scope deps have been seen
          testScopeDependencies.put(artifactName, sortedDeps);
        } else {
          internalCompileScopeDependencies.put(artifactName, sortedDeps);
          for (String dependency : sortedDeps) {
            int splitPos = dependency.indexOf(':');
            String groupId = dependency.substring(0, splitPos);
            String artifactId = dependency.substring(splitPos + 1);
            appendDependencyXml(newPropertyValue, groupId, artifactId, "    ", null, false, false, null, null);
          }
          if (newPropertyValue.length() > 0) {
            newPropertyValue.setLength(newPropertyValue.length() - 1); // drop trailing newline
          }
          allProperties.setProperty(newPropertyKey, newPropertyValue.toString());
        }
      }
    }
    // Now that all compile-scope dependencies have been seen, include only those test-scope
    // dependencies that are not also compile-scope dependencies.
    for (Map.Entry<String,SortedSet<String>> entry : testScopeDependencies.entrySet()) {
      String module = entry.getKey();
      SortedSet<String> testDeps = entry.getValue();
      SortedSet<String> compileDeps = internalCompileScopeDependencies.get(module);
      if (null == compileDeps) {
        throw new BuildException("Can't find compile scope dependencies for module " + module);
      }
      StringBuilder newPropertyValue = new StringBuilder();
      for (String dependency : testDeps) {
        // modules with separate compile-scope and test-scope POMs need their compile-scope deps
        // included in their test-scope deps.
        if (modulesWithSeparateCompileAndTestPOMs.contains(module) || ! compileDeps.contains(dependency)) {
          int splitPos = dependency.indexOf(':');
          String groupId = dependency.substring(0, splitPos);
          String artifactId = dependency.substring(splitPos + 1);
          appendDependencyXml(newPropertyValue, groupId, artifactId, "    ", null, true, false, null, null);
        }
      }
      if (newPropertyValue.length() > 0) {
        newPropertyValue.setLength(newPropertyValue.length() - 1); // drop trailing newline
      }
      allProperties.setProperty(testScopePropertyKeys.get(module), newPropertyValue.toString());
    }
  }

  /**
   * Converts either a compile output directory or an internal jar
   * dependency, taken from an Ant (test.)classpath, into an artifactId
   */
  private String dependencyToArtifactId(String newPropertyKey, String dependency) {
    StringBuilder artifactId = new StringBuilder();
    Matcher matcher = COMPILATION_OUTPUT_DIRECTORY_PATTERN.matcher(dependency);
    if (matcher.matches()) {
      // Pattern.compile("(lucene|solr)/build/(.*)/classes/java");
      String artifact = matcher.group(2);
      artifact = artifact.replace('/', '-');
      artifact = artifact.replaceAll("(?<!solr-)analysis-", "analyzers-");
      if ("lucene".equals(matcher.group(1))) {
        artifactId.append("lucene-");
      }
      artifactId.append(artifact);
    } else {
      matcher = internalJarPattern.matcher(dependency);
      if (matcher.matches()) {
        // internalJarPattern is /.*(lucene|solr)([^/]*?)-<version>\.jar/,
        // where <version> is the value of the Ant "version" property
        artifactId.append(matcher.group(1));
        artifactId.append(matcher.group(2));
      } else {
        throw new BuildException
            ("Malformed module dependency from '" + newPropertyKey + "': '" + dependency + "'");
      }
    }
    return artifactId.toString();
  }

  /**
   * Convert Ant project names to artifact names: prepend "lucene-"
   * to Lucene project names
   */
  private String antProjectToArtifactName(String origModule) {
    String module = origModule;
    if ( ! origModule.startsWith("solr-")) { // lucene modules names don't have "lucene-" prepended
      module = "lucene-" + module;
    }
    return module;
  }

  /**
   * Collect external dependencies from the given ivy.xml file, constructing
   * property values containing &lt;dependency&gt; snippets, which will be
   * filtered (substituted) when copying the POM for the module corresponding
   * to the given ivy.xml file.
   */
  private void collectExternalDependenciesFromIvyXmlFile(File ivyXmlFile)
      throws XPathExpressionException, IOException, SAXException {
    String module = getModuleName(ivyXmlFile);
    log("Collecting external dependencies from: " + ivyXmlFile.getPath(), verboseLevel);
    Document document = documentBuilder.parse(ivyXmlFile);
    // Exclude the 'start' configuration in solr/server/ivy.xml
    String dependencyPath = "/ivy-module/dependencies/dependency[not(starts-with(@conf,'start'))]";
    NodeList dependencies = (NodeList)xpath.evaluate(dependencyPath, document, XPathConstants.NODESET);
    for (int depNum = 0 ; depNum < dependencies.getLength() ; ++depNum) {
      Element dependency = (Element)dependencies.item(depNum);
      String groupId = dependency.getAttribute("org");
      String artifactId = dependency.getAttribute("name");
      String dependencyCoordinate = groupId + ':' + artifactId;
      Set<String> classifiers = dependencyClassifiers.get(dependencyCoordinate);
      if (null == classifiers) {
        classifiers = new HashSet<>();
        dependencyClassifiers.put(dependencyCoordinate, classifiers);
      }
      String conf = dependency.getAttribute("conf");
      boolean confContainsTest = conf.contains("test");
      boolean isOptional = globalOptionalExternalDependencies.contains(dependencyCoordinate)
          || ( perModuleOptionalExternalDependencies.containsKey(module)
              && perModuleOptionalExternalDependencies.get(module).contains(dependencyCoordinate));
      SortedSet<ExternalDependency> deps = allExternalDependencies.get(module);
      if (null == deps) {
        deps = new TreeSet<>();
        allExternalDependencies.put(module, deps);
      }
      NodeList artifacts = null;
      if (dependency.hasChildNodes()) {
        artifacts = (NodeList)xpath.evaluate("artifact", dependency, XPathConstants.NODESET);
      }
      if (null != artifacts && artifacts.getLength() > 0) {
        for (int artifactNum = 0 ; artifactNum < artifacts.getLength() ; ++artifactNum) {
          Element artifact = (Element)artifacts.item(artifactNum);
          String type = artifact.getAttribute("type");
          String ext = artifact.getAttribute("ext");
          // When conf contains BOTH "test" and "compile", and type != "test", this is NOT a test dependency
          boolean isTestDependency = confContainsTest && (type.equals("test") || ! conf.contains("compile"));
          if ((type.isEmpty() && ext.isEmpty()) || type.equals("jar") || ext.equals("jar")) {
            String classifier = artifact.getAttribute("maven:classifier");
            if (classifier.isEmpty()) {
              classifier = null;
            }
            classifiers.add(classifier);
            deps.add(new ExternalDependency(groupId, artifactId, classifier, isTestDependency, isOptional));
          } else { // not a jar
            nonJarDependencies.add(dependencyCoordinate);
          }
        }
      } else {
        classifiers.add(null);
        deps.add(new ExternalDependency(groupId, artifactId, null, confContainsTest, isOptional));
      }
    }
  }

  /**
   * Stores information about an external dependency
   */
  private static class ExternalDependency implements Comparable<ExternalDependency> {
    String groupId;
    String artifactId;
    boolean isTestDependency;
    boolean isOptional;
    String classifier;
    
    public ExternalDependency
        (String groupId, String artifactId, String classifier, boolean isTestDependency, boolean isOptional) {
      this.groupId = groupId;
      this.artifactId = artifactId;
      this.classifier = classifier;
      this.isTestDependency = isTestDependency;
      this.isOptional = isOptional;
    }
    
    @Override
    public boolean equals(Object o) {
      if ( ! (o instanceof ExternalDependency)) {
        return false;
      }
      ExternalDependency other = (ExternalDependency)o;
      return groupId.equals(other.groupId) 
          && artifactId.equals(other.artifactId) 
          && isTestDependency == other.isTestDependency
          && isOptional == other.isOptional
          && classifier.equals(other.classifier);
    } 
    
    @Override
    public int hashCode() {
      return groupId.hashCode() * 31
          + artifactId.hashCode() * 31
          + (isTestDependency ? 31 : 0)
          + (isOptional ? 31 : 0)
          + classifier.hashCode();
    }

    @Override
    public int compareTo(ExternalDependency other) {
      int comparison = groupId.compareTo(other.groupId);
      if (0 != comparison) {
        return comparison;
      }
      comparison = artifactId.compareTo(other.artifactId);
      if (0 != comparison) {
        return comparison;
      }
      if (null == classifier) {
        if (null != other.classifier) {
          return -1;
        }
      } else if (null == other.classifier) { // classifier is not null
        return 1;
      } else {                               // neither classifier is  null
        if (0 != (comparison = classifier.compareTo(other.classifier))) {
          return comparison;
        }
      }
      // test and optional don't matter in this sort
      return 0;
    }
  }
  
  /**
   * Extract module name from ivy.xml path.
   */
  private String getModuleName(File ivyXmlFile) {
    String path = ivyXmlFile.getAbsolutePath();
    Matcher matcher = PROPERTY_PREFIX_FROM_IVY_XML_FILE_PATTERN.matcher(path);
    if ( ! matcher.find()) {
      throw new BuildException("Can't get module name from ivy.xml path: " + path);
    }
    StringBuilder builder = new StringBuilder();
    builder.append(matcher.group(1));
    if (null != matcher.group(2)) { // "lucene/analysis/..."
      builder.append("-analyzers");
    } else if (null != matcher.group(3)) { // "solr/example/..."
      builder.append("-example");
    } else if (null != matcher.group(4)) { // "solr/server/..."
      builder.append("-server");
    }
    builder.append('-');
    builder.append(matcher.group(5));
    return builder.toString().replace("solr-solr-", "solr-");
  }

/**
 * Appends a &lt;dependency&gt; snippet to the given builder.
 */
  private void appendDependencyXml(StringBuilder builder, String groupId, String artifactId, 
                                   String indent, String version, boolean isTestDependency, 
                                   boolean isOptional, String classifier, Collection<String> exclusions) {
    builder.append(indent).append("<dependency>\n");
    builder.append(indent).append("  <groupId>").append(groupId).append("</groupId>\n");
    builder.append(indent).append("  <artifactId>").append(artifactId).append("</artifactId>\n");
    if (null != version) {
      builder.append(indent).append("  <version>").append(version).append("</version>\n");
    }
    if (isTestDependency) {
      builder.append(indent).append("  <scope>test</scope>\n");
    }
    if (isOptional) {
      builder.append(indent).append("  <optional>true</optional>\n");
    }
    if (null != classifier) {
      builder.append(indent).append("  <classifier>").append(classifier).append("</classifier>\n");
    }
    if ( ! modulesWithTransitiveDependencies.contains(artifactId) && null != exclusions && ! exclusions.isEmpty()) {
      builder.append(indent).append("  <exclusions>\n");
      for (String dependency : exclusions) {
        int splitPos = dependency.indexOf(':');
        String excludedGroupId = dependency.substring(0, splitPos);
        String excludedArtifactId = dependency.substring(splitPos + 1);
        builder.append(indent).append("    <exclusion>\n");
        builder.append(indent).append("      <groupId>").append(excludedGroupId).append("</groupId>\n");
        builder.append(indent).append("      <artifactId>").append(excludedArtifactId).append("</artifactId>\n");
        builder.append(indent).append("    </exclusion>\n");
      }
      builder.append(indent).append("  </exclusions>\n");
    }
    builder.append(indent).append("</dependency>\n");
  }
}

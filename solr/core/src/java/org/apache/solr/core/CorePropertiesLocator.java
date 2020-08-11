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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persists CoreDescriptors as properties files
 */
public class CorePropertiesLocator implements CoresLocator {

  public static final String PROPERTIES_FILENAME = "core.properties";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Path rootDirectory;

  public CorePropertiesLocator(Path coreDiscoveryRoot) {
    this.rootDirectory = coreDiscoveryRoot;
    log.debug("Config-defined core root directory: {}", this.rootDirectory);
  }

  @Override
  public void create(CoreContainer cc, CoreDescriptor... coreDescriptors) {
    for (CoreDescriptor cd : coreDescriptors) {
      Path propertiesFile = cd.getInstanceDir().resolve(PROPERTIES_FILENAME);
      if (Files.exists(propertiesFile))
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                "Could not create a new core in " + cd.getInstanceDir()
                              + " as another core is already defined there");
      writePropertiesFile(cd, propertiesFile);
    }
  }

  // TODO, this isn't atomic!  If we crash in the middle of a rename, we
  // could end up with two cores with identical names, in which case one of
  // them won't start up.  Are we happy with this?

  @Override
  public void persist(CoreContainer cc, CoreDescriptor... coreDescriptors) {
    for (CoreDescriptor cd : coreDescriptors) {
      Path propFile = cd.getInstanceDir().resolve(PROPERTIES_FILENAME);
      writePropertiesFile(cd, propFile);
    }
  }

  private void writePropertiesFile(CoreDescriptor cd, Path propfile)  {
    Properties p = buildCoreProperties(cd);
    try {
      FileUtils.createDirectories(propfile.getParent()); // Handling for symlinks.
      try (Writer os = new OutputStreamWriter(Files.newOutputStream(propfile), StandardCharsets.UTF_8)) {
        p.store(os, "Written by CorePropertiesLocator");
      }
    }
    catch (IOException e) {
      log.error("Couldn't persist core properties to {}: ", propfile, e);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Couldn't persist core properties to " + propfile.toAbsolutePath().toString() + " : " + e.getMessage());
    }
  }

  @Override
  public void delete(CoreContainer cc, CoreDescriptor... coreDescriptors) {
    if (coreDescriptors == null) {
      return;
    }
    for (CoreDescriptor cd : coreDescriptors) {
      if (cd == null) continue;
      Path propfile = cd.getInstanceDir().resolve(PROPERTIES_FILENAME);
      try {
        Files.deleteIfExists(propfile);
      } catch (IOException e) {
        log.warn("Couldn't delete core properties file {}: ", propfile, e);
      }
    }
  }

  @Override
  public void rename(CoreContainer cc, CoreDescriptor oldCD, CoreDescriptor newCD) {
    String oldName = newCD.getPersistableStandardProperties().getProperty(CoreDescriptor.CORE_NAME);
    String newName = newCD.coreProperties.getProperty(CoreDescriptor.CORE_NAME);
    if (oldName == null ||
        (newName != null && oldName.equals(newName) == false)) {
      newCD.getPersistableStandardProperties().put(CoreDescriptor.CORE_NAME, newName);
    }
    persist(cc, newCD);
  }

  @Override
  public void swap(CoreContainer cc, CoreDescriptor cd1, CoreDescriptor cd2) {
    persist(cc, cd1, cd2);
  }

  @Override
  public List<CoreDescriptor> discover(final CoreContainer cc) {
    log.debug("Looking for core definitions underneath {}", rootDirectory);
    final List<CoreDescriptor> cds = Lists.newArrayList();
    try {
      Set<FileVisitOption> options = new HashSet<>();
      options.add(FileVisitOption.FOLLOW_LINKS);
      final int maxDepth = 256;
      Files.walkFileTree(this.rootDirectory, options, maxDepth, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          if (file.getFileName().toString().equals(PROPERTIES_FILENAME)) {
            CoreDescriptor cd = buildCoreDescriptor(file, cc);
            if (cd != null) {
              if (log.isDebugEnabled()) {
                log.debug("Found core {} in {}", cd.getName(), cd.getInstanceDir());
              }
              cds.add(cd);
            }
            return FileVisitResult.SKIP_SIBLINGS;
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
          // if we get an error on the root, then fail the whole thing
          // otherwise, log a warning and continue to try and load other cores
          if (file.equals(rootDirectory)) {
            log.error("Error reading core root directory {}: {}", file, exc);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error reading core root directory");
          }
          log.warn("Error visiting {}: {}", file, exc);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Couldn't walk file tree under " + this.rootDirectory, e);
    }
    if (log.isInfoEnabled()) {
      log.info("Found {} core definitions underneath {}", cds.size(), rootDirectory);
    }
    if (cds.size() > 0) {
      if (log.isInfoEnabled()) {
        log.info("Cores are: {}", cds.stream().map(CoreDescriptor::getName).collect(Collectors.toList()));
      }
    }
    return cds;
  }

  protected CoreDescriptor buildCoreDescriptor(Path propertiesFile, CoreContainer cc) {

    Path instanceDir = propertiesFile.getParent();
    Properties coreProperties = new Properties();
    try (InputStream fis = Files.newInputStream(propertiesFile)) {
      coreProperties.load(new InputStreamReader(fis, StandardCharsets.UTF_8));
      String name = createName(coreProperties, instanceDir);
      Map<String, String> propMap = new HashMap<>();
      for (String key : coreProperties.stringPropertyNames()) {
        propMap.put(key, coreProperties.getProperty(key));
      }
      CoreDescriptor ret = new CoreDescriptor(name, instanceDir, propMap, cc.getContainerProperties(), cc.getZkController());
      ret.loadExtraProperties();
      return ret;
    }
    catch (IOException e) {
      log.error("Couldn't load core descriptor from {}:", propertiesFile, e);
      return null;
    }

  }

  protected static String createName(Properties p, Path instanceDir) {
    return p.getProperty(CoreDescriptor.CORE_NAME, instanceDir.getFileName().toString());
  }

  protected Properties buildCoreProperties(CoreDescriptor cd) {
    Properties p = new Properties();
    p.putAll(cd.getPersistableStandardProperties());
    p.putAll(cd.getPersistableUserProperties());
    return p;
  }

}

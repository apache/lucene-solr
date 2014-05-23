package org.apache.solr.core;

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

import com.google.common.collect.Lists;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

/**
 * Persists CoreDescriptors as properties files
 */
public class CorePropertiesLocator implements CoresLocator {

  public static final String PROPERTIES_FILENAME = "core.properties";

  private static final Logger logger = LoggerFactory.getLogger(CoresLocator.class);

  private final File rootDirectory;

  public CorePropertiesLocator(String coreDiscoveryRoot) {
    this.rootDirectory = new File(coreDiscoveryRoot);
    logger.info("Config-defined core root directory: {}", this.rootDirectory.getAbsolutePath());
  }

  @Override
  public void create(CoreContainer cc, CoreDescriptor... coreDescriptors) {
    for (CoreDescriptor cd : coreDescriptors) {
      File propFile = new File(new File(cd.getInstanceDir()), PROPERTIES_FILENAME);
      if (propFile.exists())
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                "Could not create a new core in " + cd.getInstanceDir()
                              + "as another core is already defined there");
      writePropertiesFile(cd, propFile);
    }
  }

  // TODO, this isn't atomic!  If we crash in the middle of a rename, we
  // could end up with two cores with identical names, in which case one of
  // them won't start up.  Are we happy with this?

  @Override
  public void persist(CoreContainer cc, CoreDescriptor... coreDescriptors) {
    for (CoreDescriptor cd : coreDescriptors) {
      File propFile = new File(new File(cd.getInstanceDir()), PROPERTIES_FILENAME);
      writePropertiesFile(cd, propFile);
    }
  }

  private void writePropertiesFile(CoreDescriptor cd, File propfile)  {
    Properties p = buildCoreProperties(cd);
    Writer os = null;
    try {
      propfile.getParentFile().mkdirs();
      os = new OutputStreamWriter(new FileOutputStream(propfile), StandardCharsets.UTF_8);
      p.store(os, "Written by CorePropertiesLocator");
    }
    catch (IOException e) {
      logger.error("Couldn't persist core properties to {}: {}", propfile.getAbsolutePath(), e);
    }
    finally {
      IOUtils.closeQuietly(os);
    }
  }

  @Override
  public void delete(CoreContainer cc, CoreDescriptor... coreDescriptors) {
    if (coreDescriptors == null) {
      return;
    }
    for (CoreDescriptor cd : coreDescriptors) {
      if (cd == null) continue;
      File instanceDir = new File(cd.getInstanceDir());
      File propertiesFile = new File(instanceDir, PROPERTIES_FILENAME);
      propertiesFile.renameTo(new File(instanceDir, PROPERTIES_FILENAME + ".unloaded"));
      // This is a best-effort: the core.properties file may already have been
      // deleted by the core unload, so we don't worry about checking if the
      // rename has succeeded.
    }
  }

  @Override
  public void rename(CoreContainer cc, CoreDescriptor oldCD, CoreDescriptor newCD) {
    persist(cc, newCD);
  }

  @Override
  public void swap(CoreContainer cc, CoreDescriptor cd1, CoreDescriptor cd2) {
    persist(cc, cd1, cd2);
  }

  @Override
  public List<CoreDescriptor> discover(CoreContainer cc) {
    logger.info("Looking for core definitions underneath {}", rootDirectory.getAbsolutePath());
    List<CoreDescriptor> cds = Lists.newArrayList();
    discoverUnder(rootDirectory, cds, cc);
    logger.info("Found {} core definitions", cds.size());
    return cds;
  }

  private void discoverUnder(File root, List<CoreDescriptor> cds, CoreContainer cc) {
    if (!root.exists())
      return;
    for (File child : root.listFiles()) {
      File propertiesFile = new File(child, PROPERTIES_FILENAME);
      if (propertiesFile.exists()) {
        CoreDescriptor cd = buildCoreDescriptor(propertiesFile, cc);
        logger.info("Found core {} in {}", cd.getName(), cd.getInstanceDir());
        cds.add(cd);
        continue;
      }
      if (child.isDirectory())
        discoverUnder(child, cds, cc);
    }
  }

  protected CoreDescriptor buildCoreDescriptor(File propertiesFile, CoreContainer cc) {
    FileInputStream fis = null;
    try {
      File instanceDir = propertiesFile.getParentFile();
      Properties coreProperties = new Properties();
      fis = new FileInputStream(propertiesFile);
      coreProperties.load(new InputStreamReader(fis, StandardCharsets.UTF_8));
      String name = createName(coreProperties, instanceDir);
      return new CoreDescriptor(cc, name, instanceDir.getAbsolutePath(), coreProperties);
    }
    catch (IOException e) {
      logger.error("Couldn't load core descriptor from {}:{}", propertiesFile.getAbsolutePath(), e.toString());
      return null;
    }
    finally {
      IOUtils.closeQuietly(fis);
    }
  }

  protected static String createName(Properties p, File instanceDir) {
    return p.getProperty(CoreDescriptor.CORE_NAME, instanceDir.getName());
  }

  protected Properties buildCoreProperties(CoreDescriptor cd) {
    Properties p = new Properties();
    p.putAll(cd.getPersistableStandardProperties());
    p.putAll(cd.getPersistableUserProperties());
    // We don't persist the instance directory, as that's defined by the location
    // of the properties file.
    p.remove(CoreDescriptor.CORE_INSTDIR);
    return p;
  }

}

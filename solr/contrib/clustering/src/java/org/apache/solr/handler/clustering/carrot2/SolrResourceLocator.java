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

package org.apache.solr.handler.clustering.carrot2;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.carrot2.util.resource.IResource;
import org.carrot2.util.resource.IResourceLocator;

/**
 * A {@link IResourceLocator} that delegates resource searches to {@link SolrCore}.
 * 
 * @lucene.experimental
 */
class SolrResourceLocator implements IResourceLocator {
  private final SolrResourceLoader resourceLoader;
  private final String carrot2ResourcesDir;

  public SolrResourceLocator(SolrCore core, SolrParams initParams) {
    resourceLoader = core.getResourceLoader();
    
    @SuppressWarnings("deprecation")
    String lexicalResourcesDir = initParams.get(CarrotParams.LEXICAL_RESOURCES_DIR);
    String resourcesDir = initParams.get(CarrotParams.RESOURCES_DIR);
    carrot2ResourcesDir = firstNonNull(resourcesDir, lexicalResourcesDir, CarrotClusteringEngine.CARROT_RESOURCES_PREFIX);
  }

  @SuppressWarnings("unchecked")
  public static <T> T firstNonNull(T... args) {
    for (T t : args) {
      if (t != null) return t;
    }
    throw new NullPointerException("At least one element has to be non-null.");
  }

  @Override
  public IResource[] getAll(final String resource) {
    final String resourceName = carrot2ResourcesDir + "/" + resource;
    CarrotClusteringEngine.log.debug("Looking for Solr resource: " + resourceName);

    InputStream resourceStream = null;
    final byte [] asBytes;
    try {
      resourceStream = resourceLoader.openResource(resourceName);
      asBytes = IOUtils.toByteArray(resourceStream);
    } catch (IOException e) {
      CarrotClusteringEngine.log.debug("Resource not found in Solr's config: " + resourceName
          + ". Using the default " + resource + " from Carrot JAR.");          
      return new IResource[] {};
    } finally {
      if (resourceStream != null) {
        try {
          resourceStream.close();
        } catch (IOException e) {
          // ignore.
        }
      }
    }

    CarrotClusteringEngine.log.info("Loaded Solr resource: " + resourceName);

    final IResource foundResource = new IResource() {
      @Override
      public InputStream open() {
        return new ByteArrayInputStream(asBytes);
      }

      @Override
      public int hashCode() {
        // In case multiple resources are found they will be deduped, but we don't use it in Solr,
        // so simply rely on instance equivalence.
        return super.hashCode();
      }
      
      @Override
      public boolean equals(Object obj) {
        // In case multiple resources are found they will be deduped, but we don't use it in Solr,
        // so simply rely on instance equivalence.
        return super.equals(obj);
      }

      @Override
      public String toString() {
        return "Solr config resource: " + resourceName;
      }
    };

    return new IResource[] { foundResource };
  }

  @Override
  public int hashCode() {
    // In case multiple locations are used locators will be deduped, but we don't use it in Solr,
    // so simply rely on instance equivalence.
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // In case multiple locations are used locators will be deduped, but we don't use it in Solr,
    // so simply rely on instance equivalence.
    return super.equals(obj);
  }

  @Override
  public String toString() {
    String configDir = "";
    try {
      configDir = "configDir=" + new File(resourceLoader.getConfigDir()).getAbsolutePath() + ", ";
    } catch (Exception ignored) {
      // If we get the exception, the resource loader implementation
      // probably does not support getConfigDir(). Not a big problem.
    }
    
    return "SolrResourceLocator, " + configDir
        + "Carrot2 relative lexicalResourcesDir=" + carrot2ResourcesDir;
  }
}
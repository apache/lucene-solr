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
package org.apache.solr.response;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.collections.ExtendedProperties;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.runtime.resource.Resource;
import org.apache.velocity.runtime.resource.loader.ResourceLoader;

/**
  * Velocity resource loader wrapper around Solr resource loader
  */
public class SolrVelocityResourceLoader extends ResourceLoader {
  private SolrResourceLoader loader;

  public SolrVelocityResourceLoader(SolrResourceLoader loader) {
    super();
    this.loader = loader;
  }

  @Override
  public void init(ExtendedProperties extendedProperties) {
  }

  @Override
  public InputStream getResourceStream(String template_name) throws ResourceNotFoundException {
    try {
      return loader.openResource("velocity/" + template_name);
    } catch (IOException ioe) {
      throw new ResourceNotFoundException(ioe);
    }
  }

  @Override
  public boolean isSourceModified(Resource resource) {
    return false;
  }

  @Override
  public long getLastModified(Resource resource) {
    return 0;
  }
}

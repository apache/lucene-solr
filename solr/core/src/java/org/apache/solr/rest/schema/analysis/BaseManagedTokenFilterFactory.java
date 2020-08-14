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
package org.apache.solr.rest.schema.analysis;
import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;

/**
 * Abstract based class for implementing TokenFilterFactory objects that
 * are managed by the REST API. Specifically, this base class is useful
 * for token filters that have configuration and data that needs to be
 * updated programmatically, such as to support a UI for adding synonyms.  
 * @since 4.8.0
 */
public abstract class BaseManagedTokenFilterFactory extends TokenFilterFactory 
  implements ResourceLoaderAware, ManagedResourceObserver
{
  
  protected String handle;
  
  protected BaseManagedTokenFilterFactory(Map<String,String> args) {
    super(args);    
    handle = require(args, "managed");
    
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }    
  }
  
  /**
   * Registers an endpoint with the RestManager so that this component can be
   * managed using the REST API. This method can be invoked before all the
   * resources the {@link org.apache.solr.rest.RestManager} needs to initialize
   * a {@link ManagedResource} are available, so this simply registers the need
   * to be managed at a specific endpoint and lets the RestManager deal with
   * initialization when ready.
   */
  @Override
  public void inform(ResourceLoader loader) throws IOException {
    SolrResourceLoader solrResourceLoader = (SolrResourceLoader)loader;
    
    // here we want to register that we need to be managed
    // at a specified path and the ManagedResource impl class
    // that should be used to manage this component
    solrResourceLoader.getManagedResourceRegistry().
      registerManagedResource(getResourceId(), getManagedResourceImplClass(), this);
  }
  
  /**
   * Let the concrete analysis component decide the path it wishes to be managed at. 
   */
  protected abstract String getResourceId();
  
  /**
   * Let the concrete analysis component determine the ManagedResource implementation.
   * As there can be many instances of the same analysis component in a schema, this
   * class should not presume to create ManagedResource. For instance, there may be
   * 10 instances of the ManagedStopFilterFactory that use the same set of English 
   * stop words and we don't want 10 copies of the ManagedWordSetResource in the same core. 
   */
  protected abstract Class<? extends ManagedResource> getManagedResourceImplClass();  
}

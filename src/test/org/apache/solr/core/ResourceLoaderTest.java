/**
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

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.solr.analysis.KeywordTokenizerFactory;
import org.apache.solr.analysis.NGramFilterFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.admin.LukeRequestHandler;
import org.apache.solr.handler.component.FacetComponent;
import org.apache.solr.request.JSONResponseWriter;
import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.io.File;

public class ResourceLoaderTest extends TestCase 
{
  public void testInstanceDir() throws Exception {
    SolrResourceLoader loader = new SolrResourceLoader(null);
    String instDir = loader.getInstanceDir();
    assertTrue(instDir + " is not equal to " + "solr/", instDir.equals("solr/") == true);

    loader = new SolrResourceLoader("solr");
    instDir = loader.getInstanceDir();
    assertTrue(instDir + " is not equal to " + "solr/", instDir.equals("solr" + File.separator) == true);
  }

  public void testAwareCompatibility() 
  {
    SolrResourceLoader loader = new SolrResourceLoader( "." );
    
    Class clazz = ResourceLoaderAware.class;
    // Check ResourceLoaderAware valid objects
    loader.assertAwareCompatibility( clazz, new NGramFilterFactory() );
    loader.assertAwareCompatibility( clazz, new KeywordTokenizerFactory() );
    
    // Make sure it throws an error for invalid objects
    Object[] invalid = new Object[] {
        // new NGramTokenFilter( null ),
        "hello",  new Float( 12.3f ),
        new LukeRequestHandler(),
        new JSONResponseWriter()
    };
    for( Object obj : invalid ) {
      try {
        loader.assertAwareCompatibility( clazz, obj );
        Assert.fail( "Should be invalid class: "+obj + " FOR " + clazz );
      }
      catch( SolrException ex ) { } // OK
    }
    

    clazz = SolrCoreAware.class;
    // Check ResourceLoaderAware valid objects
    loader.assertAwareCompatibility( clazz, new LukeRequestHandler() );
    loader.assertAwareCompatibility( clazz, new FacetComponent() );
    loader.assertAwareCompatibility( clazz, new JSONResponseWriter() );
    
    // Make sure it throws an error for invalid objects
    invalid = new Object[] {
        new NGramFilterFactory(),
        "hello",  new Float( 12.3f ),
        new KeywordTokenizerFactory()
    };
    for( Object obj : invalid ) {
      try {
        loader.assertAwareCompatibility( clazz, obj );
        Assert.fail( "Should be invalid class: "+obj + " FOR " + clazz );
      }
      catch( SolrException ex ) { } // OK
    }
  }
}
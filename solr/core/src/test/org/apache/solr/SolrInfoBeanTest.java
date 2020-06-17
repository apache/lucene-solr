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
package org.apache.solr;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.handler.admin.LukeRequestHandler;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.highlight.DefaultSolrHighlighter;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.search.LRUCache;
import org.junit.BeforeClass;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * A simple test used to increase code coverage for some standard things...
 */
public class SolrInfoBeanTest extends SolrTestCaseJ4
{
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  /**
   * Gets a list of everything we can find in the classpath and makes sure it has
   * a name, description, etc...
   */
  @SuppressWarnings({"unchecked"})
  public void testCallMBeanInfo() throws Exception {
    @SuppressWarnings({"rawtypes"})
    List<Class> classes = new ArrayList<>();
    classes.addAll(getClassesForPackage(SearchHandler.class.getPackage().getName()));
    classes.addAll(getClassesForPackage(SearchComponent.class.getPackage().getName()));
    classes.addAll(getClassesForPackage(LukeRequestHandler.class.getPackage().getName()));
    classes.addAll(getClassesForPackage(DefaultSolrHighlighter.class.getPackage().getName()));
    classes.addAll(getClassesForPackage(LRUCache.class.getPackage().getName()));
   // System.out.println(classes);
    
    int checked = 0;
    SolrMetricManager metricManager = h.getCoreContainer().getMetricManager();
    String registry = h.getCore().getCoreMetricManager().getRegistryName();
    String scope = TestUtil.randomSimpleString(random(), 2, 10);
    for(@SuppressWarnings({"rawtypes"})Class clazz : classes ) {
      if( SolrInfoBean.class.isAssignableFrom( clazz ) ) {
        try {
          SolrInfoBean info = (SolrInfoBean)clazz.newInstance();
          if (info instanceof SolrMetricProducer) {
            ((SolrMetricProducer)info).initializeMetrics(metricManager, registry, "foo", scope);
          }
          
          //System.out.println( info.getClass() );
          assertNotNull( info.getClass().getCanonicalName(), info.getName() );
          assertNotNull( info.getClass().getCanonicalName(), info.getDescription() );
          assertNotNull( info.getClass().getCanonicalName(), info.getCategory() );
          
          if( info instanceof LRUCache ) {
            continue;
          }
          
          assertNotNull( info.toString() );
          checked++;
        }
        catch( InstantiationException ex ) {
          // expected...
          //System.out.println( "unable to initialize: "+clazz );
        }
      }
    }
    assertTrue( "there are at least 10 SolrInfoBean that should be found in the classpath, found " + checked, checked > 10 );
  }
  
  @SuppressWarnings({"rawtypes"})
  private static List<Class> getClassesForPackage(String pckgname) throws Exception {
    ArrayList<File> directories = new ArrayList<>();
    ClassLoader cld = h.getCore().getResourceLoader().getClassLoader();
    String path = pckgname.replace('.', '/');
    Enumeration<URL> resources = cld.getResources(path);
    while (resources.hasMoreElements()) {
      final URI uri = resources.nextElement().toURI();
      if (!"file".equalsIgnoreCase(uri.getScheme()))
        continue;
      final File f = new File(uri);
      directories.add(f);
    }
      
    @SuppressWarnings({"rawtypes"})
    ArrayList<Class> classes = new ArrayList<>();
    for (File directory : directories) {
      if (directory.exists()) {
        String[] files = directory.list();
        for (String file : files) {
          if (file.endsWith(".class")) {
             String clazzName = file.substring(0, file.length() - 6);
             // exclude Test classes that happen to be in these packages.
             // class.ForName'ing some of them can cause trouble.
             if (!clazzName.endsWith("Test") && !clazzName.startsWith("Test")) {
               classes.add(Class.forName(pckgname + '.' + clazzName));
             }
          }
        }
      }
    }
    assertFalse("No classes found in package '"+pckgname+"'; maybe your test classes are packaged as JAR file?", classes.isEmpty());
    return classes;
  }
}

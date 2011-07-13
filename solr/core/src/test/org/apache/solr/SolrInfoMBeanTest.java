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
package org.apache.solr;

import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.handler.StandardRequestHandler;
import org.apache.solr.handler.admin.LukeRequestHandler;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.highlight.DefaultSolrHighlighter;
import org.apache.solr.search.LRUCache;
import org.junit.BeforeClass;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * A simple test used to increase code coverage for some standard things...
 */
public class SolrInfoMBeanTest extends SolrTestCaseJ4
{
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  /**
   * Gets a list of everything we can find in the classpath and makes sure it has
   * a name, description, etc...
   */
  public void testCallMBeanInfo() throws Exception {
    List<Class> classes = new ArrayList<Class>();
    classes.addAll(getClassesForPackage(StandardRequestHandler.class.getPackage().getName()));
    classes.addAll(getClassesForPackage(SearchHandler.class.getPackage().getName()));
    classes.addAll(getClassesForPackage(SearchComponent.class.getPackage().getName()));
    classes.addAll(getClassesForPackage(LukeRequestHandler.class.getPackage().getName()));
    classes.addAll(getClassesForPackage(DefaultSolrHighlighter.class.getPackage().getName()));
    classes.addAll(getClassesForPackage(LRUCache.class.getPackage().getName()));
   // System.out.println(classes);
    
    int checked = 0;
    for( Class clazz : classes ) {
      if( SolrInfoMBean.class.isAssignableFrom( clazz ) ) {
        try {
          SolrInfoMBean info = (SolrInfoMBean)clazz.newInstance();
          
          //System.out.println( info.getClass() );
          assertNotNull( info.getName() );
          assertNotNull( info.getDescription() );
          assertNotNull( info.getSource() );
          assertNotNull( info.getSourceId() );
          assertNotNull( info.getVersion() );
          assertNotNull( info.getCategory() );

          if( info instanceof LRUCache ) {
            continue;
          }
          
          assertNotNull( info.toString() );
          // increase code coverage...
          assertNotNull( info.getDocs() + "" );
          assertNotNull( info.getStatistics()+"" );
          checked++;
        }
        catch( InstantiationException ex ) {
          // expected...
          //System.out.println( "unable to initalize: "+clazz );
        }
      }
    }
    assertTrue( "there are at least 10 SolrInfoMBean that should be found in the classpath, found " + checked, checked > 10 );
  }
  
  private static List<Class> getClassesForPackage(String pckgname) throws Exception {
    ArrayList<File> directories = new ArrayList<File>();
    ClassLoader cld = h.getCore().getResourceLoader().getClassLoader();
    String path = pckgname.replace('.', '/');
    Enumeration<URL> resources = cld.getResources(path);
    while (resources.hasMoreElements()) {
      final File f = new File(resources.nextElement().toURI());
      directories.add(f);
    }
      
    ArrayList<Class> classes = new ArrayList<Class>();
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
    return classes;
  }
}

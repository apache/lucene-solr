package org.apache.solr;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.management.Query;

import org.apache.lucene.search.function.FieldCacheSource;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.handler.StandardRequestHandler;
import org.apache.solr.handler.admin.LukeRequestHandler;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.highlight.DefaultSolrHighlighter;
import org.apache.solr.search.LRUCache;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.update.UpdateHandler;

import junit.framework.TestCase;

/**
 * A simple test used to increase code coverage for some standard things...
 */
public class SolrInfoMBeanTest extends TestCase 
{
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
    assertTrue( "there are at leaset 10 SolrInfoMBean that should be found in the classpath.", checked > 10 );
  }

  private static List<Class> getClassesForPackage(String pckgname) throws Exception {
    ArrayList<File> directories = new ArrayList<File>();
    ClassLoader cld = Thread.currentThread().getContextClassLoader();
    String path = pckgname.replace('.', '/');
    Enumeration<URL> resources = cld.getResources(path);
    while (resources.hasMoreElements()) {
      directories.add(new File(URLDecoder.decode(resources.nextElement().getPath(), "UTF-8")));
    }
      
    ArrayList<Class> classes = new ArrayList<Class>();
    for (File directory : directories) {
      if (directory.exists()) {
        String[] files = directory.list();
        for (String file : files) {
          if (file.endsWith(".class")) {
             classes.add(Class.forName(pckgname + '.' + file.substring(0, file.length() - 6)));
          }
        }
      }
    }
    return classes;
  }
}

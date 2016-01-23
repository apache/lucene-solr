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

package org.apache.solr.handler.admin;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.LucenePackage;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This handler returns system info
 * 
 * NOTE: the response format is still likely to change.  It should be designed so
 * that it works nicely with an XSLT transformation.  Until we have a nice
 * XSLT front end for /admin, the format is still open to change.
 * 
 *
 * @since solr 1.2
 */
public class SystemInfoHandler extends RequestHandlerBase 
{
  private static Logger log = LoggerFactory.getLogger(SystemInfoHandler.class);
  

  // on some platforms, resolving canonical hostname can cause the thread
  // to block for several seconds if nameservices aren't available
  // so resolve this once per handler instance 
  //(ie: not static, so core reload will refresh)
  private String hostname = null;

  public SystemInfoHandler() {
    super();
    try {
      InetAddress addr = InetAddress.getLocalHost();
      hostname = addr.getCanonicalHostName();
    } catch (UnknownHostException e) {
      //default to null
    }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception
  {
    rsp.add( "core", getCoreInfo( req.getCore() ) );
    rsp.add( "lucene", getLuceneInfo() );
    rsp.add( "jvm", getJvmInfo() );
    rsp.add( "system", getSystemInfo() );
    rsp.setHttpCaching(false);
  }
  
  /**
   * Get system info
   */
  private SimpleOrderedMap<Object> getCoreInfo( SolrCore core ) throws Exception 
  {
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<Object>();
    
    IndexSchema schema = core.getSchema();
    info.add( "schema", schema != null ? schema.getSchemaName():"no schema!" );
    
    // Host
    info.add( "host", hostname );

    // Now
    info.add( "now", new Date() );
    
    // Start Time
    info.add( "start", new Date(core.getStartTime()) );

    // Solr Home
    SimpleOrderedMap<Object> dirs = new SimpleOrderedMap<Object>();
    dirs.add( "cwd" , new File( System.getProperty("user.dir")).getAbsolutePath() );
    dirs.add( "instance", new File( core.getResourceLoader().getInstanceDir() ).getAbsolutePath() );
    dirs.add( "data", new File( core.getDataDir() ).getAbsolutePath() );
    dirs.add( "index", new File( core.getIndexDir() ).getAbsolutePath() );
    info.add( "directory", dirs );
    return info;
  }
  
  /**
   * Get system info
   */
  public static SimpleOrderedMap<Object> getSystemInfo() throws Exception 
  {
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<Object>();
    
    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    info.add( "name", os.getName() );
    info.add( "version", os.getVersion() );
    info.add( "arch", os.getArch() );

    // Java 1.6
    addGetterIfAvaliable( os, "systemLoadAverage", info );

    // com.sun.management.UnixOperatingSystemMXBean
    addGetterIfAvaliable( os, "openFileDescriptorCount", info );
    addGetterIfAvaliable( os, "maxFileDescriptorCount", info );

    // com.sun.management.OperatingSystemMXBean
    addGetterIfAvaliable( os, "committedVirtualMemorySize", info );
    addGetterIfAvaliable( os, "totalPhysicalMemorySize", info );
    addGetterIfAvaliable( os, "totalSwapSpaceSize", info );
    addGetterIfAvaliable( os, "processCpuTime", info );

    try { 
      if( !os.getName().toLowerCase(Locale.ENGLISH).startsWith( "windows" ) ) {
        // Try some command line things
        info.add( "uname",  execute( "uname -a" ) );
        info.add( "ulimit", execute( "ulimit -n" ) );
        info.add( "uptime", execute( "uptime" ) );
      }
    }
    catch( Throwable ex ) {} // ignore
    return info;
  }
  
  /**
   * Try to run a getter function.  This is useful because java 1.6 has a few extra
   * useful functions on the <code>OperatingSystemMXBean</code>
   * 
   * If you are running a sun jvm, there are nice functions in:
   * UnixOperatingSystemMXBean and com.sun.management.OperatingSystemMXBean
   * 
   * it is package protected so it can be tested...
   */
  static void addGetterIfAvaliable( Object obj, String getter, NamedList<Object> info )
  {
    // This is a 1.6 function, so lets do a little magic to *try* to make it work
    try {
      String n = Character.toUpperCase( getter.charAt(0) ) + getter.substring( 1 );
      Method m = obj.getClass().getMethod( "get" + n );
      Object v = m.invoke( obj, (Object[])null );
      if( v != null ) {
        info.add( getter, v );
      }
    }
    catch( Exception ex ) {} // don't worry, this only works for 1.6
  }
  
  
  /**
   * Utility function to execute a function
   */
  private static String execute( String cmd )
  {
    DataInputStream in = null;
    BufferedReader reader = null;
    
    try {
      Process process = Runtime.getRuntime().exec(cmd);
      in = new DataInputStream( process.getInputStream() );
      // use default charset from locale here, because the command invoked also uses the default locale:
      return IOUtils.toString( in );
    }
    catch( Exception ex ) {
      // ignore - log.warn("Error executing command", ex);
      return "(error executing: " + cmd + ")";
    }
    finally {
      IOUtils.closeQuietly( reader );
      IOUtils.closeQuietly( in );
    }
  }
  
  /**
   * Get JVM Info - including memory info
   */
  public static SimpleOrderedMap<Object> getJvmInfo()
  {
    SimpleOrderedMap<Object> jvm = new SimpleOrderedMap<Object>();
    jvm.add( "version", System.getProperty("java.vm.version") );
    jvm.add( "name", System.getProperty("java.vm.name") );
    
    Runtime runtime = Runtime.getRuntime();
    jvm.add( "processors", runtime.availableProcessors() );
    
    // not thread safe, but could be thread local
    DecimalFormat df = new DecimalFormat("#.#");

    SimpleOrderedMap<Object> mem = new SimpleOrderedMap<Object>();
    SimpleOrderedMap<Object> raw = new SimpleOrderedMap<Object>();
    long free = runtime.freeMemory();
    long max = runtime.maxMemory();
    long total = runtime.totalMemory();
    long used = total - free;
    double percentUsed = ((double)(used)/(double)max)*100;
    raw.add("free",  free );
    mem.add("free",  humanReadableUnits(free, df));
    raw.add("total", total );
    mem.add("total", humanReadableUnits(total, df));
    raw.add("max",   max );
    mem.add("max",   humanReadableUnits(max, df));
    raw.add("used",  used );
    mem.add("used",  humanReadableUnits(used, df) + 
            " (%" + df.format(percentUsed) + ")");
    raw.add("used%", percentUsed);

    mem.add("raw", raw);
    jvm.add("memory", mem);

    // JMX properties -- probably should be moved to a different handler
    SimpleOrderedMap<Object> jmx = new SimpleOrderedMap<Object>();
    try{
      RuntimeMXBean mx = ManagementFactory.getRuntimeMXBean();
      jmx.add( "bootclasspath", mx.getBootClassPath());
      jmx.add( "classpath", mx.getClassPath() );

      // the input arguments passed to the Java virtual machine
      // which does not include the arguments to the main method.
      jmx.add( "commandLineArgs", mx.getInputArguments());

      jmx.add( "startTime", new Date(mx.getStartTime()));
      jmx.add( "upTimeMS",  mx.getUptime() );

    }
    catch (Exception e) {
      log.warn("Error getting JMX properties", e);
    }
    jvm.add( "jmx", jmx );
    return jvm;
  }
  
  private static SimpleOrderedMap<Object> getLuceneInfo() throws Exception 
  {
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<Object>();

    Package p = SolrCore.class.getPackage();

    info.add( "solr-spec-version", p.getSpecificationVersion() );
    info.add( "solr-impl-version", p.getImplementationVersion() );
  
    p = LucenePackage.class.getPackage();

    info.add( "lucene-spec-version", p.getSpecificationVersion() );
    info.add( "lucene-impl-version", p.getImplementationVersion() );

    return info;
  }
  
  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Get System Info";
  }

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
  
  private static final long ONE_KB = 1024;
  private static final long ONE_MB = ONE_KB * ONE_KB;
  private static final long ONE_GB = ONE_KB * ONE_MB;

  /**
   * Return good default units based on byte size.
   */
  private static String humanReadableUnits(long bytes, DecimalFormat df) {
    String newSizeAndUnits;

    if (bytes / ONE_GB > 0) {
      newSizeAndUnits = String.valueOf(df.format((float)bytes / ONE_GB)) + " GB";
    } else if (bytes / ONE_MB > 0) {
      newSizeAndUnits = String.valueOf(df.format((float)bytes / ONE_MB)) + " MB";
    } else if (bytes / ONE_KB > 0) {
      newSizeAndUnits = String.valueOf(df.format((float)bytes / ONE_KB)) + " KB";
    } else {
      newSizeAndUnits = String.valueOf(bytes) + " bytes";
    }

    return newSizeAndUnits;
  }
  
}




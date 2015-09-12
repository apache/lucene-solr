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

package org.apache.solr.handler.admin;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.PlatformManagedObject;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.LucenePackage;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.NAME;


/**
 * This handler returns system info
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

  private CoreContainer cc;

  public SystemInfoHandler() {
    super();
    init();
  }

  public SystemInfoHandler(CoreContainer cc) {
    super();
    this.cc = cc;
    init();
  }
  
  private void init() {
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
    SolrCore core = req.getCore();
    if (core != null) rsp.add( "core", getCoreInfo( core, req.getSchema() ) );
    boolean solrCloudMode =  getCoreContainer(req, core).isZooKeeperAware();
    rsp.add( "mode", solrCloudMode ? "solrcloud" : "std");
    if (solrCloudMode) {
      rsp.add("zkHost", getCoreContainer(req, core).getZkController().getZkServerAddress());
    }
    if (cc != null)
      rsp.add( "solr_home", cc.getSolrHome());
    rsp.add( "lucene", getLuceneInfo() );
    rsp.add( "jvm", getJvmInfo() );
    rsp.add( "system", getSystemInfo() );
    rsp.setHttpCaching(false);
  }

  private CoreContainer getCoreContainer(SolrQueryRequest req, SolrCore core) {
    CoreContainer coreContainer;
    if (core != null) {
       coreContainer = req.getCore().getCoreDescriptor().getCoreContainer();
    } else {
      coreContainer = cc;
    }
    return coreContainer;
  }
  
  /**
   * Get system info
   */
  private SimpleOrderedMap<Object> getCoreInfo( SolrCore core, IndexSchema schema ) {
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();
    
    info.add( "schema", schema != null ? schema.getSchemaName():"no schema!" );
    
    // Host
    info.add( "host", hostname );

    // Now
    info.add( "now", new Date() );
    
    // Start Time
    info.add( "start", core.getStartTimeStamp() );

    // Solr Home
    SimpleOrderedMap<Object> dirs = new SimpleOrderedMap<>();
    dirs.add( "cwd" , new File( System.getProperty("user.dir")).getAbsolutePath() );
    dirs.add( "instance", new File( core.getResourceLoader().getInstanceDir() ).getAbsolutePath() );
    try {
      dirs.add( "data", core.getDirectoryFactory().normalize(core.getDataDir()));
    } catch (IOException e) {
      log.warn("Problem getting the normalized data directory path", e);
      dirs.add( "data", "N/A" );
    }
    dirs.add( "dirimpl", core.getDirectoryFactory().getClass().getName());
    try {
      dirs.add( "index", core.getDirectoryFactory().normalize(core.getIndexDir()) );
    } catch (IOException e) {
      log.warn("Problem getting the normalized index directory path", e);
      dirs.add( "index", "N/A" );
    }
    info.add( "directory", dirs );
    return info;
  }
  
  /**
   * Get system info
   */
  public static SimpleOrderedMap<Object> getSystemInfo() {
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();
    
    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    info.add(NAME, os.getName());
    info.add( "version", os.getVersion() );
    info.add( "arch", os.getArch() );
    info.add( "systemLoadAverage", os.getSystemLoadAverage());

    // This is a public Oracle/OpenJDK extension, but may not be in other JDKs:
    // com.sun.management.OperatingSystemMXBean
    try {
      final Class<? extends PlatformManagedObject> intf = Class.forName("com.sun.management.OperatingSystemMXBean")
          .asSubclass(PlatformManagedObject.class);
      final PlatformManagedObject bean = ManagementFactory.getPlatformMXBean(intf);
      if (bean != null) {
        addMXBeanProperty( bean, intf, "committedVirtualMemorySize", info);
        addMXBeanProperty( bean, intf, "freePhysicalMemorySize", info);
        addMXBeanProperty( bean, intf, "freeSwapSpaceSize", info);
        addMXBeanProperty( bean, intf, "processCpuTime", info);
        addMXBeanProperty( bean, intf, "totalPhysicalMemorySize", info);
        addMXBeanProperty( bean, intf, "totalSwapSpaceSize", info);
      }
    } catch (Exception e) {
      // ignore
    }

    // This is a public Oracle/OpenJDK extension, but may not be in other JDKs:
    // com.sun.management.UnixOperatingSystemMXBean
    try {
      final Class<? extends PlatformManagedObject> intf = Class.forName("com.sun.management.UnixOperatingSystemMXBean")
          .asSubclass(PlatformManagedObject.class);
      final PlatformManagedObject bean = ManagementFactory.getPlatformMXBean(intf);
      if (bean != null) {
        addMXBeanProperty( bean, intf, "openFileDescriptorCount", info );
        addMXBeanProperty( bean, intf, "maxFileDescriptorCount", info );
      }
    } catch (Exception e) {
      // ignore
    }

    try { 
      if( !os.getName().toLowerCase(Locale.ROOT).startsWith( "windows" ) ) {
        // Try some command line things
        info.add( "uname",  execute( "uname -a" ) );
        info.add( "uptime", execute( "uptime" ) );
      }
    } catch( Exception ex ) {
      log.warn("Unable to execute command line tools.", ex);
    } 
    return info;
  }
  
  /**
   * Try to run a getter function on a {@link PlatformManagedObject}.
   * <p>
   * If you are running a OpenJDK/Oracle JVM, there are nice functions in:
   * {@code com.sun.management.UnixOperatingSystemMXBean} and
   * {@code com.sun.management.OperatingSystemMXBean}
   * <p>
   * If the given bean does not have the property, it is handled like {@code null}
   * and not added to the given named list.
   */
  static void addMXBeanProperty(PlatformManagedObject obj, Class<? extends PlatformManagedObject> intf,
      String property, NamedList<Object> info) {
    try {
      final String method = "get" + Character.toUpperCase( property.charAt(0) ) + property.substring( 1 );
      final Object v = intf.getMethod( method ).invoke( intf.cast(obj) );
      if( v != null ) {
        info.add( property, v );
      }
    } catch (Exception e) {
      log.warn("Cannot get property '{}' of MXBean interface: {}", property, intf.getName());
    }
  }
  
  
  /**
   * Utility function to execute a function
   */
  private static String execute( String cmd )
  {
    InputStream in = null;
    Process process = null;
    
    try {
      process = Runtime.getRuntime().exec(cmd);
      in = process.getInputStream();
      // use default charset from locale here, because the command invoked also uses the default locale:
      return IOUtils.toString(new InputStreamReader(in, Charset.defaultCharset()));
    } catch( Exception ex ) {
      // ignore - log.warn("Error executing command", ex);
      return "(error executing: " + cmd + ")";
    } catch (Error err) {
      if (err.getMessage() != null && (err.getMessage().contains("posix_spawn") || err.getMessage().contains("UNIXProcess"))) {
        log.warn("Error forking command due to JVM locale bug (see https://issues.apache.org/jira/browse/SOLR-6387): " + err.getMessage());
        return "(error executing: " + cmd + ")";
      }
      throw err;
    } finally {
      if (process != null) {
        IOUtils.closeQuietly( process.getOutputStream() );
        IOUtils.closeQuietly( process.getInputStream() );
        IOUtils.closeQuietly( process.getErrorStream() );
      }
    }
  }
  
  /**
   * Get JVM Info - including memory info
   */
  public static SimpleOrderedMap<Object> getJvmInfo()
  {
    SimpleOrderedMap<Object> jvm = new SimpleOrderedMap<>();

    final String javaVersion = System.getProperty("java.specification.version", "unknown"); 
    final String javaVendor = System.getProperty("java.specification.vendor", "unknown"); 
    final String javaName = System.getProperty("java.specification.name", "unknown"); 
    final String jreVersion = System.getProperty("java.version", "unknown");
    final String jreVendor = System.getProperty("java.vendor", "unknown");
    final String vmVersion = System.getProperty("java.vm.version", "unknown"); 
    final String vmVendor = System.getProperty("java.vm.vendor", "unknown"); 
    final String vmName = System.getProperty("java.vm.name", "unknown"); 

    // Summary Info
    jvm.add( "version", jreVersion + " " + vmVersion);
    jvm.add(NAME, jreVendor + " " + vmName);
    
    // details
    SimpleOrderedMap<Object> java = new SimpleOrderedMap<>();
    java.add( "vendor", javaVendor );
    java.add(NAME, javaName);
    java.add( "version", javaVersion );
    jvm.add( "spec", java );
    SimpleOrderedMap<Object> jre = new SimpleOrderedMap<>();
    jre.add( "vendor", jreVendor );
    jre.add( "version", jreVersion );
    jvm.add( "jre", jre );
    SimpleOrderedMap<Object> vm = new SimpleOrderedMap<>();
    vm.add( "vendor", vmVendor );
    vm.add(NAME, vmName);
    vm.add( "version", vmVersion );
    jvm.add( "vm", vm );
           
    
    Runtime runtime = Runtime.getRuntime();
    jvm.add( "processors", runtime.availableProcessors() );
    
    // not thread safe, but could be thread local
    DecimalFormat df = new DecimalFormat("#.#", DecimalFormatSymbols.getInstance(Locale.ROOT));

    SimpleOrderedMap<Object> mem = new SimpleOrderedMap<>();
    SimpleOrderedMap<Object> raw = new SimpleOrderedMap<>();
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
    SimpleOrderedMap<Object> jmx = new SimpleOrderedMap<>();
    try{
      RuntimeMXBean mx = ManagementFactory.getRuntimeMXBean();
      if (mx.isBootClassPathSupported()) {
        jmx.add( "bootclasspath", mx.getBootClassPath());
      }
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
  
  private static SimpleOrderedMap<Object> getLuceneInfo() {
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();

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




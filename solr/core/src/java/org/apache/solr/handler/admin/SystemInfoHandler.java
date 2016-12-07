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

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.PlatformManagedObject;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.LucenePackage;
import org.apache.lucene.util.Constants;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.RTimer;

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
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * <p>
   * Undocumented expert level system property to prevent doing a reverse lookup of our hostname.
   * This property ill be logged as a suggested workaround if any probems are noticed when doing reverse 
   * lookup.
   * </p>
   *
   * <p>
   * TODO: should we refactor this (and the associated logic) into a helper method for any other places
   * where DNS is used?
   * </p>
   * @see #initHostname
   */
  private static final String PREVENT_REVERSE_DNS_OF_LOCALHOST_SYSPROP = "solr.dns.prevent.reverse.lookup";
  
  // on some platforms, resolving canonical hostname can cause the thread
  // to block for several seconds if nameservices aren't available
  // so resolve this once per handler instance 
  //(ie: not static, so core reload will refresh)
  private String hostname = null;

  private CoreContainer cc;

  public SystemInfoHandler() {
    this(null);
  }

  public SystemInfoHandler(CoreContainer cc) {
    super();
    this.cc = cc;
    initHostname();
  }
  
  private void initHostname() {
    if (null != System.getProperty(PREVENT_REVERSE_DNS_OF_LOCALHOST_SYSPROP, null)) {
      log.info("Resolving canonical hostname for local host prevented due to '{}' sysprop",
               PREVENT_REVERSE_DNS_OF_LOCALHOST_SYSPROP);
      hostname = null;
      return;
    }
    
    RTimer timer = new RTimer();
    try {
      InetAddress addr = InetAddress.getLocalHost();
      hostname = addr.getCanonicalHostName();
    } catch (Exception e) {
      log.warn("Unable to resolve canonical hostname for local host, possible DNS misconfiguration. " +
               "Set the '"+PREVENT_REVERSE_DNS_OF_LOCALHOST_SYSPROP+"' sysprop to true on startup to " +
               "prevent future lookups if DNS can not be fixed.", e);
      hostname = null;
      return;
    }
    timer.stop();
    
    if (15000D < timer.getTime()) {
      String readableTime = String.format(Locale.ROOT, "%.3f", (timer.getTime() / 1000));
      log.warn("Resolving canonical hostname for local host took {} seconds, possible DNS misconfiguration. " +
               "Set the '{}' sysprop to true on startup to prevent future lookups if DNS can not be fixed.",
               readableTime, PREVENT_REVERSE_DNS_OF_LOCALHOST_SYSPROP);
    
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
    dirs.add("instance", core.getResourceLoader().getInstancePath().toString());
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
    info.add(NAME, os.getName()); // add at least this one
    try {
      // add remaining ones dynamically using Java Beans API
      addMXBeanProperties(os, OperatingSystemMXBean.class, info);
    } catch (IntrospectionException | ReflectiveOperationException e) {
      log.warn("Unable to fetch properties of OperatingSystemMXBean.", e);
    }

    // There are some additional beans we want to add (not available on all JVMs):
    for (String clazz : Arrays.asList(
        "com.sun.management.OperatingSystemMXBean",
        "com.sun.management.UnixOperatingSystemMXBean", 
        "com.ibm.lang.management.OperatingSystemMXBean"
    )) {
      try {
        final Class<? extends PlatformManagedObject> intf = Class.forName(clazz)
            .asSubclass(PlatformManagedObject.class);
        addMXBeanProperties(os, intf, info);
      } catch (ClassNotFoundException e) {
        // ignore
      } catch (IntrospectionException | ReflectiveOperationException e) {
        log.warn("Unable to fetch properties of JVM-specific OperatingSystemMXBean.", e);
      }
    }

    // Try some command line things:
    try { 
      if (!Constants.WINDOWS) {
        info.add( "uname",  execute( "uname -a" ) );
        info.add( "uptime", execute( "uptime" ) );
      }
    } catch( Exception ex ) {
      log.warn("Unable to execute command line tools to get operating system properties.", ex);
    } 
    return info;
  }
  
  /**
   * Add all bean properties of a {@link PlatformManagedObject} to the given {@link NamedList}.
   * <p>
   * If you are running a OpenJDK/Oracle JVM, there are nice properties in:
   * {@code com.sun.management.UnixOperatingSystemMXBean} and
   * {@code com.sun.management.OperatingSystemMXBean}
   */
  static <T extends PlatformManagedObject> void addMXBeanProperties(T obj, Class<? extends T> intf, NamedList<Object> info)
      throws IntrospectionException, ReflectiveOperationException {
    if (intf.isInstance(obj)) {
      final BeanInfo beanInfo = Introspector.getBeanInfo(intf, intf.getSuperclass(), Introspector.IGNORE_ALL_BEANINFO);
      for (final PropertyDescriptor desc : beanInfo.getPropertyDescriptors()) {
        final String name = desc.getName();
        if (info.get(name) == null) {
          try {
            final Object v = desc.getReadMethod().invoke(obj);
            if(v != null) {
              info.add(name, v);
            }
          } catch (InvocationTargetException ite) {
            // ignore (some properties throw UOE)
          }
        }
      }
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




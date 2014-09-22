package org.apache.solr.util;

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

import java.io.PrintStream;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.noggit.CharArr;
import org.noggit.JSONParser;
import org.noggit.JSONWriter;
import org.noggit.ObjectBuilder;

/**
 * Command-line utility for working with Solr.
 */
public class SolrCLI {
   
  /**
   * Defines the interface to a Solr tool that can be run from this command-line app.
   */
  public interface Tool {
    String getName();    
    Option[] getOptions();    
    int runTool(CommandLine cli) throws Exception;
  }
  
  /**
   * Helps build SolrCloud aware tools by initializing a CloudSolrServer
   * instance before running the tool.
   */
  public static abstract class SolrCloudTool implements Tool {
    
    public Option[] getOptions() {
      return cloudOptions;
    }
    
    public int runTool(CommandLine cli) throws Exception {
      
      // quiet down the ZK logging for cli tools
      LogManager.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
      LogManager.getLogger("org.apache.solr.common.cloud").setLevel(Level.WARN);
      
      String zkHost = cli.getOptionValue("zkHost", ZK_HOST);
      
      log.debug("Connecting to Solr cluster: " + zkHost);
      CloudSolrServer cloudSolrServer = null;
      try {
        cloudSolrServer = new CloudSolrServer(zkHost);
        
        String collection = cli.getOptionValue("collection");
        if (collection != null)
          cloudSolrServer.setDefaultCollection(collection);
        
        cloudSolrServer.connect();        
        runCloudTool(cloudSolrServer, cli);
      } finally {
        if (cloudSolrServer != null) {
          try {
            cloudSolrServer.shutdown();
          } catch (Exception ignore) {}
        }
      }
      
      return 0;
    }
    
    /**
     * Runs a SolrCloud tool with CloudSolrServer initialized
     */
    protected abstract void runCloudTool(CloudSolrServer cloudSolrServer, CommandLine cli) 
        throws Exception;
  }
  
  public static Logger log = Logger.getLogger(SolrCLI.class);    
  public static final String DEFAULT_SOLR_URL = "http://localhost:8983/solr";  
  public static final String ZK_HOST = "localhost:9983";
  
  @SuppressWarnings("static-access")
  public static Option[] cloudOptions =  new Option[] {
    OptionBuilder
        .withArgName("HOST")
        .hasArg()
        .isRequired(false)
        .withDescription("Address of the Zookeeper ensemble; defaults to: "+ZK_HOST)
        .create("zkHost"),
    OptionBuilder
        .withArgName("COLLECTION")
        .hasArg()
        .isRequired(false)
        .withDescription("Name of collection; no default")
        .create("collection")
  };      
        
  /**
   * Runs a tool.
   */
  public static void main(String[] args) throws Exception {
    if (args == null || args.length == 0 || args[0] == null || args[0].trim().length() == 0) {
      System.err.println("Invalid command-line args! Must pass the name of a tool to run.\n"
          + "Supported tools:\n");
      displayToolOptions(System.err);
      System.exit(1);
    }
    
    // Determine the tool
    String toolType = args[0].trim().toLowerCase(Locale.ROOT);
    Tool tool = newTool(toolType);
    
    String[] toolArgs = new String[args.length - 1];
    System.arraycopy(args, 1, toolArgs, 0, toolArgs.length);    
    
    // process command-line args to configure this application
    CommandLine cli = 
        processCommandLineArgs(joinCommonAndToolOptions(tool.getOptions()), toolArgs);

    // run the tool
    int exitCode = tool.runTool(cli);
    
    System.exit(exitCode);    
  }
  
  /**
   * Support options common to all tools.
   */
  public static Option[] getCommonToolOptions() {
    return new Option[0];
  }
   
  // Creates an instance of the requested tool, using classpath scanning if necessary
  private static Tool newTool(String toolType) throws Exception {
    if ("healthcheck".equals(toolType))
      return new HealthcheckTool();
    else if ("status".equals(toolType))
      return new StatusTool();
    else if ("api".equals(toolType))
      return new ApiTool();
    
    // If you add a built-in tool to this class, add it here to avoid
    // classpath scanning

    for (Class<Tool> next : findToolClassesInPackage("org.apache.solr.util")) {
      Tool tool = next.newInstance();
      if (toolType.equals(tool.getName()))
        return tool;  
    }
    
    throw new IllegalArgumentException(toolType + " not supported!");
  }
  
  private static void displayToolOptions(PrintStream out) throws Exception {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("healthcheck", getToolOptions(new HealthcheckTool()));
    formatter.printHelp("status", getToolOptions(new StatusTool()));
    formatter.printHelp("api", getToolOptions(new ApiTool()));
    
    List<Class<Tool>> toolClasses = findToolClassesInPackage("org.apache.solr.util");
    for (Class<Tool> next : toolClasses) {
      Tool tool = next.newInstance();
      formatter.printHelp(tool.getName(), getToolOptions(tool));      
    }    
  }
  
  private static Options getToolOptions(Tool tool) {
    Options options = new Options();
    options.addOption("h", "help", false, "Print this message");
    options.addOption("v", "verbose", false, "Generate verbose log messages");
    Option[] toolOpts = joinCommonAndToolOptions(tool.getOptions());
    for (int i = 0; i < toolOpts.length; i++)
      options.addOption(toolOpts[i]);
    return options;
  }
  
  public static Option[] joinCommonAndToolOptions(Option[] toolOpts) {
    return joinOptions(getCommonToolOptions(), toolOpts);
  }
  
  public static Option[] joinOptions(Option[] lhs, Option[] rhs) {
    List<Option> options = new ArrayList<Option>();
    if (lhs != null && lhs.length > 0) {
      for (Option opt : lhs)
        options.add(opt);      
    }
    
    if (rhs != null) {
      for (Option opt : rhs)
        options.add(opt);
    }
    
    return options.toArray(new Option[0]);
  }
  
  
  /**
   * Parses the command-line arguments passed by the user.
   */
  public static CommandLine processCommandLineArgs(Option[] customOptions, String[] args) {
    Options options = new Options();
    
    options.addOption("h", "help", false, "Print this message");
    options.addOption("v", "verbose", false, "Generate verbose log messages");
    
    if (customOptions != null) {
      for (int i = 0; i < customOptions.length; i++)
        options.addOption(customOptions[i]);
    }
    
    CommandLine cli = null;
    try {
      cli = (new GnuParser()).parse(options, args);
    } catch (ParseException exp) {
      boolean hasHelpArg = false;
      if (args != null && args.length > 0) {
        for (int z = 0; z < args.length; z++) {
          if ("-h".equals(args[z]) || "-help".equals(args[z])) {
            hasHelpArg = true;
            break;
          }
        }
      }
      if (!hasHelpArg) {
        System.err.println("Failed to parse command-line arguments due to: "
            + exp.getMessage());
      }
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(SolrCLI.class.getName(), options);
      System.exit(1);
    }
    
    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(SolrCLI.class.getName(), options);
      System.exit(0);
    }
    
    return cli;
  }
  
  /**
   * Scans Jar files on the classpath for Tool implementations to activate.
   */
  @SuppressWarnings("unchecked")
  private static List<Class<Tool>> findToolClassesInPackage(String packageName) {
    List<Class<Tool>> toolClasses = new ArrayList<Class<Tool>>();
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      String path = packageName.replace('.', '/');
      Enumeration<URL> resources = classLoader.getResources(path);
      Set<String> classes = new TreeSet<String>();
      while (resources.hasMoreElements()) {
        URL resource = (URL) resources.nextElement();
        classes.addAll(findClasses(resource.getFile(), packageName));
      }
      
      for (String classInPackage : classes) {
        Class<?> theClass = Class.forName(classInPackage);
        if (Tool.class.isAssignableFrom(theClass)) {
          toolClasses.add((Class<Tool>) theClass);
        }
      }
    } catch (Exception e) {
      // safe to squelch this as it's just looking for tools to run
      //e.printStackTrace();
    }
    return toolClasses;
  }
  
  private static Set<String> findClasses(String path, String packageName)
      throws Exception {
    Set<String> classes = new TreeSet<String>();
    if (path.startsWith("file:") && path.contains("!")) {
      String[] split = path.split("!");
      URL jar = new URL(split[0]);
      ZipInputStream zip = new ZipInputStream(jar.openStream());
      ZipEntry entry;
      while ((entry = zip.getNextEntry()) != null) {
        if (entry.getName().endsWith(".class")) {
          String className = entry.getName().replaceAll("[$].*", "")
              .replaceAll("[.]class", "").replace('/', '.');
          if (className.startsWith(packageName)) {
            classes.add(className);
          }
        }
      }
    }
    return classes;
  }
  
  /**
   * Determine if a request to Solr failed due to a communication error,
   * which is generally retry-able. 
   */
  public static boolean checkCommunicationError(Exception exc) {
    Throwable rootCause = SolrException.getRootCause(exc);
    boolean wasCommError =
        (rootCause instanceof ConnectException ||
            rootCause instanceof ConnectTimeoutException ||
            rootCause instanceof NoHttpResponseException ||
            rootCause instanceof SocketException);
    return wasCommError;
  }
  
  public static HttpClient getHttpClient() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 128);
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 32);
    params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false);
    return HttpClientUtil.createClient(params);    
  }
  
  @SuppressWarnings("deprecation")
  public static void closeHttpClient(HttpClient httpClient) {
    if (httpClient != null) {
      try {
        httpClient.getConnectionManager().shutdown();
      } catch (Exception exc) {
        // safe to ignore, we're just shutting things down
      }
    }    
  }

  /**
   * Useful when a tool just needs to send one request to Solr. 
   */
  public static Map<String,Object> getJson(String getUrl) throws Exception {
    Map<String,Object> json = null;
    HttpClient httpClient = getHttpClient();
    try {
      json = getJson(httpClient, getUrl, 2);
    } finally {
      closeHttpClient(httpClient);
    }
    return json;
  }
  
  /**
   * Utility function for sending HTTP GET request to Solr with built-in retry support.
   */
  public static Map<String,Object> getJson(HttpClient httpClient, String getUrl, int attempts) throws Exception {
    Map<String,Object> json = null;
    if (attempts >= 1) {
      try {
        json = getJson(httpClient, getUrl);
      } catch (Exception exc) {
        if (--attempts > 0 && checkCommunicationError(exc)) {
          log.warn("Request to "+getUrl+" failed due to: "+exc.getMessage()+
              ", sleeping for 5 seconds before re-trying the request ...");
          try {
            Thread.sleep(5000);
          } catch (InterruptedException ie) { Thread.interrupted(); }
          
          // retry using recursion with one-less attempt available
          json = getJson(httpClient, getUrl, attempts);
        } else {
          // no more attempts or error is not retry-able
          throw exc;
        }
      }
    }
    
    return json;
  }
  
  /**
   * Utility function for sending HTTP GET request to Solr and then doing some
   * validation of the response.
   */
  @SuppressWarnings({"unchecked"})
  public static Map<String,Object> getJson(HttpClient httpClient, String getUrl) throws Exception {
    Map<String,Object> json = null;
       
    // ensure we're requesting JSON back from Solr
    HttpGet httpGet = new HttpGet(new URIBuilder(getUrl).setParameter("wt", "json").build());

    //Will throw HttpResponseException if a non-ok response
    String content = httpClient.execute(httpGet, new BasicResponseHandler());
    
    Object resp = ObjectBuilder.getVal(new JSONParser(content));
    if (resp != null && resp instanceof Map) {
      json = (Map<String,Object>)resp;
    } else {
      throw new SolrServerException("Expected JSON object in response from "+
          getUrl+" but received "+ resp);
    }
    
    // lastly check the response JSON from Solr to see if it is an error
    Long statusCode = asLong("/responseHeader/status", json);
    
    if (statusCode == -1) {
      throw new SolrServerException("Unable to determine outcome of GET request to: "+
          getUrl+"! Response: "+json);
    } else if (statusCode != 0) {
      String errMsg = asString("/error/msg", json);
      
      errMsg = errMsg == null ? String.valueOf(json) : errMsg;
      throw new SolrServerException("Request to "+getUrl+" failed due to: "+errMsg);
    }

    return json;
  }  

  /**
   * Helper function for reading a String value from a JSON Object tree. 
   */
  public static String asString(String jsonPath, Map<String,Object> json) {
    return pathAs(String.class, jsonPath, json);
  }

  /**
   * Helper function for reading a Long value from a JSON Object tree. 
   */
  public static Long asLong(String jsonPath, Map<String,Object> json) {
    return pathAs(Long.class, jsonPath, json);
  }
  
  /**
   * Helper function for reading a List of Strings from a JSON Object tree. 
   */
  @SuppressWarnings("unchecked")
  public static List<String> asList(String jsonPath, Map<String,Object> json) {
    return pathAs(List.class, jsonPath, json);
  }
  
  /**
   * Helper function for reading a Map from a JSON Object tree. 
   */
  @SuppressWarnings("unchecked")
  public static Map<String,Object> asMap(String jsonPath, Map<String,Object> json) {
    return pathAs(Map.class, jsonPath, json);
  }
  
  @SuppressWarnings("unchecked")
  public static <T> T pathAs(Class<T> clazz, String jsonPath, Map<String,Object> json) {
    T val = null;
    Object obj = atPath(jsonPath, json);
    if (obj != null) {
      if (clazz.isAssignableFrom(obj.getClass())) {
        val = (T) obj;
      } else {
        // no ok if it's not null and of a different type
        throw new IllegalStateException("Expected a " + clazz.getName() + " at path "+
           jsonPath+" but found "+obj+" instead! "+json);
      }
    } // it's ok if it is null
    return val;
  }
  
  /**
   * Helper function for reading an Object of unknown type from a JSON Object tree. 
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Object atPath(String jsonPath, Map<String,Object> json) {
    if ("/".equals(jsonPath))
      return json;
    
    if (!jsonPath.startsWith("/"))
      throw new IllegalArgumentException("Invalid JSON path: "+
        jsonPath+"! Must start with a /");
    
    Map<String,Object> parent = json;      
    Object result = null;
    String[] path = jsonPath.split("/");
    for (int p=1; p < path.length; p++) {
      Object child = parent.get(path[p]);      
      if (child == null)
        break;
      
      if (p == path.length-1) {
        // success - found the node at the desired path
        result = child;
      } else {
        if (child instanceof Map) {
          // keep walking the path down to the desired node
          parent = (Map)child;
        } else {
          // early termination - hit a leaf before the requested node
          break;
        }
      }
    }
    return result;
  }
  

  /**
   * Get the status of a Solr server.
   */
  public static class StatusTool implements Tool {

    @Override
    public String getName() {
      return "status";
    }
    
    @SuppressWarnings("static-access")
    @Override
    public Option[] getOptions() {
      return new Option[] {
        OptionBuilder
            .withArgName("URL")
            .hasArg()
            .isRequired(false)
            .withDescription("Address of the Solr Web application, defaults to: "+DEFAULT_SOLR_URL)
            .create("solr")
      };      
    }    

    @Override
    public int runTool(CommandLine cli) throws Exception {
      String solrUrl = cli.getOptionValue("solr", DEFAULT_SOLR_URL);
      if (!solrUrl.endsWith("/"))
        solrUrl += "/";
      
      int exitCode = 0;
      String systemInfoUrl = solrUrl+"admin/info/system";
      HttpClient httpClient = getHttpClient();
      try {        
        // hit Solr to get system info
        Map<String,Object> systemInfo = getJson(httpClient, systemInfoUrl, 2);
        
        // convert raw JSON into user-friendly output
        Map<String,Object> status = 
            reportStatus(solrUrl, systemInfo, httpClient);
        
        // pretty-print the status to stdout
        CharArr arr = new CharArr();
        new JSONWriter(arr, 2).write(status);
        System.out.println(arr.toString());
        
      } catch (Exception exc) {
        if (checkCommunicationError(exc)) {
          // this is not actually an error from the tool as it's ok if Solr is not online.
          System.err.println("Solr at "+solrUrl+" not online.");
        } else {
          System.err.print("Failed to get system information from "+solrUrl+" due to: ");
          exc.printStackTrace(System.err);          
          exitCode = 1;
        }
      } finally {
        closeHttpClient(httpClient);        
      }
            
      return exitCode;
    }    
    
    protected Map<String,Object> reportStatus(String solrUrl, Map<String,Object> info, HttpClient httpClient) 
        throws Exception
    {
      Map<String,Object> status = new LinkedHashMap<String,Object>();
      
      status.put("version", asString("/lucene/solr-impl-version", info));      
      status.put("startTime", asString("/jvm/jmx/startTime", info));
      status.put("uptime", uptime(asLong("/jvm/jmx/upTimeMS", info)));
      
      String usedMemory = asString("/jvm/memory/used", info);
      String totalMemory = asString("/jvm/memory/total", info);
      status.put("memory", usedMemory+" of "+totalMemory);
      
      // if this is a Solr in solrcloud mode, gather some basic cluster info
      if ("solrcloud".equals(info.get("mode"))) {
        
        // TODO: Need a better way to get the zkHost from a running server
        // as it can be set from solr.xml vs. on the command-line
        String zkHost = null;
        List<String> args = asList("/jvm/jmx/commandLineArgs", info);
        if (args != null) {
          for (String arg : args) {
            if (arg.startsWith("-DzkHost=")) {
              zkHost = arg.substring("-DzkHost=".length());
              break;
            } else if (arg.startsWith("-DzkRun")) {
              URL serverUrl = new URL(solrUrl);
              String host = serverUrl.getHost();
              int port = serverUrl.getPort();
              zkHost = host+":"+(port+1000)+" (embedded)";
              break;
            }
          }
        }
        
        status.put("cloud", getCloudStatus(httpClient, solrUrl, zkHost));
      }
      
      return status;
    }
    
    /**
     * Calls the CLUSTERSTATUS endpoint in Solr to get basic status information about
     * the SolrCloud cluster. 
     */
    protected Map<String,String> getCloudStatus(HttpClient httpClient, String solrUrl, String zkHost) 
        throws Exception
    {
      Map<String,String> cloudStatus = new LinkedHashMap<String,String>();      
      cloudStatus.put("ZooKeeper", (zkHost != null) ? zkHost : "?");      
      
      String clusterStatusUrl = solrUrl+"admin/collections?action=CLUSTERSTATUS";
      Map<String,Object> json = getJson(httpClient, clusterStatusUrl, 2);
      
      List<String> liveNodes = asList("/cluster/live_nodes", json); 
      cloudStatus.put("liveNodes", String.valueOf(liveNodes.size()));
      
      Map<String,Object> collections = asMap("/cluster/collections", json);
      cloudStatus.put("collections", String.valueOf(collections.size()));
      
      return cloudStatus;      
    }
        
  } // end StatusTool class
  
  /**
   * Used to send an arbitrary HTTP request to a Solr API endpoint.
   */
  public static class ApiTool implements Tool {

    @Override
    public String getName() {
      return "api";
    }
    
    @SuppressWarnings("static-access")
    @Override
    public Option[] getOptions() {
      return new Option[] {
        OptionBuilder
            .withArgName("URL")
            .hasArg()
            .isRequired(false)
            .withDescription("Send a GET request to a Solr API endpoint")
            .create("get")
      };      
    }    

    @Override
    public int runTool(CommandLine cli) throws Exception {
      String getUrl = cli.getOptionValue("get");
      if (getUrl != null) {
        Map<String,Object> json = getJson(getUrl);
        
        // pretty-print the response to stdout
        CharArr arr = new CharArr();
        new JSONWriter(arr, 2).write(json);
        System.out.println(arr.toString());        
      }
      
      return 0;
    }    
  } // end ApiTool class

  private static final long MS_IN_MIN = 60 * 1000L;
  private static final long MS_IN_HOUR = MS_IN_MIN * 60L;
  private static final long MS_IN_DAY = MS_IN_HOUR * 24L;
  
  private static final String uptime(long uptimeMs) {
    if (uptimeMs <= 0L) return "?";
    
    long numDays = (uptimeMs >= MS_IN_DAY) 
        ? (long) Math.floor(uptimeMs / MS_IN_DAY) : 0L;
    long rem = uptimeMs - (numDays * MS_IN_DAY);
    long numHours = (rem >= MS_IN_HOUR) 
        ? (long) Math.floor(rem / MS_IN_HOUR) : 0L;
    rem = rem - (numHours * MS_IN_HOUR);
    long numMinutes = (rem >= MS_IN_MIN) 
        ? (long) Math.floor(rem / MS_IN_MIN) : 0L;
    rem = rem - (numMinutes * MS_IN_MIN);
    long numSeconds = Math.round(rem / 1000);
    return String.format(Locale.ROOT, "%d days, %d hours, %d minutes, %d seconds", numDays,
        numHours, numMinutes, numSeconds);
  }
    
  static class ReplicaHealth implements Comparable<ReplicaHealth> {
    String shard;
    String name;
    String url;
    String status;
    long numDocs;
    boolean isLeader;
    String uptime;
    String memory;
        
    ReplicaHealth(String shard, String name, String url, String status,
        long numDocs, boolean isLeader, String uptime, String memory) {
      this.shard = shard;
      this.name = name;
      this.url = url;
      this.numDocs = numDocs;
      this.status = status;
      this.isLeader = isLeader;
      this.uptime = uptime;
      this.memory = memory;
    }
    
    public Map<String,Object> asMap() {
      Map<String,Object> map = new LinkedHashMap<String,Object>();
      map.put("name", name);
      map.put("url", url);
      map.put("numDocs", numDocs);
      map.put("status", status);
      if (uptime != null)
        map.put("uptime", uptime);
      if (memory != null)
        map.put("memory", memory);
      if (isLeader)
        map.put("leader", true);
      return map;
    }    
    
    public String toString() {      
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(asMap());
      return arr.toString();             
    }
    
    public int hashCode() {
      return this.shard.hashCode() + (isLeader ? 1 : 0);
    }
    
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (!(obj instanceof ReplicaHealth)) return true;
      ReplicaHealth that = (ReplicaHealth) obj;
      return this.shard.equals(that.shard) && this.isLeader == that.isLeader;
    }
    
    public int compareTo(ReplicaHealth other) {
      if (this == other) return 0;
      if (other == null) return 1;
      
      int myShardIndex = 
          Integer.parseInt(this.shard.substring("shard".length()));
      
      int otherShardIndex = 
          Integer.parseInt(other.shard.substring("shard".length()));
      
      if (myShardIndex == otherShardIndex) {
        // same shard index, list leaders first
        return this.isLeader ? -1 : 1;
      }
      
      return myShardIndex - otherShardIndex;
    }
  }
  
  static enum ShardState {
    healthy, degraded, down, no_leader
  }
  
  static class ShardHealth {
    String shard;
    List<ReplicaHealth> replicas;
    
    ShardHealth(String shard, List<ReplicaHealth> replicas) {
      this.shard = shard;
      this.replicas = replicas;      
    }
    
    public ShardState getShardState() {
      boolean healthy = true;
      boolean hasLeader = false;
      boolean atLeastOneActive = false;
      for (ReplicaHealth replicaHealth : replicas) {
        if (replicaHealth.isLeader) 
          hasLeader = true;
        
        if (!"active".equals(replicaHealth.status)) {
          healthy = false;
        } else {
          atLeastOneActive = true;
        }
      }
      
      if (!hasLeader)
        return ShardState.no_leader;
      
      return healthy ? ShardState.healthy : (atLeastOneActive ? ShardState.degraded : ShardState.down);
    }
    
    public Map<String,Object> asMap() {
      Map<String,Object> map = new LinkedHashMap<>();
      map.put("shard", shard);
      map.put("status", getShardState().toString());
      List<Object> replicaList = new ArrayList<Object>();
      for (ReplicaHealth replica : replicas)
        replicaList.add(replica.asMap());
      map.put("replicas", replicaList);
      return map;
    }
        
    public String toString() {
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(asMap());
      return arr.toString();             
    }    
  }
  
  /**
   * Requests health information about a specific collection in SolrCloud.
   */
  public static class HealthcheckTool extends SolrCloudTool {
        
    @Override
    public String getName() {
      return "healthcheck";
    }
        
    @Override
    protected void runCloudTool(CloudSolrServer cloudSolrServer, CommandLine cli) throws Exception {
      
      String collection = cli.getOptionValue("collection");
      if (collection == null)
        throw new IllegalArgumentException("Must provide a collection to run a healthcheck against!");
      
      log.info("Running healthcheck for "+collection);
      
      ZkStateReader zkStateReader = cloudSolrServer.getZkStateReader();

      ClusterState clusterState = zkStateReader.getClusterState();
      Set<String> liveNodes = clusterState.getLiveNodes();
      Collection<Slice> slices = clusterState.getSlices(collection);
      if (slices == null)
        throw new IllegalArgumentException("Collection "+collection+" not found!");
      
      SolrQuery q = new SolrQuery("*:*");
      q.setRows(0);      
      QueryResponse qr = cloudSolrServer.query(q);
      String collErr = null;
      long docCount = -1;
      try {
        docCount = qr.getResults().getNumFound();
      } catch (Exception exc) {
        collErr = String.valueOf(exc);
      }
      
      List<Object> shardList = new ArrayList<>();
      boolean collectionIsHealthy = (docCount != -1);
      
      for (Slice slice : slices) {
        String shardName = slice.getName();
        // since we're reporting health of this shard, there's no guarantee of a leader
        String leaderUrl = null;
        try {
          leaderUrl = zkStateReader.getLeaderUrl(collection, shardName, 1000);
        } catch (Exception exc) {
          log.warn("Failed to get leader for shard "+shardName+" due to: "+exc);
        }
        
        List<ReplicaHealth> replicaList = new ArrayList<ReplicaHealth>();        
        for (Replica r : slice.getReplicas()) {
          
          String uptime = null;
          String memory = null;
          String replicaStatus = null;
          long numDocs = -1L;
          
          ZkCoreNodeProps replicaCoreProps = new ZkCoreNodeProps(r);
          String coreUrl = replicaCoreProps.getCoreUrl();
          boolean isLeader = coreUrl.equals(leaderUrl);

          // if replica's node is not live, it's status is DOWN
          String nodeName = replicaCoreProps.getNodeName();
          if (nodeName == null || !liveNodes.contains(nodeName)) {
            replicaStatus = ZkStateReader.DOWN;
          } else {
            // query this replica directly to get doc count and assess health
            HttpSolrServer solr = new HttpSolrServer(coreUrl);
            String solrUrl = solr.getBaseURL();
            q = new SolrQuery("*:*");
            q.setRows(0);
            q.set("distrib", "false");
            try {
              qr = solr.query(q);
              numDocs = qr.getResults().getNumFound();

              int lastSlash = solrUrl.lastIndexOf('/');
              String systemInfoUrl = solrUrl.substring(0,lastSlash)+"/admin/info/system";
              Map<String,Object> info = getJson(solr.getHttpClient(), systemInfoUrl, 2);
              uptime = uptime(asLong("/jvm/jmx/upTimeMS", info));
              String usedMemory = asString("/jvm/memory/used", info);
              String totalMemory = asString("/jvm/memory/total", info);
              memory = usedMemory+" of "+totalMemory;

              // if we get here, we can trust the state
              replicaStatus = replicaCoreProps.getState();
            } catch (Exception exc) {
              log.error("ERROR: " + exc + " when trying to reach: " + solrUrl);

              if (checkCommunicationError(exc)) {
                replicaStatus = "down";
              } else {
                replicaStatus = "error: "+exc;
              }
            } finally {
              solr.shutdown();
            }
          }

          replicaList.add(new ReplicaHealth(shardName, r.getName(), coreUrl, 
              replicaStatus, numDocs, isLeader, uptime, memory));          
        }
        
        ShardHealth shardHealth = new ShardHealth(shardName, replicaList);        
        if (ShardState.healthy != shardHealth.getShardState())
          collectionIsHealthy = false; // at least one shard is un-healthy
        
        shardList.add(shardHealth.asMap());        
      }
      
      
      Map<String,Object> report = new LinkedHashMap<String,Object>();
      report.put("collection", collection);
      report.put("status", collectionIsHealthy ? "healthy" : "degraded");
      if (collErr != null) {
        report.put("error", collErr);
      }
      report.put("numDocs", docCount);
      report.put("numShards", slices.size());      
      report.put("shards", shardList);
                        
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(report);
      System.out.println(arr.toString());
    }
  } // end HealthcheckTool  
}

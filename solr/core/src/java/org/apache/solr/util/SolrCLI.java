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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.noggit.CharArr;
import org.noggit.JSONParser;
import org.noggit.JSONWriter;
import org.noggit.ObjectBuilder;

import static org.apache.solr.common.params.CommonParams.NAME;

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
   * Helps build SolrCloud aware tools by initializing a CloudSolrClient
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
      int exitStatus = 0;
      try (CloudSolrClient cloudSolrClient = new CloudSolrClient(zkHost)) {

        String collection = cli.getOptionValue("collection");
        if (collection != null)
          cloudSolrClient.setDefaultCollection(collection);
        
        cloudSolrClient.connect();
        exitStatus = runCloudTool(cloudSolrClient, cli);
      } catch (Exception exc) {
        // since this is a CLI, spare the user the stacktrace
        String excMsg = exc.getMessage();
        if (excMsg != null) {
          System.err.println("\nERROR: "+excMsg+"\n");
          exitStatus = 1;
        } else {
          throw exc;
        }
      }
      
      return exitStatus;
    }
    
    /**
     * Runs a SolrCloud tool with CloudSolrServer initialized
     */
    protected abstract int runCloudTool(CloudSolrClient cloudSolrClient, CommandLine cli)
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

    // for SSL support, try to accommodate relative paths set for SSL store props
    String solrInstallDir = System.getProperty("solr.install.dir");
    if (solrInstallDir != null) {
      checkSslStoreSysProp(solrInstallDir, "keyStore");
      checkSslStoreSysProp(solrInstallDir, "trustStore");
    }

    // run the tool
    System.exit(tool.runTool(cli));
  }

  protected static void checkSslStoreSysProp(String solrInstallDir, String key) {
    String sysProp = "javax.net.ssl."+key;
    String keyStore = System.getProperty(sysProp);
    if (keyStore == null)
      return;

    File keyStoreFile = new File(keyStore);
    if (keyStoreFile.isFile())
      return; // configured setting is OK

    keyStoreFile = new File(solrInstallDir, "server/"+keyStore);
    if (keyStoreFile.isFile()) {
      System.setProperty(sysProp, keyStoreFile.getAbsolutePath());
    } else {
      System.err.println("WARNING: "+sysProp+" file "+keyStore+
          " not found! https requests to Solr will likely fail; please update your "+
          sysProp+" setting to use an absolute path.");
    }
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
    else if ("create_collection".equals(toolType))
      return new CreateCollectionTool();
    else if ("create_core".equals(toolType))
      return new CreateCoreTool();
    else if ("create".equals(toolType))
      return new CreateTool();
    else if ("delete".equals(toolType))
      return new DeleteTool();

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
    formatter.printHelp("create_collection", getToolOptions(new CreateCollectionTool()));
    formatter.printHelp("create_core", getToolOptions(new CreateCoreTool()));
    formatter.printHelp("create", getToolOptions(new CreateTool()));
    formatter.printHelp("delete", getToolOptions(new DeleteTool()));

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
        if (Tool.class.isAssignableFrom(theClass))
          toolClasses.add((Class<Tool>) theClass);
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
          if (className.startsWith(packageName))
            classes.add(className);
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
  
  public static CloseableHttpClient getHttpClient() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 128);
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 32);
    params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false);
    return HttpClientUtil.createClient(params);    
  }
  
  @SuppressWarnings("deprecation")
  public static void closeHttpClient(CloseableHttpClient httpClient) {
    if (httpClient != null) {
      try {
        HttpClientUtil.close(httpClient);
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
    CloseableHttpClient httpClient = getHttpClient();
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

  private static class SolrResponseHandler implements ResponseHandler<Map<String,Object>> {
    public Map<String,Object> handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
      HttpEntity entity = response.getEntity();
      if (entity != null) {

        String respBody = EntityUtils.toString(entity);
        Object resp = null;
        try {
          resp = ObjectBuilder.getVal(new JSONParser(respBody));
        } catch (JSONParser.ParseException pe) {
          throw new ClientProtocolException("Expected JSON response from server but received: "+respBody+
              "\nTypically, this indicates a problem with the Solr server; check the Solr server logs for more information.");
        }

        if (resp != null && resp instanceof Map) {
          return (Map<String,Object>)resp;
        } else {
          throw new ClientProtocolException("Expected JSON object in response but received "+ resp);
        }
      } else {
        StatusLine statusLine = response.getStatusLine();
        throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
      }
    }
  }
  
  /**
   * Utility function for sending HTTP GET request to Solr and then doing some
   * validation of the response.
   */
  @SuppressWarnings({"unchecked"})
  public static Map<String,Object> getJson(HttpClient httpClient, String getUrl) throws Exception {
    // ensure we're requesting JSON back from Solr
    HttpGet httpGet = new HttpGet(new URIBuilder(getUrl).setParameter(CommonParams.WT, CommonParams.JSON).build());
    // make the request and get back a parsed JSON object
    Map<String,Object> json = httpClient.execute(httpGet, new SolrResponseHandler());
    // check the response JSON from Solr to see if it is an error
    Long statusCode = asLong("/responseHeader/status", json);
    if (statusCode == -1) {
      throw new SolrServerException("Unable to determine outcome of GET request to: "+
          getUrl+"! Response: "+json);
    } else if (statusCode != 0) {
      String errMsg = asString("/error/msg", json);
      if (errMsg == null)
        errMsg = String.valueOf(json);
      throw new SolrServerException(errMsg);
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
      CloseableHttpClient httpClient = getHttpClient();
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
    
    public Map<String,Object> reportStatus(String solrUrl, Map<String,Object> info, HttpClient httpClient)
        throws Exception
    {
      Map<String,Object> status = new LinkedHashMap<String,Object>();

      String solrHome = (String)info.get("solr_home");
      status.put("solr_home", solrHome != null ? solrHome : "?");
      status.put("version", asString("/lucene/solr-impl-version", info));      
      status.put("startTime", asString("/jvm/jmx/startTime", info));
      status.put("uptime", uptime(asLong("/jvm/jmx/upTimeMS", info)));
      
      String usedMemory = asString("/jvm/memory/used", info);
      String totalMemory = asString("/jvm/memory/total", info);
      status.put("memory", usedMemory+" of "+totalMemory);
      
      // if this is a Solr in solrcloud mode, gather some basic cluster info
      if ("solrcloud".equals(info.get("mode"))) {
        String zkHost = (String)info.get("zkHost");
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

  private static final String DEFAULT_CONFIG_SET = "data_driven_schema_configs";

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
      map.put(NAME, name);
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
        
        if (!Replica.State.ACTIVE.toString().equals(replicaHealth.status)) {
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
    protected int runCloudTool(CloudSolrClient cloudSolrClient, CommandLine cli) throws Exception {
      
      String collection = cli.getOptionValue("collection");
      if (collection == null)
        throw new IllegalArgumentException("Must provide a collection to run a healthcheck against!");
      
      log.debug("Running healthcheck for "+collection);
      
      ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();

      ClusterState clusterState = zkStateReader.getClusterState();
      Set<String> liveNodes = clusterState.getLiveNodes();
      Collection<Slice> slices = clusterState.getSlices(collection);
      if (slices == null)
        throw new IllegalArgumentException("Collection "+collection+" not found!");
      
      SolrQuery q = new SolrQuery("*:*");
      q.setRows(0);      
      QueryResponse qr = cloudSolrClient.query(q);
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

          // if replica's node is not live, its status is DOWN
          String nodeName = replicaCoreProps.getNodeName();
          if (nodeName == null || !liveNodes.contains(nodeName)) {
            replicaStatus = Replica.State.DOWN.toString();
          } else {
            // query this replica directly to get doc count and assess health
            q = new SolrQuery("*:*");
            q.setRows(0);
            q.set("distrib", "false");
            try (HttpSolrClient solr = new HttpSolrClient(coreUrl)) {

              String solrUrl = solr.getBaseURL();

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
              log.error("ERROR: " + exc + " when trying to reach: " + coreUrl);

              if (checkCommunicationError(exc)) {
                replicaStatus = Replica.State.DOWN.toString();
              } else {
                replicaStatus = "error: "+exc;
              }
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

      return 0;
    }
  } // end HealthcheckTool

  private static final Option[] CREATE_COLLECTION_OPTIONS = new Option[] {
    OptionBuilder
        .withArgName("HOST")
        .hasArg()
        .isRequired(false)
        .withDescription("Address of the Zookeeper ensemble; defaults to: "+ZK_HOST)
        .create("zkHost"),
        OptionBuilder
            .withArgName("HOST")
            .hasArg()
            .isRequired(false)
            .withDescription("Base Solr URL, which can be used to determine the zkHost if that's not known")
            .create("solrUrl"),
        OptionBuilder
            .withArgName("NAME")
            .hasArg()
            .isRequired(true)
            .withDescription("Name of collection to create.")
            .create(NAME),
        OptionBuilder
            .withArgName("#")
            .hasArg()
            .isRequired(false)
            .withDescription("Number of shards; default is 1")
            .create("shards"),
        OptionBuilder
            .withArgName("#")
            .hasArg()
            .isRequired(false)
            .withDescription("Number of copies of each document across the collection (replicas per shard); default is 1")
            .create("replicationFactor"),
        OptionBuilder
            .withArgName("#")
            .hasArg()
            .isRequired(false)
            .withDescription("Maximum number of shards per Solr node; default is determined based on the number of shards, replication factor, and live nodes.")
            .create("maxShardsPerNode"),
        OptionBuilder
            .withArgName("NAME")
            .hasArg()
            .isRequired(false)
            .withDescription("Configuration directory to copy when creating the new collection; default is "+DEFAULT_CONFIG_SET)
            .create("confdir"),
        OptionBuilder
            .withArgName("NAME")
            .hasArg()
            .isRequired(false)
            .withDescription("Configuration name; default is the collection name")
            .create("confname"),
        OptionBuilder
            .withArgName("DIR")
            .hasArg()
            .isRequired(true)
            .withDescription("Path to configsets directory on the local system.")
            .create("configsetsDir")
  };

  public static String getZkHost(CommandLine cli) throws Exception {
    String zkHost = cli.getOptionValue("zkHost");
    if (zkHost != null)
      return zkHost;

    // find it using the localPort
    String solrUrl = cli.getOptionValue("solrUrl");
    if (solrUrl == null)
      throw new IllegalStateException(
          "Must provide either the -zkHost or -solrUrl parameters to use the create_collection command!");

    if (!solrUrl.endsWith("/"))
      solrUrl += "/";

    String systemInfoUrl = solrUrl+"admin/info/system";
    CloseableHttpClient httpClient = getHttpClient();
    try {
      // hit Solr to get system info
      Map<String,Object> systemInfo = getJson(httpClient, systemInfoUrl, 2);

      // convert raw JSON into user-friendly output
      StatusTool statusTool = new StatusTool();
      Map<String,Object> status = statusTool.reportStatus(solrUrl, systemInfo, httpClient);
      Map<String,Object> cloud = (Map<String, Object>)status.get("cloud");
      if (cloud != null) {
        String zookeeper = (String) cloud.get("ZooKeeper");
        if (zookeeper.endsWith("(embedded)")) {
          zookeeper = zookeeper.substring(0, zookeeper.length() - "(embedded)".length());
        }
        zkHost = zookeeper;
      }
    } finally {
      HttpClientUtil.close(httpClient);
    }

    return zkHost;
  }

  /**
   * Supports create_collection command in the bin/solr script.
   */
  public static class CreateCollectionTool implements Tool {

    @Override
    public String getName() {
      return "create_collection";
    }

    @SuppressWarnings("static-access")
    @Override
    public Option[] getOptions() {
      return CREATE_COLLECTION_OPTIONS;
    }

    public int runTool(CommandLine cli) throws Exception {

      // quiet down the ZK logging for cli tools
      LogManager.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
      LogManager.getLogger("org.apache.solr.common.cloud").setLevel(Level.WARN);

      String zkHost = getZkHost(cli);
      if (zkHost == null) {
        System.err.println("\nERROR: Solr at "+cli.getOptionValue("solrUrl")+
            " is running in standalone server mode, please use the create_core command instead;\n" +
            "create_collection can only be used when running in SolrCloud mode.\n");
        return 1;
      }

      int toolExitStatus = 0;

      try (CloudSolrClient cloudSolrServer = new CloudSolrClient(zkHost)) {
        System.out.println("Connecting to ZooKeeper at " + zkHost);
        cloudSolrServer.connect();
        toolExitStatus = runCloudTool(cloudSolrServer, cli);
      } catch (Exception exc) {
        // since this is a CLI, spare the user the stacktrace
        String excMsg = exc.getMessage();
        if (excMsg != null) {
          System.err.println("\nERROR: "+excMsg+"\n");
          toolExitStatus = 1;
        } else {
          throw exc;
        }
      }

      return toolExitStatus;
    }

    protected int runCloudTool(CloudSolrClient cloudSolrClient, CommandLine cli) throws Exception {
      Set<String> liveNodes = cloudSolrClient.getZkStateReader().getClusterState().getLiveNodes();
      if (liveNodes.isEmpty())
        throw new IllegalStateException("No live nodes found! Cannot create a collection until " +
            "there is at least 1 live node in the cluster.");
      String firstLiveNode = liveNodes.iterator().next();

      String collectionName = cli.getOptionValue(NAME);

      // build a URL to create the collection
      int numShards = optionAsInt(cli, "shards", 1);
      int replicationFactor = optionAsInt(cli, "replicationFactor", 1);
      int maxShardsPerNode = -1;

      if (cli.hasOption("maxShardsPerNode")) {
        maxShardsPerNode = Integer.parseInt(cli.getOptionValue("maxShardsPerNode"));
      } else {
        // need number of live nodes to determine maxShardsPerNode if it is not set
        int numNodes = liveNodes.size();
        maxShardsPerNode = ((numShards*replicationFactor)+numNodes-1)/numNodes;
      }

      String confname = cli.getOptionValue("confname", collectionName);
      boolean configExistsInZk =
          cloudSolrClient.getZkStateReader().getZkClient().exists("/configs/"+confname, true);

      if (configExistsInZk) {
        System.out.println("Re-using existing configuration directory "+confname);
      } else {
        String configSet = cli.getOptionValue("confdir", DEFAULT_CONFIG_SET);
        File configSetDir = null;
        // we try to be flexible and allow the user to specify a configuration directory instead of a configset name
        File possibleConfigDir = new File(configSet);
        if (possibleConfigDir.isDirectory()) {
          configSetDir = possibleConfigDir;
        } else {
          File configsetsDir = new File(cli.getOptionValue("configsetsDir"));
          if (!configsetsDir.isDirectory())
            throw new FileNotFoundException(configsetsDir.getAbsolutePath()+" not found!");

          // upload the configset if it exists
          configSetDir = new File(configsetsDir, configSet);
          if (!configSetDir.isDirectory()) {
            throw new FileNotFoundException("Specified config " + configSet +
                " not found in " + configsetsDir.getAbsolutePath());
          }
        }

        File confDir = new File(configSetDir, "conf");
        if (!confDir.isDirectory()) {
          // config dir should contain a conf sub-directory but if not and there's a solrconfig.xml, then use it
          if ((new File(configSetDir, "solrconfig.xml")).isFile()) {
            confDir = configSetDir;
          } else {
            System.err.println("Specified configuration directory "+configSetDir.getAbsolutePath()+
                " is invalid;\nit should contain either conf sub-directory or solrconfig.xml");
            return 1;
          }
        }

        // test to see if that config exists in ZK
        System.out.println("Uploading "+confDir.getAbsolutePath()+
            " for config "+confname+" to ZooKeeper at "+cloudSolrClient.getZkHost());
        cloudSolrClient.uploadConfig(confDir.toPath(), confname);
      }

      String baseUrl = cloudSolrClient.getZkStateReader().getBaseUrlForNodeName(firstLiveNode);

      // since creating a collection is a heavy-weight operation, check for existence first
      String collectionListUrl = baseUrl+"/admin/collections?action=list";
      if (safeCheckCollectionExists(collectionListUrl, collectionName)) {
        System.err.println("\nCollection '"+collectionName+"' already exists!");
        System.err.println("\nChecked collection existence using Collections API command:\n"+collectionListUrl);
        System.err.println();
        return 1;
      }

      // doesn't seem to exist ... try to create
      String createCollectionUrl =
          String.format(Locale.ROOT,
              "%s/admin/collections?action=CREATE&name=%s&numShards=%d&replicationFactor=%d&maxShardsPerNode=%d&collection.configName=%s",
              baseUrl,
              collectionName,
              numShards,
              replicationFactor,
              maxShardsPerNode,
              confname);

      System.out.println("\nCreating new collection '"+collectionName+"' using command:\n"+createCollectionUrl+"\n");

      Map<String,Object> json = null;
      try {
        json = getJson(createCollectionUrl);
      } catch (SolrServerException sse) {
        // check if already exists
        if (safeCheckCollectionExists(collectionListUrl, collectionName)) {
          System.err.println("Collection '"+collectionName+"' already exists!");
          System.err.println("\nChecked collection existence using Collections API command:\n"+collectionListUrl);
        } else {
          System.err.println("Failed to create collection '"+collectionName+"' due to: "+sse.getMessage());
        }
        System.err.println();
        return 1;
      }

      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(json);
      System.out.println(arr.toString());
      System.out.println();

      return 0;
    }

    protected boolean safeCheckCollectionExists(String url, String collection) {
      boolean exists = false;
      try {
        Map<String,Object> existsCheckResult = getJson(url);
        List<String> collections = (List<String>) existsCheckResult.get("collections");
        exists = collections != null && collections.contains(collection);
      } catch (Exception exc) {
        // just ignore it since we're only interested in a positive result here
      }
      return exists;
    }

    protected int optionAsInt(CommandLine cli, String option, int defaultVal) {
      return Integer.parseInt(cli.getOptionValue(option, String.valueOf(defaultVal)));
    }
  } // end CreateCollectionTool class

  public static class CreateCoreTool implements Tool {

    @Override
    public String getName() {
      return "create_core";
    }

    @SuppressWarnings("static-access")
    @Override
    public Option[] getOptions() {
      return new Option[] {
          OptionBuilder
              .withArgName("URL")
              .hasArg()
              .isRequired(false)
              .withDescription("Base Solr URL, default is http://localhost:8983/solr")
              .create("solrUrl"),
          OptionBuilder
              .withArgName("NAME")
              .hasArg()
              .isRequired(true)
              .withDescription("Name of the core to create.")
              .create(NAME),
          OptionBuilder
              .withArgName("CONFIG")
              .hasArg()
              .isRequired(false)
              .withDescription("Configuration directory to copy when creating the new core; default is "+DEFAULT_CONFIG_SET)
              .create("confdir"),
          OptionBuilder
              .withArgName("DIR")
              .hasArg()
              .isRequired(true)
              .withDescription("Path to configsets directory on the local system.")
              .create("configsetsDir")
      };
    }

    @Override
    public int runTool(CommandLine cli) throws Exception {

      String solrUrl = cli.getOptionValue("solrUrl", "http://localhost:8983/solr");
      if (!solrUrl.endsWith("/"))
        solrUrl += "/";

      File configsetsDir = new File(cli.getOptionValue("configsetsDir"));
      if (!configsetsDir.isDirectory())
        throw new FileNotFoundException(configsetsDir.getAbsolutePath() + " not found!");

      String configSet = cli.getOptionValue("confdir", DEFAULT_CONFIG_SET);
      File configSetDir = new File(configsetsDir, configSet);
      if (!configSetDir.isDirectory()) {
        // we allow them to pass a directory instead of a configset name
        File possibleConfigDir = new File(configSet);
        if (possibleConfigDir.isDirectory()) {
          configSetDir = possibleConfigDir;
        } else {
          throw new FileNotFoundException("Specified config directory " + configSet +
              " not found in " + configsetsDir.getAbsolutePath());
        }
      }

      String coreName = cli.getOptionValue(NAME);

      String systemInfoUrl = solrUrl+"admin/info/system";
      CloseableHttpClient httpClient = getHttpClient();
      String solrHome = null;
      try {
        Map<String,Object> systemInfo = getJson(httpClient, systemInfoUrl, 2);
        if ("solrcloud".equals(systemInfo.get("mode"))) {
          System.err.println("\nERROR: Solr at "+solrUrl+
              " is running in SolrCloud mode, please use create_collection command instead.\n");
          return 1;
        }

        // convert raw JSON into user-friendly output
        solrHome = (String)systemInfo.get("solr_home");
        if (solrHome == null)
          solrHome = configsetsDir.getParentFile().getAbsolutePath();

      } finally {
        closeHttpClient(httpClient);
      }

      String coreStatusUrl = solrUrl+"admin/cores?action=STATUS&core="+coreName;
      if (safeCheckCoreExists(coreStatusUrl, coreName)) {
        System.err.println("\nCore '"+coreName+"' already exists!");
        System.err.println("\nChecked core existence using Core API command:\n"+coreStatusUrl);
        System.err.println();
        return 1;
      }

      File coreInstanceDir = new File(solrHome, coreName);
      File confDir = new File(configSetDir,"conf");
      if (!coreInstanceDir.isDirectory()) {
        coreInstanceDir.mkdirs();
        if (!coreInstanceDir.isDirectory())
          throw new IOException("Failed to create new core instance directory: "+coreInstanceDir.getAbsolutePath());

        if (confDir.isDirectory()) {
          FileUtils.copyDirectoryToDirectory(confDir, coreInstanceDir);
        } else {
          // hmmm ... the configset we're cloning doesn't have a conf sub-directory,
          // we'll just assume it is OK if it has solrconfig.xml
          if ((new File(configSetDir, "solrconfig.xml")).isFile()) {
            FileUtils.copyDirectory(configSetDir, new File(coreInstanceDir, "conf"));
          } else {
            System.err.println("\n"+configSetDir.getAbsolutePath()+" doesn't contain a conf subdirectory or solrconfig.xml\n");
            return 1;
          }
        }
        System.out.println("\nSetup new core instance directory:\n"+coreInstanceDir.getAbsolutePath());
      }

      String createCoreUrl =
          String.format(Locale.ROOT,
              "%sadmin/cores?action=CREATE&name=%s&instanceDir=%s",
              solrUrl,
              coreName,
              coreName);

      System.out.println("\nCreating new core '"+coreName+"' using command:\n"+createCoreUrl+"\n");

      Map<String,Object> json = null;
      try {
        json = getJson(createCoreUrl);
      } catch (SolrServerException sse) {
        // mostly likely the core already exists ...
        if (safeCheckCoreExists(coreStatusUrl, coreName)) {
          // core already exists
          System.err.println("Core '"+coreName+"' already exists!");
          System.err.println("\nChecked core existence using Core API command:\n"+coreStatusUrl);
        } else {
          System.err.println("Failed to create core '"+coreName+"' due to: "+sse.getMessage());
        }
        System.err.println();
        return 1;
      }

      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(json);
      System.out.println(arr.toString());
      System.out.println();

      return 0;
    }

    protected boolean safeCheckCoreExists(String coreStatusUrl, String coreName) {
      boolean exists = false;
      try {
        Map<String,Object> existsCheckResult = getJson(coreStatusUrl);
        Map<String,Object> status = (Map<String, Object>)existsCheckResult.get("status");
        Map<String,Object> coreStatus = (Map<String, Object>)status.get(coreName);
        exists = coreStatus != null && coreStatus.containsKey(NAME);
      } catch (Exception exc) {
        // just ignore it since we're only interested in a positive result here
      }
      return exists;
    }
  } // end CreateCoreTool class

  public static class CreateTool implements Tool {

    @Override
    public String getName() {
      return "create";
    }

    @SuppressWarnings("static-access")
    @Override
    public Option[] getOptions() {
      return CREATE_COLLECTION_OPTIONS;
    }

    @Override
    public int runTool(CommandLine cli) throws Exception {

      String solrUrl = cli.getOptionValue("solrUrl", "http://localhost:8983/solr");
      if (!solrUrl.endsWith("/"))
        solrUrl += "/";

      String systemInfoUrl = solrUrl+"admin/info/system";
      CloseableHttpClient httpClient = getHttpClient();

      int result = -1;
      Tool tool = null;
      try {
        Map<String, Object> systemInfo = getJson(httpClient, systemInfoUrl, 2);
        if ("solrcloud".equals(systemInfo.get("mode"))) {
          tool = new CreateCollectionTool();
        } else {
          tool = new CreateCoreTool();
        }
        result = tool.runTool(cli);
      } catch (Exception exc) {
        System.err.println("ERROR: create failed due to: "+exc.getMessage());
        System.err.println();
        result = 1;
      } finally {
        closeHttpClient(httpClient);
      }

      return result;
    }

  } // end CreateTool class

  public static class DeleteTool implements Tool {

    @Override
    public String getName() {
      return "delete";
    }

    @SuppressWarnings("static-access")
    @Override
    public Option[] getOptions() {
      return new Option[]{
          OptionBuilder
              .withArgName("URL")
              .hasArg()
              .isRequired(false)
              .withDescription("Base Solr URL, default is http://localhost:8983/solr")
              .create("solrUrl"),
          OptionBuilder
              .withArgName("NAME")
              .hasArg()
              .isRequired(true)
              .withDescription("Name of the core / collection to delete.")
              .create(NAME),
          OptionBuilder
              .withArgName("true|false")
              .hasArg()
              .isRequired(false)
              .withDescription("Flag to indicate if the underlying configuration directory for a collection should also be deleted; default is true")
              .create("deleteConfig"),
          OptionBuilder
              .isRequired(false)
              .withDescription("Skip safety checks when deleting the configuration directory used by a collection")
              .create("forceDeleteConfig"),
          OptionBuilder
              .withArgName("HOST")
              .hasArg()
              .isRequired(false)
              .withDescription("Address of the Zookeeper ensemble; defaults to: "+ZK_HOST)
              .create("zkHost")
      };
    }

    @Override
    public int runTool(CommandLine cli) throws Exception {

      // quiet down the ZK logging for cli tools
      LogManager.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
      LogManager.getLogger("org.apache.solr.common.cloud").setLevel(Level.WARN);

      String solrUrl = cli.getOptionValue("solrUrl", "http://localhost:8983/solr");
      if (!solrUrl.endsWith("/"))
        solrUrl += "/";

      String systemInfoUrl = solrUrl+"admin/info/system";
      CloseableHttpClient httpClient = getHttpClient();

      int result = 0;
      try {
        Map<String,Object> systemInfo = getJson(httpClient, systemInfoUrl, 2);
        if ("solrcloud".equals(systemInfo.get("mode"))) {
          result = deleteCollection(cli);
        } else {
          result = deleteCore(cli, httpClient, solrUrl);
        }
      } finally {
        closeHttpClient(httpClient);
      }

      return result;
    }

    protected int deleteCollection(CommandLine cli) throws Exception {

      String zkHost = getZkHost(cli);

      int toolExitStatus = 0;
      try (CloudSolrClient cloudSolrClient = new CloudSolrClient(zkHost)) {
        System.out.println("Connecting to ZooKeeper at " + zkHost);
        cloudSolrClient.connect();
        toolExitStatus = deleteCollection(cloudSolrClient, cli);
      } catch (Exception exc) {
        // since this is a CLI, spare the user the stacktrace
        String excMsg = exc.getMessage();
        if (excMsg != null) {
          System.err.println("\nERROR: "+excMsg+"\n");
          toolExitStatus = 1;
        } else {
          throw exc;
        }
      }

      return toolExitStatus;
    }

    protected int deleteCollection(CloudSolrClient cloudSolrClient, CommandLine cli) throws Exception {
      Set<String> liveNodes = cloudSolrClient.getZkStateReader().getClusterState().getLiveNodes();
      if (liveNodes.isEmpty())
        throw new IllegalStateException("No live nodes found! Cannot delete a collection until " +
            "there is at least 1 live node in the cluster.");
      String firstLiveNode = liveNodes.iterator().next();
      ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
      String baseUrl = zkStateReader.getBaseUrlForNodeName(firstLiveNode);

      String collectionName = cli.getOptionValue(NAME);

      if (!zkStateReader.getClusterState().hasCollection(collectionName)) {
        System.err.println("\nERROR: Collection "+collectionName+" not found!");
        System.err.println();
        return 1;
      }

      String configName = zkStateReader.readConfigName(collectionName);
      boolean deleteConfig = "true".equals(cli.getOptionValue("deleteConfig", "true"));
      if (deleteConfig && configName != null) {
        if (cli.hasOption("forceDeleteConfig")) {
          log.warn("Skipping safety checks, configuration directory "+configName+" will be deleted with impunity.");
        } else {
          // need to scan all Collections to see if any are using the config
          Set<String> collections = zkStateReader.getClusterState().getCollections();

          // give a little note to the user if there are many collections in case it takes a while
          if (collections.size() > 50)
            log.info("Scanning " + collections.size() +
                " to ensure no other collections are using config " + configName);

          for (String next : collections) {
            if (collectionName.equals(next))
              continue; // don't check the collection we're deleting

            if (configName.equals(zkStateReader.readConfigName(next))) {
              deleteConfig = false;
              log.warn("Configuration directory "+configName+" is also being used by "+next+
                  "; configuration will not be deleted from ZooKeeper. You can pass the -forceDeleteConfig flag to force delete.");
              break;
            }
          }
        }
      }

      String deleteCollectionUrl =
          String.format(Locale.ROOT,
              "%s/admin/collections?action=DELETE&name=%s",
              baseUrl,
              collectionName);

      System.out.println("\nDeleting collection '"+collectionName+"' using command:\n"+deleteCollectionUrl+"\n");

      Map<String,Object> json = null;
      try {
        json = getJson(deleteCollectionUrl);
      } catch (SolrServerException sse) {
        System.err.println("Failed to delete collection '"+collectionName+"' due to: "+sse.getMessage());
        System.err.println();
        return 1;
      }

      if (deleteConfig) {
        String configZnode = "/configs/" + configName;
        try {
          zkStateReader.getZkClient().clean(configZnode);
        } catch (Exception exc) {
          System.err.println("\nERROR: Failed to delete configuration directory "+configZnode+" in ZooKeeper due to: "+
            exc.getMessage()+"\nYou'll need to manually delete this znode using the zkcli script.");
        }
      }

      if (json != null) {
        CharArr arr = new CharArr();
        new JSONWriter(arr, 2).write(json);
        System.out.println(arr.toString());
        System.out.println();
      }

      return 0;
    }

    protected int deleteCore(CommandLine cli, CloseableHttpClient httpClient, String solrUrl) throws Exception {

      int status = 0;
      String coreName = cli.getOptionValue(NAME);
      String deleteCoreUrl =
          String.format(Locale.ROOT,
              "%sadmin/cores?action=UNLOAD&core=%s&deleteIndex=true&deleteDataDir=true&deleteInstanceDir=true",
              solrUrl,
              coreName);

      System.out.println("\nDeleting core '"+coreName+"' using command:\n"+deleteCoreUrl+"\n");

      Map<String,Object> json = null;
      try {
        json = getJson(deleteCoreUrl);
      } catch (SolrServerException sse) {
        System.err.println("Failed to delete core '"+coreName+"' due to: "+sse.getMessage());
        System.err.println();
        status = 1;
      }

      if (json != null) {
        CharArr arr = new CharArr();
        new JSONWriter(arr, 2).write(json);
        System.out.println(arr.toString());
        System.out.println();
      }

      return status;
    }

  } // end DeleteTool class
}

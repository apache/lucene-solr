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
import java.io.InputStream;
import java.io.PrintStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.OS;
import org.apache.commons.exec.environment.EnvironmentUtils;
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
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.noggit.CharArr;
import org.noggit.JSONParser;
import org.noggit.JSONWriter;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public static abstract class ToolBase implements Tool {
    protected PrintStream stdout;
    protected boolean verbose = false;

    protected ToolBase() {
      this(System.out);
    }

    protected ToolBase(PrintStream stdout) {
      this.stdout = stdout;
    }

    protected void echo(final String msg) {
      stdout.println(msg);
    }

    public int runTool(CommandLine cli) throws Exception {
      verbose = cli.hasOption("verbose");

      int toolExitStatus = 0;
      try {
        runImpl(cli);
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

    protected abstract void runImpl(CommandLine cli) throws Exception;
  }
  
  /**
   * Helps build SolrCloud aware tools by initializing a CloudSolrClient
   * instance before running the tool.
   */
  public static abstract class SolrCloudTool extends ToolBase {

    protected SolrCloudTool(PrintStream stdout) { super(stdout); }

    public Option[] getOptions() {
      return cloudOptions;
    }
    
    protected void runImpl(CommandLine cli) throws Exception {
      String zkHost = cli.getOptionValue("zkHost", ZK_HOST);
      
      log.debug("Connecting to Solr cluster: " + zkHost);
      try (CloudSolrClient cloudSolrClient = new CloudSolrClient(zkHost)) {

        String collection = cli.getOptionValue("collection");
        if (collection != null)
          cloudSolrClient.setDefaultCollection(collection);
        
        cloudSolrClient.connect();
        runCloudTool(cloudSolrClient, cli);
      }
    }
    
    /**
     * Runs a SolrCloud tool with CloudSolrServer initialized
     */
    protected abstract void runCloudTool(CloudSolrClient cloudSolrClient, CommandLine cli)
        throws Exception;
  }
  
  public static Logger log = LoggerFactory.getLogger(SolrCLI.class);
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

  private static void exit(int exitStatus) {
    // TODO: determine if we're running in a test and don't exit
    try {
      System.exit(exitStatus);
    } catch (java.lang.SecurityException secExc) {
      if (exitStatus != 0)
        throw new RuntimeException("SolrCLI failed to exit with status "+exitStatus);
    }
  }
        
  /**
   * Runs a tool.
   */
  public static void main(String[] args) throws Exception {
    if (args == null || args.length == 0 || args[0] == null || args[0].trim().length() == 0) {
      System.err.println("Invalid command-line args! Must pass the name of a tool to run.\n"
          + "Supported tools:\n");
      displayToolOptions(System.err);
      exit(1);
    }

    if (args.length == 1 && Arrays.asList("-v","-version","version").contains(args[0])) {
      // Simple version tool, no need for its own class
      System.out.println(Version.LATEST);
      exit(0);
    }

    String configurerClassName = System.getProperty("solr.authentication.httpclient.configurer");
    if (configurerClassName!=null) {
      try {
        Class c = Class.forName(configurerClassName);
        HttpClientConfigurer configurer = (HttpClientConfigurer)c.newInstance();
        HttpClientUtil.setConfigurer(configurer);
        log.info("Set HttpClientConfigurer from: "+configurerClassName);
      } catch (Exception ex) {
        throw new RuntimeException("Error during loading of configurer '"+configurerClassName+"'.", ex);
      }
    }

    // Determine the tool
    String toolType = args[0].trim().toLowerCase(Locale.ROOT);
    Tool tool = newTool(toolType);

    // the parser doesn't like -D props
    List<String> toolArgList = new ArrayList<String>();
    List<String> dashDList = new ArrayList<String>();
    for (int a=1; a < args.length; a++) {
      String arg = args[a];
      if (arg.startsWith("-D")) {
        dashDList.add(arg);
      } else {
        toolArgList.add(arg);
      }
    }
    String[] toolArgs = toolArgList.toArray(new String[0]);

    // process command-line args to configure this application
    CommandLine cli = 
        processCommandLineArgs(joinCommonAndToolOptions(tool.getOptions()), toolArgs);

    List argList = cli.getArgList();
    argList.addAll(dashDList);

    // for SSL support, try to accommodate relative paths set for SSL store props
    String solrInstallDir = System.getProperty("solr.install.dir");
    if (solrInstallDir != null) {
      checkSslStoreSysProp(solrInstallDir, "keyStore");
      checkSslStoreSysProp(solrInstallDir, "trustStore");
    }

    // run the tool
    exit(tool.runTool(cli));
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
    else if ("config".equals(toolType))
      return new ConfigTool();
    else if ("run_example".equals(toolType))
      return new RunExampleTool();

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
    formatter.printHelp("config", getToolOptions(new ConfigTool()));
    formatter.printHelp("run_example", getToolOptions(new RunExampleTool()));

    List<Class<Tool>> toolClasses = findToolClassesInPackage("org.apache.solr.util");
    for (Class<Tool> next : toolClasses) {
      Tool tool = next.newInstance();
      formatter.printHelp(tool.getName(), getToolOptions(tool));      
    }    
  }
  
  private static Options getToolOptions(Tool tool) {
    Options options = new Options();
    options.addOption("help", false, "Print this message");
    options.addOption("verbose", false, "Generate verbose log messages");
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
    
    options.addOption("help", false, "Print this message");
    options.addOption("verbose", false, "Generate verbose log messages");
    
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
          if ("--help".equals(args[z]) || "-help".equals(args[z])) {
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
      exit(1);
    }
    
    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(SolrCLI.class.getName(), options);
      exit(0);
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
      log.debug("Failed to find Tool impl classes in "+packageName+" due to: "+e);
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

  public static final String JSON_CONTENT_TYPE = "application/json";

  public static NamedList<Object> postJsonToSolr(SolrClient solrClient, String updatePath, String jsonBody) throws Exception {
    ContentStreamBase.StringStream contentStream = new ContentStreamBase.StringStream(jsonBody);
    contentStream.setContentType(JSON_CONTENT_TYPE);
    ContentStreamUpdateRequest req = new ContentStreamUpdateRequest(updatePath);
    req.addContentStream(contentStream);
    return solrClient.request(req);
  }

  /**
   * Useful when a tool just needs to send one request to Solr. 
   */
  public static Map<String,Object> getJson(String getUrl) throws Exception {
    Map<String,Object> json = null;
    CloseableHttpClient httpClient = getHttpClient();
    try {
      json = getJson(httpClient, getUrl, 2, true);
    } finally {
      closeHttpClient(httpClient);
    }
    return json;
  }
  
  /**
   * Utility function for sending HTTP GET request to Solr with built-in retry support.
   */
  public static Map<String,Object> getJson(HttpClient httpClient, String getUrl, int attempts, boolean isFirstAttempt) throws Exception {
    Map<String,Object> json = null;
    if (attempts >= 1) {
      try {
        json = getJson(httpClient, getUrl);
      } catch (Exception exc) {
        if (--attempts > 0 && checkCommunicationError(exc)) {
          if (!isFirstAttempt) // only show the log warning after the second attempt fails
            log.warn("Request to "+getUrl+" failed due to: "+exc.getMessage()+
                ", sleeping for 5 seconds before re-trying the request ...");
          try {
            Thread.sleep(5000);
          } catch (InterruptedException ie) { Thread.interrupted(); }
          
          // retry using recursion with one-less attempt available
          json = getJson(httpClient, getUrl, attempts, false);
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
    } else {
      // make sure no "failure" object in there either
      Object failureObj = json.get("failure");
      if (failureObj != null) {
        if (failureObj instanceof Map) {
          Object err = ((Map)failureObj).get("");
          if (err != null)
            throw new SolrServerException(err.toString());
        }
        throw new SolrServerException(failureObj.toString());
      }
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
  public static class StatusTool extends ToolBase {

    public StatusTool() { this(System.out); }
    public StatusTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "status";
    }
    
    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[] {
        OptionBuilder
            .withArgName("URL")
            .hasArg()
            .isRequired(false)
            .withDescription("Address of the Solr Web application, defaults to: "+DEFAULT_SOLR_URL)
            .create("solr"),
          OptionBuilder
            .withArgName("SECS")
            .hasArg()
            .isRequired(false)
            .withDescription("Wait up to the specified number of seconds to see Solr running.")
            .create("maxWaitSecs")
      };      
    }    

    protected void runImpl(CommandLine cli) throws Exception {
      int maxWaitSecs = Integer.parseInt(cli.getOptionValue("maxWaitSecs", "0"));
      String solrUrl = cli.getOptionValue("solr", DEFAULT_SOLR_URL);
      if (maxWaitSecs > 0) {
        int solrPort = (new URL(solrUrl)).getPort();
        echo("Waiting up to "+maxWaitSecs+" to see Solr running on port "+solrPort);
        try {
          waitToSeeSolrUp(solrUrl, maxWaitSecs);
          echo("Started Solr server on port "+solrPort+". Happy searching!");
        } catch (TimeoutException timeout) {
          throw new Exception("Solr at "+solrUrl+" did not come online within "+maxWaitSecs+" seconds!");
        }
      } else {
        try {
          CharArr arr = new CharArr();
          new JSONWriter(arr, 2).write(getStatus(solrUrl));
          echo(arr.toString());
        } catch (Exception exc) {
          if (checkCommunicationError(exc)) {
            // this is not actually an error from the tool as it's ok if Solr is not online.
            System.err.println("Solr at "+solrUrl+" not online.");
          } else {
            throw new Exception("Failed to get system information from " + solrUrl + " due to: "+exc);
          }
        }
      }
    }

    public Map<String,Object> waitToSeeSolrUp(String solrUrl, int maxWaitSecs) throws Exception {
      long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(maxWaitSecs, TimeUnit.SECONDS);
      while (System.nanoTime() < timeout) {
        try {
          return getStatus(solrUrl);
        } catch (Exception exc) {
          try {
            Thread.sleep(2000L);
          } catch (InterruptedException interrupted) {
            timeout = 0; // stop looping
          }
        }
      }
      throw new TimeoutException("Did not see Solr at "+solrUrl+" come online within "+maxWaitSecs);
    }

    public Map<String,Object> getStatus(String solrUrl) throws Exception {
      Map<String,Object> status = null;

      if (!solrUrl.endsWith("/"))
        solrUrl += "/";

      String systemInfoUrl = solrUrl+"admin/info/system";
      CloseableHttpClient httpClient = getHttpClient();
      try {
        // hit Solr to get system info
        Map<String,Object> systemInfo = getJson(httpClient, systemInfoUrl, 2, true);
        // convert raw JSON into user-friendly output
        status = reportStatus(solrUrl, systemInfo, httpClient);
      } finally {
        closeHttpClient(httpClient);
      }

      return status;
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
      Map<String,Object> json = getJson(httpClient, clusterStatusUrl, 2, true);
      
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
  public static class ApiTool extends ToolBase {

    public ApiTool() { this(System.out); }
    public ApiTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "api";
    }
    
    @SuppressWarnings("static-access")
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

    protected void runImpl(CommandLine cli) throws Exception {
      String getUrl = cli.getOptionValue("get");
      if (getUrl != null) {
        Map<String,Object> json = getJson(getUrl);
        
        // pretty-print the response to stdout
        CharArr arr = new CharArr();
        new JSONWriter(arr, 2).write(json);
        echo(arr.toString());        
      }
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

    public HealthcheckTool() { this(System.out); }
    public HealthcheckTool(PrintStream stdout) { super(stdout); }

    @Override
    public String getName() {
      return "healthcheck";
    }
        
    @Override
    protected void runCloudTool(CloudSolrClient cloudSolrClient, CommandLine cli) throws Exception {
      
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
              Map<String,Object> info = getJson(solr.getHttpClient(), systemInfoUrl, 2, true);
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
      echo(arr.toString());
    }
  } // end HealthcheckTool

  private static final Option[] CREATE_COLLECTION_OPTIONS = new Option[] {
        OptionBuilder
            .withArgName("HOST")
            .hasArg()
            .isRequired(false)
            .withDescription("Address of the Zookeeper ensemble; defaults to: " + ZK_HOST)
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

  /**
   * Get the base URL of a live Solr instance from either the solrUrl command-line option from ZooKeeper.
   */
  public static String resolveSolrUrl(CommandLine cli) throws Exception {
    String solrUrl = cli.getOptionValue("solrUrl");
    if (solrUrl == null) {
      String zkHost = cli.getOptionValue("zkHost");
      if (zkHost == null)
        throw new IllegalStateException("Must provide either the '-solrUrl' or '-zkHost' parameters!");

      try (CloudSolrClient cloudSolrClient = new CloudSolrClient(zkHost)) {
        cloudSolrClient.connect();
        Set<String> liveNodes = cloudSolrClient.getZkStateReader().getClusterState().getLiveNodes();
        if (liveNodes.isEmpty())
          throw new IllegalStateException("No live nodes found! Cannot determine 'solrUrl' from ZooKeeper: "+zkHost);

        String firstLiveNode = liveNodes.iterator().next();
        solrUrl = cloudSolrClient.getZkStateReader().getBaseUrlForNodeName(firstLiveNode);
      }
    }
    return solrUrl;
  }

  /**
   * Get the ZooKeeper connection string from either the zkHost command-line option or by looking it
   * up from a running Solr instance based on the solrUrl option.
   */
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
      Map<String,Object> systemInfo = getJson(httpClient, systemInfoUrl, 2, true);

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

  public static boolean safeCheckCollectionExists(String url, String collection) {
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

  public static boolean safeCheckCoreExists(String coreStatusUrl, String coreName) {
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

  /**
   * Supports create_collection command in the bin/solr script.
   */
  public static class CreateCollectionTool extends ToolBase {
    
    public CreateCollectionTool() {
      this(System.out);
    }
    
    public CreateCollectionTool(PrintStream stdout) {
      super(stdout);
    }

    public String getName() {
      return "create_collection";
    }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return CREATE_COLLECTION_OPTIONS;
    }

    protected void runImpl(CommandLine cli) throws Exception {

      String zkHost = getZkHost(cli);
      if (zkHost == null) {
        throw new IllegalStateException("Solr at "+cli.getOptionValue("solrUrl")+
            " is running in standalone server mode, please use the create_core command instead;\n" +
            "create_collection can only be used when running in SolrCloud mode.\n");
      }

      try (CloudSolrClient cloudSolrClient = new CloudSolrClient(zkHost)) {
        echo("\nConnecting to ZooKeeper at " + zkHost+" ...");
        cloudSolrClient.connect();
        runCloudTool(cloudSolrClient, cli);
      }
    }

    protected void runCloudTool(CloudSolrClient cloudSolrClient, CommandLine cli) throws Exception {

      Set<String> liveNodes = cloudSolrClient.getZkStateReader().getClusterState().getLiveNodes();
      if (liveNodes.isEmpty())
        throw new IllegalStateException("No live nodes found! Cannot create a collection until " +
            "there is at least 1 live node in the cluster.");
      
      String baseUrl = cli.getOptionValue("solrUrl");
      if (baseUrl == null) {
        String firstLiveNode = liveNodes.iterator().next();
        baseUrl = cloudSolrClient.getZkStateReader().getBaseUrlForNodeName(firstLiveNode);
      }

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
          cloudSolrClient.getZkStateReader().getZkClient().exists("/configs/" + confname, true);

      if (".system".equals(collectionName)) {
        //do nothing
      } else if (configExistsInZk) {
        echo("Re-using existing configuration directory "+confname);
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
            throw new IllegalArgumentException("Specified configuration directory "+configSetDir.getAbsolutePath()+
                " is invalid;\nit should contain either conf sub-directory or solrconfig.xml");
          }
        }

        // test to see if that config exists in ZK
        echo("Uploading "+confDir.getAbsolutePath()+
            " for config "+confname+" to ZooKeeper at "+cloudSolrClient.getZkHost());
        cloudSolrClient.uploadConfig(confDir.toPath(), confname);
      }

      // since creating a collection is a heavy-weight operation, check for existence first
      String collectionListUrl = baseUrl+"/admin/collections?action=list";
      if (safeCheckCollectionExists(collectionListUrl, collectionName)) {
        throw new IllegalStateException("\nCollection '"+collectionName+
            "' already exists!\nChecked collection existence using Collections API command:\n"+
            collectionListUrl);
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

      echo("\nCreating new collection '"+collectionName+"' using command:\n"+createCollectionUrl+"\n");

      Map<String,Object> json = null;
      try {
        json = getJson(createCollectionUrl);
      } catch (SolrServerException sse) {
        throw new Exception("Failed to create collection '"+collectionName+"' due to: "+sse.getMessage());
      }

      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(json);
      echo(arr.toString());
    }

    protected int optionAsInt(CommandLine cli, String option, int defaultVal) {
      return Integer.parseInt(cli.getOptionValue(option, String.valueOf(defaultVal)));
    }
  } // end CreateCollectionTool class

  public static class CreateCoreTool extends ToolBase {

    public CreateCoreTool() { this(System.out); }
    public CreateCoreTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "create_core";
    }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[] {
          OptionBuilder
              .withArgName("URL")
              .hasArg()
              .isRequired(false)
              .withDescription("Base Solr URL, default is " + DEFAULT_SOLR_URL)
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

    protected void runImpl(CommandLine cli) throws Exception {

      String solrUrl = cli.getOptionValue("solrUrl", DEFAULT_SOLR_URL);
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
        Map<String,Object> systemInfo = getJson(httpClient, systemInfoUrl, 2, true);
        if ("solrcloud".equals(systemInfo.get("mode"))) {
          throw new IllegalStateException("Solr at "+solrUrl+
              " is running in SolrCloud mode, please use create_collection command instead.");
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
        throw new IllegalArgumentException("\nCore '"+coreName+
            "' already exists!\nChecked core existence using Core API command:\n"+coreStatusUrl);
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
            throw new IllegalArgumentException("\n"+configSetDir.getAbsolutePath()+" doesn't contain a conf subdirectory or solrconfig.xml\n");
          }
        }
        echo("\nCopying configuration to new core instance directory:\n" + coreInstanceDir.getAbsolutePath());
      }

      String createCoreUrl =
          String.format(Locale.ROOT,
              "%sadmin/cores?action=CREATE&name=%s&instanceDir=%s",
              solrUrl,
              coreName,
              coreName);

      echo("\nCreating new core '" + coreName + "' using command:\n" + createCoreUrl + "\n");

      try {
        Map<String,Object> json = getJson(createCoreUrl);
        CharArr arr = new CharArr();
        new JSONWriter(arr, 2).write(json);
        echo(arr.toString());
        echo("\n");
      } catch (Exception e) {
        /* create-core failed, cleanup the copied configset before propagating the error. */
        FileUtils.deleteDirectory(coreInstanceDir);
        throw e;
      }
    }
  } // end CreateCoreTool class

  public static class CreateTool extends ToolBase {

    public CreateTool() { this(System.out); }
    public CreateTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "create";
    }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return CREATE_COLLECTION_OPTIONS;
    }

    protected void runImpl(CommandLine cli) throws Exception {

      String solrUrl = cli.getOptionValue("solrUrl", DEFAULT_SOLR_URL);
      if (!solrUrl.endsWith("/"))
        solrUrl += "/";

      String systemInfoUrl = solrUrl+"admin/info/system";
      CloseableHttpClient httpClient = getHttpClient();

      Tool tool = null;
      try {
        Map<String, Object> systemInfo = getJson(httpClient, systemInfoUrl, 2, true);
        if ("solrcloud".equals(systemInfo.get("mode"))) {
          tool = new CreateCollectionTool(stdout);
        } else {
          tool = new CreateCoreTool(stdout);
        }
        tool.runTool(cli);
      } finally {
        closeHttpClient(httpClient);
      }
    }

  } // end CreateTool class

  public static class DeleteTool extends ToolBase {

    public DeleteTool() { this(System.out); }
    public DeleteTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "delete";
    }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[]{
          OptionBuilder
              .withArgName("URL")
              .hasArg()
              .isRequired(false)
              .withDescription("Base Solr URL, default is " + DEFAULT_SOLR_URL)
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

    protected void runImpl(CommandLine cli) throws Exception {

      String solrUrl = cli.getOptionValue("solrUrl", DEFAULT_SOLR_URL);
      if (!solrUrl.endsWith("/"))
        solrUrl += "/";

      String systemInfoUrl = solrUrl+"admin/info/system";
      CloseableHttpClient httpClient = getHttpClient();
      try {
        Map<String,Object> systemInfo = getJson(httpClient, systemInfoUrl, 2, true);
        if ("solrcloud".equals(systemInfo.get("mode"))) {
          deleteCollection(cli);
        } else {
          deleteCore(cli, httpClient, solrUrl);
        }
      } finally {
        closeHttpClient(httpClient);
      }
    }

    protected void deleteCollection(CommandLine cli) throws Exception {
      String zkHost = getZkHost(cli);
      try (CloudSolrClient cloudSolrClient = new CloudSolrClient(zkHost)) {
        echo("Connecting to ZooKeeper at " + zkHost);
        cloudSolrClient.connect();
        deleteCollection(cloudSolrClient, cli);
      }
    }

    protected void deleteCollection(CloudSolrClient cloudSolrClient, CommandLine cli) throws Exception {
      Set<String> liveNodes = cloudSolrClient.getZkStateReader().getClusterState().getLiveNodes();
      if (liveNodes.isEmpty())
        throw new IllegalStateException("No live nodes found! Cannot delete a collection until " +
            "there is at least 1 live node in the cluster.");

      String firstLiveNode = liveNodes.iterator().next();
      ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
      String baseUrl = zkStateReader.getBaseUrlForNodeName(firstLiveNode);
      String collectionName = cli.getOptionValue(NAME);
      if (!zkStateReader.getClusterState().hasCollection(collectionName)) {
        throw new IllegalArgumentException("Collection "+collectionName+" not found!");
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

      echo("\nDeleting collection '" + collectionName + "' using command:\n" + deleteCollectionUrl + "\n");

      Map<String,Object> json = null;
      try {
        json = getJson(deleteCollectionUrl);
      } catch (SolrServerException sse) {
        throw new Exception("Failed to delete collection '"+collectionName+"' due to: "+sse.getMessage());
      }

      if (deleteConfig) {
        String configZnode = "/configs/" + configName;
        try {
          zkStateReader.getZkClient().clean(configZnode);
        } catch (Exception exc) {
          System.err.println("\nWARNING: Failed to delete configuration directory "+configZnode+" in ZooKeeper due to: "+
            exc.getMessage()+"\nYou'll need to manually delete this znode using the zkcli script.");
        }
      }

      if (json != null) {
        CharArr arr = new CharArr();
        new JSONWriter(arr, 2).write(json);
        echo(arr.toString());
        echo("\n");
      }
    }

    protected void deleteCore(CommandLine cli, CloseableHttpClient httpClient, String solrUrl) throws Exception {
      String coreName = cli.getOptionValue(NAME);
      String deleteCoreUrl =
          String.format(Locale.ROOT,
              "%sadmin/cores?action=UNLOAD&core=%s&deleteIndex=true&deleteDataDir=true&deleteInstanceDir=true",
              solrUrl,
              coreName);

      echo("\nDeleting core '" + coreName + "' using command:\n" + deleteCoreUrl + "\n");

      Map<String,Object> json = null;
      try {
        json = getJson(deleteCoreUrl);
      } catch (SolrServerException sse) {
        throw new Exception("Failed to delete core '"+coreName+"' due to: "+sse.getMessage());
      }

      if (json != null) {
        CharArr arr = new CharArr();
        new JSONWriter(arr, 2).write(json);
        echo(arr.toString());
        echo("\n");
      }
    }
  } // end DeleteTool class

  /**
   * Sends a POST to the Config API to perform a specified action.
   */
  public static class ConfigTool extends ToolBase {

    public ConfigTool() { this(System.out); }
    public ConfigTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "config";
    }

    @SuppressWarnings("static-access")
    @Override
    public Option[] getOptions() {
      Option[] configOptions = new Option[] {
          OptionBuilder
              .withArgName("ACTION")
              .hasArg()
              .isRequired(false)
              .withDescription("Config API action, one of: set-property, unset-property; default is set-property")
              .create("action"),
          OptionBuilder
              .withArgName("PROP")
              .hasArg()
              .isRequired(true)
              .withDescription("Name of the Config API property to apply the action to, such as: updateHandler.autoSoftCommit.maxTime")
              .create("property"),
          OptionBuilder
              .withArgName("VALUE")
              .hasArg()
              .isRequired(false)
              .withDescription("Set the property to this value; accepts JSON objects and strings")
              .create("value"),
          OptionBuilder
              .withArgName("HOST")
              .hasArg()
              .isRequired(false)
              .withDescription("Base Solr URL, which can be used to determine the zkHost if that's not known")
              .create("solrUrl")
      };
      return joinOptions(configOptions, cloudOptions);
    }

    protected void runImpl(CommandLine cli) throws Exception {
      String solrUrl = resolveSolrUrl(cli);
      String action = cli.getOptionValue("action", "set-property");
      String collection = cli.getOptionValue("collection", "gettingstarted");
      String property = cli.getOptionValue("property");
      String value = cli.getOptionValue("value");

      Map<String,Object> jsonObj = new HashMap<>();
      if (value != null) {
        Map<String,String> setMap = new HashMap<>();
        setMap.put(property, value);
        jsonObj.put(action, setMap);
      } else {
        jsonObj.put(action, property);
      }

      CharArr arr = new CharArr();
      (new JSONWriter(arr, 0)).write(jsonObj);
      String jsonBody = arr.toString();

      String updatePath = "/"+collection+"/config";

      echo("\nPOSTing request to Config API: " + solrUrl + updatePath);
      echo(jsonBody);

      try (SolrClient solrClient = new HttpSolrClient(solrUrl)) {
        NamedList<Object> result = postJsonToSolr(solrClient, updatePath, jsonBody);
        Integer statusCode = (Integer)((NamedList)result.get("responseHeader")).get("status");
        if (statusCode == 0) {
          if (value != null) {
            echo("Successfully " + action + " " + property + " to " + value);
          } else {
            echo("Successfully " + action + " " + property);
          }
        } else {
          throw new Exception("Failed to "+action+" property due to:\n"+result);
        }
      }
    }

  } // end ConfigTool class

  /**
   * Supports an interactive session with the user to launch (or relaunch the -e cloud example)
   */
  public static class RunExampleTool extends ToolBase {

    private static final String PROMPT_FOR_NUMBER = "Please enter %s [%d]: ";
    private static final String PROMPT_FOR_NUMBER_IN_RANGE = "Please enter %s between %d and %d [%d]: ";
    private static final String PROMPT_NUMBER_TOO_SMALL = "%d is too small! "+PROMPT_FOR_NUMBER_IN_RANGE;
    private static final String PROMPT_NUMBER_TOO_LARGE = "%d is too large! "+PROMPT_FOR_NUMBER_IN_RANGE;

    protected InputStream userInput;
    protected Executor executor;
    protected String script;
    protected File serverDir;
    protected File exampleDir;
    protected String urlScheme;

    /**
     * Default constructor used by the framework when running as a command-line application.
     */
    public RunExampleTool() {
      this(null, System.in, System.out);
    }

    public RunExampleTool(Executor executor, InputStream userInput, PrintStream stdout) {
      super(stdout);
      this.executor = (executor != null) ? executor : new DefaultExecutor();
      this.userInput = userInput;
    }

    public String getName() {
      return "run_example";
    }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[] {
          OptionBuilder
            .isRequired(false)
            .withDescription("Don't prompt for input; accept all defaults when running examples that accept user input")
            .create("noprompt"),
          OptionBuilder
            .withArgName("NAME")
            .hasArg()
            .isRequired(true)
            .withDescription("Name of the example to launch, one of: cloud, techproducts, dih, schemaless")
            .withLongOpt("example")
            .create('e'),
          OptionBuilder
            .withArgName("PATH")
            .hasArg()
            .isRequired(false)
            .withDescription("Path to the bin/solr script")
            .create("script"),
          OptionBuilder
            .withArgName("DIR")
            .hasArg()
            .isRequired(true)
            .withDescription("Path to the Solr server directory.")
            .withLongOpt("serverDir")
            .create('d'),
          OptionBuilder
            .withArgName("DIR")
            .hasArg()
            .isRequired(false)
            .withDescription("Path to the Solr example directory; if not provided, ${serverDir}/../example is expected to exist.")
            .create("exampleDir"),
          OptionBuilder
            .withArgName("SCHEME")
            .hasArg()
            .isRequired(false)
            .withDescription("Solr URL scheme: http or https, defaults to http if not specified")
            .create("urlScheme"),
          OptionBuilder
            .withArgName("PORT")
            .hasArg()
            .isRequired(false)
            .withDescription("Specify the port to start the Solr HTTP listener on; default is 8983")
            .withLongOpt("port")
            .create('p'),
          OptionBuilder
            .withArgName("HOSTNAME")
            .hasArg()
            .isRequired(false)
            .withDescription("Specify the hostname for this Solr instance")
            .withLongOpt("host")
            .create('h'),
          OptionBuilder
            .withArgName("ZKHOST")
            .hasArg()
            .isRequired(false)
            .withDescription("ZooKeeper connection string; only used when running in SolrCloud mode using -c")
            .withLongOpt("zkhost")
            .create('z'),
          OptionBuilder
            .isRequired(false)
            .withDescription("Start Solr in SolrCloud mode; if -z not supplied, an embedded ZooKeeper instance is started on Solr port+1000, such as 9983 if Solr is bound to 8983")
            .withLongOpt("cloud")
            .create('c'),
          OptionBuilder
            .withArgName("MEM")
            .hasArg()
            .isRequired(false)
            .withDescription("Sets the min (-Xms) and max (-Xmx) heap size for the JVM, such as: -m 4g results in: -Xms4g -Xmx4g; by default, this script sets the heap size to 512m")
            .withLongOpt("memory")
            .create('m'),
          OptionBuilder
            .withArgName("OPTS")
            .hasArg()
            .isRequired(false)
            .withDescription("Additional options to be passed to the JVM when starting example Solr server(s)")
            .withLongOpt("addlopts")
            .create('a')
      };
    }

    protected void runImpl(CommandLine cli) throws Exception {
      this.urlScheme = cli.getOptionValue("urlScheme", "http");

      serverDir = new File(cli.getOptionValue("serverDir"));
      if (!serverDir.isDirectory())
        throw new IllegalArgumentException("Value of -serverDir option is invalid! "+
            serverDir.getAbsolutePath()+" is not a directory!");

      script = cli.getOptionValue("script");
      if (script != null) {
        if (!(new File(script)).isFile())
          throw new IllegalArgumentException("Value of -script option is invalid! "+script+" not found");
      } else {
        File scriptFile = new File(serverDir.getParentFile(), "bin/solr");
        if (scriptFile.isFile()) {
          script = scriptFile.getAbsolutePath();
        } else {
          scriptFile = new File(serverDir.getParentFile(), "bin/solr.cmd");
          if (scriptFile.isFile()) {
            script = scriptFile.getAbsolutePath();
          } else {
            throw new IllegalArgumentException("Cannot locate the bin/solr script! Please pass -script to this application.");
          }
        }
      }

      exampleDir =
          (cli.hasOption("exampleDir")) ? new File(cli.getOptionValue("exampleDir"))
              : new File(serverDir.getParent(), "example");
      if (!exampleDir.isDirectory())
        throw new IllegalArgumentException("Value of -exampleDir option is invalid! "+
            exampleDir.getAbsolutePath()+" is not a directory!");

      if (verbose) {
        echo("Running with\nserverDir="+serverDir.getAbsolutePath()+
            ",\nexampleDir="+exampleDir.getAbsolutePath()+"\nscript="+script);
      }

      String exampleType = cli.getOptionValue("example");
      if ("cloud".equals(exampleType)) {
        runCloudExample(cli);
      } else if ("dih".equals(exampleType)) {
        runDihExample(cli);
      } else if ("techproducts".equals(exampleType) || "schemaless".equals(exampleType)) {
        runExample(cli, exampleType);
      } else {
        throw new IllegalArgumentException("Unsupported example "+exampleType+
            "! Please choose one of: cloud, dih, schemaless, or techproducts");
      }
    }

    protected void runDihExample(CommandLine cli) throws Exception {
      File dihSolrHome = new File(exampleDir, "example-DIH/solr");
      if (!dihSolrHome.isDirectory()) {
        dihSolrHome = new File(serverDir.getParentFile(), "example/example-DIH/solr");
        if (!dihSolrHome.isDirectory()) {
          throw new Exception("example/example-DIH/solr directory not found");
        }
      }

      boolean isCloudMode = cli.hasOption('c');
      String zkHost = cli.getOptionValue('z');
      int port = Integer.parseInt(cli.getOptionValue('p', "8983"));

      Map<String,Object> nodeStatus = startSolr(dihSolrHome, isCloudMode, cli, port, zkHost, 30);
      String solrUrl = (String)nodeStatus.get("baseUrl");
      echo("\nSolr dih example launched successfully. Direct your Web browser to "+solrUrl+" to visit the Solr Admin UI");
    }

    protected void runExample(CommandLine cli, String exampleName) throws Exception {
      File exDir = setupExampleDir(serverDir, exampleDir, exampleName);
      String collectionName = "schemaless".equals(exampleName) ? "gettingstarted" : exampleName;
      String configSet =
          "techproducts".equals(exampleName) ? "sample_techproducts_configs" : "data_driven_schema_configs";

      boolean isCloudMode = cli.hasOption('c');
      String zkHost = cli.getOptionValue('z');
      int port = Integer.parseInt(cli.getOptionValue('p', "8983"));
      Map<String,Object> nodeStatus = startSolr(new File(exDir, "solr"), isCloudMode, cli, port, zkHost, 30);

      // invoke the CreateTool
      File configsetsDir = new File(serverDir, "solr/configsets");

      String solrUrl = (String)nodeStatus.get("baseUrl");

      // safe check if core / collection already exists
      boolean alreadyExists = false;
      if (nodeStatus.get("cloud") != null) {
        String collectionListUrl = solrUrl+"/admin/collections?action=list";
        if (safeCheckCollectionExists(collectionListUrl, collectionName)) {
          alreadyExists = true;
          echo("\nWARNING: Collection '"+collectionName+
              "' already exists!\nChecked collection existence using Collections API command:\n"+collectionListUrl+"\n");
        }
      } else {
        String coreName = collectionName;
        String coreStatusUrl = solrUrl+"/admin/cores?action=STATUS&core="+coreName;
        if (safeCheckCoreExists(coreStatusUrl, coreName)) {
          alreadyExists = true;
          echo("\nWARNING: Core '" + coreName +
              "' already exists!\nChecked core existence using Core API command:\n" + coreStatusUrl+"\n");
        }
      }

      if (!alreadyExists) {
        String[] createArgs = new String[] {
            "-name", collectionName,
            "-shards", "1",
            "-replicationFactor", "1",
            "-confname", collectionName,
            "-confdir", configSet,
            "-configsetsDir", configsetsDir.getAbsolutePath(),
            "-solrUrl", solrUrl
        };
        CreateTool createTool = new CreateTool(stdout);
        int createCode =
            createTool.runTool(processCommandLineArgs(joinCommonAndToolOptions(createTool.getOptions()), createArgs));
        if (createCode != 0)
          throw new Exception("Failed to create "+collectionName+" using command: "+ Arrays.asList(createArgs));
      }

      if ("techproducts".equals(exampleName)) {

        File exampledocsDir = new File(exampleDir, "exampledocs");
        if (!exampledocsDir.isDirectory()) {
          File readOnlyExampleDir = new File(serverDir.getParentFile(), "example");
          if (readOnlyExampleDir.isDirectory()) {
            exampledocsDir = new File(readOnlyExampleDir, "exampledocs");
          }
        }

        if (exampledocsDir.isDirectory()) {
          String updateUrl = String.format(Locale.ROOT, "%s/%s/update", solrUrl, collectionName);
          echo("Indexing tech product example docs from "+exampledocsDir.getAbsolutePath());

          String currentPropVal = System.getProperty("url");
          System.setProperty("url", updateUrl);
          SimplePostTool.main(new String[] {exampledocsDir.getAbsolutePath()+"/*.xml"});
          if (currentPropVal != null) {
            System.setProperty("url", currentPropVal); // reset
          } else {
            System.clearProperty("url");
          }
        } else {
          echo("exampledocs directory not found, skipping indexing step for the techproducts example");
        }
      }

      echo("\nSolr "+exampleName+" example launched successfully. Direct your Web browser to "+solrUrl+" to visit the Solr Admin UI");
    }

    protected void runCloudExample(CommandLine cli) throws Exception {

      boolean prompt = !cli.hasOption("noprompt");
      int numNodes = 2;
      int[] cloudPorts = new int[]{ 8983, 7574, 8984, 7575 };
      File cloudDir = new File(exampleDir, "cloud");
      if (!cloudDir.isDirectory())
        cloudDir.mkdir();
      
      echo("\nWelcome to the SolrCloud example!\n");

      Scanner readInput = prompt ? new Scanner(userInput, StandardCharsets.UTF_8.name()) : null;
      if (prompt) {
        echo("This interactive session will help you launch a SolrCloud cluster on your local workstation.");

        // get the number of nodes to start
        numNodes = promptForInt(readInput,
            "To begin, how many Solr nodes would you like to run in your local cluster? (specify 1-4 nodes) [2]: ",
            "a number", numNodes, 1, 4);

        echo("Ok, let's start up "+numNodes+" Solr nodes for your example SolrCloud cluster.");

        // get the ports for each port
        for (int n=0; n < numNodes; n++) {
          String promptMsg = 
              String.format(Locale.ROOT, "Please enter the port for node%d [%d]: ", (n+1), cloudPorts[n]);
          int port = promptForPort(readInput, n+1, promptMsg, cloudPorts[n]);
          while (!isPortAvailable(port)) {
            port = promptForPort(readInput, n+1,
                "Oops! Looks like port "+port+
                    " is already being used by another process. Please choose a different port.", cloudPorts[n]);
          }

          cloudPorts[n] = port;
          if (verbose)
            echo("Using port "+port+" for node "+(n+1));
        }
      } else {
        echo("Starting up "+numNodes+" Solr nodes for your example SolrCloud cluster.\n");
      }

      // setup a unique solr.solr.home directory for each node
      File node1Dir = setupExampleDir(serverDir, cloudDir, "node1");
      for (int n=2; n <= numNodes; n++) {
        File nodeNDir = new File(cloudDir, "node"+n);
        if (!nodeNDir.isDirectory()) {
          echo("Cloning " + node1Dir.getAbsolutePath() + " into\n   "+nodeNDir.getAbsolutePath());
          FileUtils.copyDirectory(node1Dir, nodeNDir);
        } else {
          echo(nodeNDir.getAbsolutePath()+" already exists.");
        }
      }

      // deal with extra args passed to the script to run the example
      String zkHost = cli.getOptionValue('z');

      // start the first node (most likely with embedded ZK)
      Map<String,Object> nodeStatus =
          startSolr(new File(node1Dir,"solr"), true, cli, cloudPorts[0], zkHost, 30);

      if (zkHost == null) {
        Map<String,Object> cloudStatus = (Map<String,Object>)nodeStatus.get("cloud");
        if (cloudStatus != null) {
          String zookeeper = (String)cloudStatus.get("ZooKeeper");
          if (zookeeper != null)
            zkHost = zookeeper;
        }
        if (zkHost == null)
          throw new Exception("Could not get the ZooKeeper connection string for node1!");
      }

      if (numNodes > 1) {
        // start the other nodes
        for (int n = 1; n < numNodes; n++)
          startSolr(new File(cloudDir, "node"+(n+1)+"/solr"), true, cli, cloudPorts[n], zkHost, 30);
      }

      String solrUrl = (String)nodeStatus.get("baseUrl");
      if (solrUrl.endsWith("/"))
        solrUrl = solrUrl.substring(0,solrUrl.length()-1);

      // wait until live nodes == numNodes
      waitToSeeLiveNodes(10 /* max wait */, zkHost, numNodes);

      // create the collection
      String collectionName =
          createCloudExampleCollection(numNodes, readInput, prompt, solrUrl);

      // update the config to enable soft auto-commit
      echo("\nEnabling auto soft-commits with maxTime 3 secs using the Config API");
      setCollectionConfigProperty(solrUrl, collectionName, "updateHandler.autoSoftCommit.maxTime", "3000");

      echo("\n\nSolrCloud example running, please visit: "+solrUrl+" \n");
    }

    protected void setCollectionConfigProperty(String solrUrl, String collectionName, String propName, String propValue) {
      ConfigTool configTool = new ConfigTool(stdout);
      String[] configArgs =
          new String[] { "-collection", collectionName, "-property", propName, "-value", propValue, "-solrUrl", solrUrl };

      // let's not fail if we get this far ... just report error and finish up
      try {
        configTool.runTool(processCommandLineArgs(joinCommonAndToolOptions(configTool.getOptions()), configArgs));
      } catch (Exception exc) {
        System.err.println("Failed to update '"+propName+"' property due to: "+exc);
      }
    }

    protected void waitToSeeLiveNodes(int maxWaitSecs, String zkHost, int numNodes) {
      CloudSolrClient cloudClient = null;
      try {
        cloudClient = new CloudSolrClient(zkHost);
        cloudClient.connect();
        Set<String> liveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes();
        int numLiveNodes = (liveNodes != null) ? liveNodes.size() : 0;
        long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(maxWaitSecs, TimeUnit.SECONDS);
        while (System.nanoTime() < timeout && numLiveNodes < numNodes) {
          echo("\nWaiting up to "+maxWaitSecs+" seconds to see "+
              (numNodes-numLiveNodes)+" more nodes join the SolrCloud cluster ...");
          try {
            Thread.sleep(2000);
          } catch (InterruptedException ie) {
            Thread.interrupted();
          }
          liveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes();
          numLiveNodes = (liveNodes != null) ? liveNodes.size() : 0;
        }
        if (numLiveNodes < numNodes) {
          echo("\nWARNING: Only "+numLiveNodes+" of "+numNodes+
              " are active in the cluster after "+maxWaitSecs+
              " seconds! Please check the solr.log for each node to look for errors.\n");
        }
      } catch (Exception exc) {
        System.err.println("Failed to see if "+numNodes+" joined the SolrCloud cluster due to: "+exc);
      } finally {
        if (cloudClient != null) {
          try {
            cloudClient.close();
          } catch (Exception ignore) {}
        }
      }
    }

    protected Map<String,Object> startSolr(File solrHomeDir,
                                           boolean cloudMode,
                                           CommandLine cli,
                                           int port,
                                           String zkHost,
                                           int maxWaitSecs)
        throws Exception
    {

      String extraArgs = readExtraArgs(cli.getArgs());

      String host = cli.getOptionValue('h');
      String memory = cli.getOptionValue('m');

      String hostArg = (host != null && !"localhost".equals(host)) ? " -h "+host : "";
      String zkHostArg = (zkHost != null) ? " -z "+zkHost : "";
      String memArg = (memory != null) ? " -m "+memory : "";
      String cloudModeArg = cloudMode ? "-cloud " : "";

      String addlOpts = cli.getOptionValue('a');
      String addlOptsArg = (addlOpts != null) ? " -a \""+addlOpts+"\"" : "";

      File cwd = new File(System.getProperty("user.dir"));
      File binDir = (new File(script)).getParentFile();

      boolean isWindows = (OS.isFamilyDOS() || OS.isFamilyWin9x() || OS.isFamilyWindows());
      String callScript = (!isWindows && cwd.equals(binDir.getParentFile())) ? "bin/solr" : script;

      String cwdPath = cwd.getAbsolutePath();
      String solrHome = solrHomeDir.getAbsolutePath();

      // don't display a huge path for solr home if it is relative to the cwd
      if (!isWindows && solrHome.startsWith(cwdPath))
        solrHome = solrHome.substring(cwdPath.length()+1);

      String startCmd =
          String.format(Locale.ROOT, "%s start %s -p %d -s \"%s\" %s %s %s %s %s",
              callScript, cloudModeArg, port, solrHome, hostArg, zkHostArg, memArg, extraArgs, addlOptsArg);
      startCmd = startCmd.replaceAll("\\s+", " ").trim(); // for pretty printing

      echo("\nStarting up Solr on port " + port + " using command:");
      echo(startCmd + "\n");

      String solrUrl =
          String.format(Locale.ROOT, "%s://%s:%d/solr", urlScheme, (host != null ? host : "localhost"), port);

      Map<String,Object> nodeStatus = checkPortConflict(solrUrl, solrHomeDir, port, cli);
      if (nodeStatus != null)
        return nodeStatus; // the server they are trying to start is already running

      int code = 0;
      if (isWindows) {
        // On Windows, the execution doesn't return, so we have to execute async
        // and when calling the script, it seems to be inheriting the environment that launched this app
        // so we have to prune out env vars that may cause issues
        Map<String,String> startEnv = new HashMap<>();
        Map<String,String> procEnv = EnvironmentUtils.getProcEnvironment();
        if (procEnv != null) {
          for (String envVar : procEnv.keySet()) {
            String envVarVal = procEnv.get(envVar);
            if (envVarVal != null && !"EXAMPLE".equals(envVar) && !envVar.startsWith("SOLR_")) {
              startEnv.put(envVar, envVarVal);
            }
          }
        }
        executor.execute(org.apache.commons.exec.CommandLine.parse(startCmd), startEnv, new DefaultExecuteResultHandler());

        // brief wait before proceeding on Windows
        try {
          Thread.sleep(3000);
        } catch (InterruptedException ie) {
          // safe to ignore ...
          Thread.interrupted();
        }

      } else {
        code = executor.execute(org.apache.commons.exec.CommandLine.parse(startCmd));
      }
      if (code != 0)
        throw new Exception("Failed to start Solr using command: "+startCmd);

      return getNodeStatus(solrUrl, maxWaitSecs);
    }

    protected Map<String,Object> checkPortConflict(String solrUrl, File solrHomeDir, int port, CommandLine cli) {
      // quickly check if the port is in use
      if (isPortAvailable(port))
        return null; // not in use ... try to start

      Map<String,Object> nodeStatus = null;
      try {
        nodeStatus = (new StatusTool()).getStatus(solrUrl);
      } catch (Exception ignore) { /* just trying to determine if this example is already running. */ }

      if (nodeStatus != null) {
        String solr_home = (String)nodeStatus.get("solr_home");
        if (solr_home != null) {
          String solrHomePath = solrHomeDir.getAbsolutePath();
          if (!solrHomePath.endsWith("/"))
            solrHomePath += "/";
          if (!solr_home.endsWith("/"))
            solr_home += "/";

          if (solrHomePath.equals(solr_home)) {
            CharArr arr = new CharArr();
            new JSONWriter(arr, 2).write(nodeStatus);
            echo("Solr is already setup and running on port " + port + " with status:\n" + arr.toString());
            echo("\nIf this is not the example node you are trying to start, please choose a different port.");
            nodeStatus.put("baseUrl", solrUrl);
            return nodeStatus;
          }
        }
      }

      throw new IllegalStateException("Port "+port+" is already being used by another process.");
    }

    protected String readExtraArgs(String[] extraArgsArr) {
      String extraArgs = "";
      if (extraArgsArr != null && extraArgsArr.length > 0) {
        StringBuilder sb = new StringBuilder();
        int app = 0;
        for (int e=0; e < extraArgsArr.length; e++) {
          String arg = extraArgsArr[e];
          if ("e".equals(arg) || "example".equals(arg)) {
            e++; // skip over the example arg
            continue;
          }

          if (app > 0) sb.append(" ");
          sb.append(arg);
          ++app;
        }
        extraArgs = sb.toString().trim();
      }
      return extraArgs;
    }

    protected String createCloudExampleCollection(int numNodes, Scanner readInput, boolean prompt, String solrUrl) throws Exception {
      // yay! numNodes SolrCloud nodes running
      int numShards = 2;
      int replicationFactor = 2;
      String cloudConfig = "data_driven_schema_configs";
      String collectionName = "gettingstarted";

      File configsetsDir = new File(serverDir, "solr/configsets");
      String collectionListUrl = solrUrl+"/admin/collections?action=list";

      if (prompt) {
        echo("\nNow let's create a new collection for indexing documents in your "+numNodes+"-node cluster.");

        while (true) {
          collectionName =
              prompt(readInput, "Please provide a name for your new collection: ["+collectionName+"] ", collectionName);

          // Test for existence and then prompt to either create another or skip the create step
          if (safeCheckCollectionExists(collectionListUrl, collectionName)) {
            echo("\nCollection '"+collectionName+"' already exists!");
            int oneOrTwo = promptForInt(readInput,
                "Do you want to re-use the existing collection or create a new one? Enter 1 to reuse, 2 to create new [1]: ", "a 1 or 2", 1, 1, 2);
            if (oneOrTwo == 1) {
              return collectionName;
            } else {
              continue;
            }
          } else {
            break; // user selected a collection that doesn't exist ... proceed on
          }
        }

        numShards = promptForInt(readInput,
            "How many shards would you like to split " + collectionName + " into? [2]", "a shard count", 2, 1, 4);

        replicationFactor = promptForInt(readInput,
            "How many replicas per shard would you like to create? [2] ", "a replication factor", 2, 1, 4);

        echo("Please choose a configuration for the "+collectionName+" collection, available options are:");
        cloudConfig =
            prompt(readInput, "basic_configs, data_driven_schema_configs, or sample_techproducts_configs ["+cloudConfig+"] ", cloudConfig);

        // validate the cloudConfig name
        while (!isValidConfig(configsetsDir, cloudConfig)) {
          echo(cloudConfig+" is not a valid configuration directory! Please choose a configuration for the "+collectionName+" collection, available options are:");
          cloudConfig =
              prompt(readInput, "basic_configs, data_driven_schema_configs, or sample_techproducts_configs ["+cloudConfig+"] ", cloudConfig);
        }
      } else {
        // must verify if default collection exists
        if (safeCheckCollectionExists(collectionListUrl, collectionName)) {
          echo("\nCollection '"+collectionName+"' already exists! Skipping collection creation step.");
          return collectionName;
        }
      }

      // invoke the CreateCollectionTool
      String[] createArgs = new String[] {
          "-name", collectionName,
          "-shards", String.valueOf(numShards),
          "-replicationFactor", String.valueOf(replicationFactor),
          "-confname", collectionName,
          "-confdir", cloudConfig,
          "-configsetsDir", configsetsDir.getAbsolutePath(),
          "-solrUrl", solrUrl
      };

      CreateCollectionTool createCollectionTool = new CreateCollectionTool(stdout);
      int createCode =
          createCollectionTool.runTool(
              processCommandLineArgs(joinCommonAndToolOptions(createCollectionTool.getOptions()), createArgs));

      if (createCode != 0)
        throw new Exception("Failed to create collection using command: "+ Arrays.asList(createArgs));

      return collectionName;
    }

    protected boolean isValidConfig(File configsetsDir, String config) {
      File configDir = new File(configsetsDir, config);
      if (configDir.isDirectory())
        return true;

      // not a built-in configset ... maybe it's a custom directory?
      configDir = new File(config);
      if (configDir.isDirectory())
        return true;

      return false;
    }

    protected Map<String,Object> getNodeStatus(String solrUrl, int maxWaitSecs) throws Exception {
      StatusTool statusTool = new StatusTool();
      if (verbose)
        echo("\nChecking status of Solr at " + solrUrl + " ...");

      URL solrURL = new URL(solrUrl);
      Map<String,Object> nodeStatus = statusTool.waitToSeeSolrUp(solrUrl, maxWaitSecs);
      nodeStatus.put("baseUrl", solrUrl);
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(nodeStatus);
      String mode = (nodeStatus.get("cloud") != null) ? "cloud" : "standalone";
      if (verbose)
        echo("\nSolr is running on "+solrURL.getPort()+" in " + mode + " mode with status:\n" + arr.toString());

      return nodeStatus;
    }

    protected File setupExampleDir(File serverDir, File exampleParentDir, String dirName) throws IOException {
      File solrXml = new File(serverDir, "solr/solr.xml");
      if (!solrXml.isFile())
        throw new IllegalArgumentException("Value of -serverDir option is invalid! "+
            solrXml.getAbsolutePath()+" not found!");

      File zooCfg = new File(serverDir, "solr/zoo.cfg");
      if (!zooCfg.isFile())
        throw new IllegalArgumentException("Value of -serverDir option is invalid! "+
            zooCfg.getAbsolutePath()+" not found!");

      File solrHomeDir = new File(exampleParentDir, dirName+"/solr");
      if (!solrHomeDir.isDirectory()) {
        echo("Creating Solr home directory "+solrHomeDir);
        solrHomeDir.mkdirs();
      } else {
        echo("Solr home directory "+solrHomeDir.getAbsolutePath()+" already exists.");
      }

      copyIfNeeded(solrXml, new File(solrHomeDir, "solr.xml"));
      copyIfNeeded(zooCfg, new File(solrHomeDir, "zoo.cfg"));

      return solrHomeDir.getParentFile();
    }

    protected void copyIfNeeded(File src, File dest) throws IOException {
      if (!dest.isFile())
        FileUtils.copyFile(src, dest);

      if (!dest.isFile())
        throw new IllegalStateException("Required file "+dest.getAbsolutePath()+" not found!");
    }

    protected boolean isPortAvailable(int port) {
      Socket s = null;
      try {
        s = new Socket("localhost", port);
        return false;
      } catch (IOException e) {
        return true;
      } finally {
        if (s != null) {
          try {
            s.close();
          } catch (IOException ignore) {}
        }
      }
    }

    protected Integer promptForPort(Scanner s, int node, String prompt, Integer defVal) {
      return promptForInt(s, prompt, "a port for node "+node, defVal, null, null);
    }

    protected Integer promptForInt(Scanner s, String prompt, String label, Integer defVal, Integer min, Integer max) {
      Integer inputAsInt = null;

      String value = prompt(s, prompt, null /* default is null since we handle that here */);
      if (value != null) {
        int attempts = 3;
        while (value != null && --attempts > 0) {
          try {
            inputAsInt = new Integer(value);

            if (min != null) {
              if (inputAsInt < min) {
                value = prompt(s, String.format(Locale.ROOT, PROMPT_NUMBER_TOO_SMALL, inputAsInt, label, min, max, defVal));
                inputAsInt = null;
                continue;
              }
            }

            if (max != null) {
              if (inputAsInt > max) {
                value = prompt(s, String.format(Locale.ROOT, PROMPT_NUMBER_TOO_LARGE, inputAsInt, label, min, max, defVal));
                inputAsInt = null;
                continue;
              }
            }

          } catch (NumberFormatException nfe) {
            if (verbose)
              echo(value+" is not a number!");

            if (min != null && max != null) {
              value = prompt(s, String.format(Locale.ROOT, PROMPT_FOR_NUMBER_IN_RANGE, label, min, max, defVal));
            } else {
              value = prompt(s, String.format(Locale.ROOT, PROMPT_FOR_NUMBER, label, defVal));
            }
          }
        }
        if (attempts == 0 && value != null && inputAsInt == null)
          echo("Too many failed attempts! Going with default value "+defVal);
      }

      return (inputAsInt != null) ? inputAsInt : defVal;
    }

    protected String prompt(Scanner s, String prompt) {
      return prompt(s, prompt, null);
    }

    protected String prompt(Scanner s, String prompt, String defaultValue) {
      echo(prompt);
      String nextInput = s.nextLine();
      if (nextInput != null) {
        nextInput = nextInput.trim();
        if (nextInput.isEmpty())
          nextInput = null;
      }
      return (nextInput != null) ? nextInput : defaultValue;
    }

  } // end RunExampleTool class
}

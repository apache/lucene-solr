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
package org.apache.solr.util;

import javax.net.ssl.SSLPeerUnverifiedException;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.OS;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.Variable;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.autoscaling.sim.NoopDistributedQueueFactory;
import org.apache.solr.cloud.autoscaling.sim.SimCloudManager;
import org.apache.solr.cloud.autoscaling.sim.SimScenario;
import org.apache.solr.cloud.autoscaling.sim.SimUtils;
import org.apache.solr.cloud.autoscaling.sim.SnapshotCloudManager;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.Sha256AuthenticationProvider;
import org.apache.solr.util.configuration.SSLConfigurationsFactory;
import org.noggit.CharArr;
import org.noggit.JSONParser;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.FORBIDDEN;
import static org.apache.solr.common.SolrException.ErrorCode.UNAUTHORIZED;
import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.Utils.fromJSONString;

/**
 * Command-line utility for working with Solr.
 */
public class SolrCLI {
  private static final long MAX_WAIT_FOR_CORE_LOAD_NANOS = TimeUnit.NANOSECONDS.convert(1, TimeUnit.MINUTES);

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

    protected void echoIfVerbose(final String msg, CommandLine cli) {
      if (cli.hasOption("verbose")) {
        echo(msg);
      }
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
          System.err.println("\nERROR: " + excMsg + "\n");
          if (verbose) {
            exc.printStackTrace(System.err);
          }
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
      raiseLogLevelUnlessVerbose(cli);
      String zkHost = cli.getOptionValue("zkHost", ZK_HOST);

      log.debug("Connecting to Solr cluster: {}", zkHost);
      try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(Collections.singletonList(zkHost), Optional.empty()).build()) {

        String collection = cli.getOptionValue("collection");
        if (collection != null)
          cloudSolrClient.setDefaultCollection(collection);

        cloudSolrClient.connect();
        runCloudTool(cloudSolrClient, cli);
      }
    }

    /**
     * Runs a SolrCloud tool with CloudSolrClient initialized
     */
    protected abstract void runCloudTool(CloudSolrClient cloudSolrClient, CommandLine cli)
        throws Exception;
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String DEFAULT_SOLR_URL = "http://localhost:8983/solr";
  public static final String ZK_HOST = "localhost:9983";

  public static Option[] cloudOptions =  new Option[] {
      Option.builder("zkHost")
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc("Address of the ZooKeeper ensemble; defaults to: "+ ZK_HOST + '.')
          .build(),
      Option.builder("c")
          .argName("COLLECTION")
          .hasArg()
          .required(false)
          .desc("Name of collection; no default.")
          .longOpt("collection")
          .build(),
      Option.builder("verbose")
          .required(false)
          .desc("Enable more verbose command output.")
          .build()
  };

  private static void exit(int exitStatus) {
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
      displayToolOptions();
      exit(1);
    }

    if (args.length == 1 && Arrays.asList("-v","-version","version").contains(args[0])) {
      // Simple version tool, no need for its own class
      System.out.println(Version.LATEST);
      exit(0);
    }

    SSLConfigurationsFactory.current().init();

    Tool tool = null;
    try {
      tool = findTool(args);
    } catch (IllegalArgumentException iae) {
      System.err.println(iae.getMessage());
      System.exit(1);
    }
    CommandLine cli = parseCmdLine(tool.getName(), args, tool.getOptions());
    System.exit(tool.runTool(cli));
  }

  public static Tool findTool(String[] args) throws Exception {
    String toolType = args[0].trim().toLowerCase(Locale.ROOT);
    return newTool(toolType);
  }

  /**
   * @deprecated Use the method that takes a tool name as the first argument instead.
   */
  @Deprecated
  public static CommandLine parseCmdLine(String[] args, Option[] toolOptions) throws Exception {
    return parseCmdLine(SolrCLI.class.getName(), args, toolOptions);
  }

  public static CommandLine parseCmdLine(String toolName, String[] args, Option[] toolOptions) throws Exception {
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
        processCommandLineArgs(toolName, joinCommonAndToolOptions(toolOptions), toolArgs);

    List<String> argList = cli.getArgList();
    argList.addAll(dashDList);

    // for SSL support, try to accommodate relative paths set for SSL store props
    String solrInstallDir = System.getProperty("solr.install.dir");
    if (solrInstallDir != null) {
      checkSslStoreSysProp(solrInstallDir, "keyStore");
      checkSslStoreSysProp(solrInstallDir, "trustStore");
    }

    return cli;
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

  private static void raiseLogLevelUnlessVerbose(CommandLine cli) {
    if (! cli.hasOption("verbose")) {
      StartupLoggingUtils.changeLogLevel("WARN");
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
    else if ("upconfig".equals(toolType))
      return new ConfigSetUploadTool();
    else if ("downconfig".equals(toolType))
      return new ConfigSetDownloadTool();
    else if ("rm".equals(toolType))
      return new ZkRmTool();
    else if ("mv".equals(toolType))
      return new ZkMvTool();
    else if ("cp".equals(toolType))
      return new ZkCpTool();
    else if ("ls".equals(toolType))
      return new ZkLsTool();
    else if ("mkroot".equals(toolType))
      return new ZkMkrootTool();
    else if ("assert".equals(toolType))
      return new AssertTool();
    else if ("utils".equals(toolType))
      return new UtilsTool();
    else if ("auth".equals(toolType))
      return new AuthTool();
    else if ("autoscaling".equals(toolType))
      return new AutoscalingTool();
    else if ("export".equals(toolType))
      return new ExportTool();
    else if ("package".equals(toolType))
      return new PackageTool();

    // If you add a built-in tool to this class, add it here to avoid
    // classpath scanning

    for (Class<Tool> next : findToolClassesInPackage("org.apache.solr.util")) {
      Tool tool = next.newInstance();
      if (toolType.equals(tool.getName()))
        return tool;
    }

    throw new IllegalArgumentException(toolType + " is not a valid command!");
  }

  private static void displayToolOptions() throws Exception {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("healthcheck", getToolOptions(new HealthcheckTool()));
    formatter.printHelp("status", getToolOptions(new StatusTool()));
    formatter.printHelp("api", getToolOptions(new ApiTool()));
    formatter.printHelp("autoscaling", getToolOptions(new AutoscalingTool()));
    formatter.printHelp("create_collection", getToolOptions(new CreateCollectionTool()));
    formatter.printHelp("create_core", getToolOptions(new CreateCoreTool()));
    formatter.printHelp("create", getToolOptions(new CreateTool()));
    formatter.printHelp("delete", getToolOptions(new DeleteTool()));
    formatter.printHelp("config", getToolOptions(new ConfigTool()));
    formatter.printHelp("run_example", getToolOptions(new RunExampleTool()));
    formatter.printHelp("upconfig", getToolOptions(new ConfigSetUploadTool()));
    formatter.printHelp("downconfig", getToolOptions(new ConfigSetDownloadTool()));
    formatter.printHelp("rm", getToolOptions(new ZkRmTool()));
    formatter.printHelp("cp", getToolOptions(new ZkCpTool()));
    formatter.printHelp("mv", getToolOptions(new ZkMvTool()));
    formatter.printHelp("ls", getToolOptions(new ZkLsTool()));
    formatter.printHelp("package", getToolOptions(new PackageTool()));

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
   * @deprecated Use the method that takes a tool name as the first argument instead.
   */
  @Deprecated
  public static CommandLine processCommandLineArgs(Option[] customOptions, String[] args) {
    return processCommandLineArgs(SolrCLI.class.getName(), customOptions, args);
  }

  /**
   * Parses the command-line arguments passed by the user.
   */
  public static CommandLine processCommandLineArgs(String toolName, Option[] customOptions, String[] args) {
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
      formatter.printHelp(toolName, options);
      exit(1);
    }

    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(toolName, options);
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
      ClassLoader classLoader = SolrCLI.class.getClassLoader();
      String path = packageName.replace('.', '/');
      Enumeration<URL> resources = classLoader.getResources(path);
      Set<String> classes = new TreeSet<String>();
      while (resources.hasMoreElements()) {
        URL resource = resources.nextElement();
        classes.addAll(findClasses(resource.getFile(), packageName));
      }

      for (String classInPackage : classes) {
        Class<?> theClass = Class.forName(classInPackage);
        if (Tool.class.isAssignableFrom(theClass))
          toolClasses.add((Class<Tool>) theClass);
      }
    } catch (Exception e) {
      // safe to squelch this as it's just looking for tools to run
      log.debug("Failed to find Tool impl classes in {}, due to: ", packageName, e);
    }
    return toolClasses;
  }

  private static Set<String> findClasses(String path, String packageName)
      throws Exception {
    Set<String> classes = new TreeSet<String>();
    if (path.startsWith("file:") && path.contains("!")) {
      String[] split = path.split("!");
      URL jar = new URL(split[0]);
      try (ZipInputStream zip = new ZipInputStream(jar.openStream())) {
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

  /**
   * Tries a simple HEAD request and throws SolrException in case of Authorization error
   * @param url the url to do a HEAD request to
   * @param httpClient the http client to use (make sure it has authentication optinos set)
   * @return the HTTP response code
   * @throws SolrException if auth/autz problems
   * @throws IOException if connection failure
   */
  private static int attemptHttpHead(String url, HttpClient httpClient) throws SolrException, IOException {
    HttpResponse response = httpClient.execute(new HttpHead(url), HttpClientUtil.createNewHttpClientRequestContext());
    int code = response.getStatusLine().getStatusCode();
    if (code == UNAUTHORIZED.code || code == FORBIDDEN.code) {
      throw new SolrException(SolrException.ErrorCode.getErrorCode(code),
          "Solr requires authentication for " + url + ". Please supply valid credentials. HTTP code=" + code);
    }
    return code;
  }

  private static boolean exceptionIsAuthRelated(Exception exc) {
    return (exc instanceof SolrException
        && Arrays.asList(UNAUTHORIZED.code, FORBIDDEN.code).contains(((SolrException) exc).code()));
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
        if (exceptionIsAuthRelated(exc)) {
          throw exc;
        }
        if (--attempts > 0 && checkCommunicationError(exc)) {
          if (!isFirstAttempt) // only show the log warning after the second attempt fails
            log.warn("Request to {} failed, sleeping for 5 seconds before re-trying the request ...", getUrl, exc);
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

  @SuppressWarnings("unchecked")
  private static class SolrResponseHandler implements ResponseHandler<Map<String,Object>> {
    public Map<String,Object> handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
      HttpEntity entity = response.getEntity();
      if (entity != null) {

        String respBody = EntityUtils.toString(entity);
        Object resp = null;
        try {
          resp = fromJSONString(respBody);
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
    try {
      // ensure we're requesting JSON back from Solr
      HttpGet httpGet = new HttpGet(new URIBuilder(getUrl).setParameter(CommonParams.WT, CommonParams.JSON).build());

      // make the request and get back a parsed JSON object
      Map<String, Object> json = httpClient.execute(httpGet, new SolrResponseHandler(), HttpClientUtil.createNewHttpClientRequestContext());
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
    } catch (ClientProtocolException cpe) {
      // Currently detecting authentication by string-matching the HTTP response
      // Perhaps SolrClient should have thrown an exception itself??
      if (cpe.getMessage().contains("HTTP ERROR 401") || cpe.getMessage().contentEquals("HTTP ERROR 403")) {
        int code = cpe.getMessage().contains("HTTP ERROR 401") ? 401 : 403;
        throw new SolrException(SolrException.ErrorCode.getErrorCode(code),
            "Solr requires authentication for " + getUrl + ". Please supply valid credentials. HTTP code=" + code);
      } else {
        throw cpe;
      }
    }
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
   *
   * To find a path to a child that starts with a slash (e.g. queryHandler named /query)
   * you must escape the slash. For instance /config/requestHandler/\/query/defaults/echoParams
   * would get the echoParams value for the "/query" request handler.
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
    String[] path = jsonPath.split("(?<![\\\\])/"); // Break on all slashes _not_ preceeded by a backslash
    for (int p=1; p < path.length; p++) {
      String part = path[p];

      if (part.startsWith("\\")) {
        part = part.substring(1);
      }

      Object child = parent.get(part);
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


  public static class AutoscalingTool extends ToolBase {

    public AutoscalingTool() {
      this(System.out);
    }

    public AutoscalingTool(PrintStream stdout) {
      super(stdout);
    }

    @Override
    public Option[] getOptions() {
      return new Option[] {
          Option.builder("zkHost")
              .argName("HOST")
              .hasArg()
              .required(false)
              .desc("Address of the Zookeeper ensemble; defaults to: "+ZK_HOST)
              .build(),
          Option.builder("a")
              .argName("CONFIG")
              .hasArg()
              .required(false)
              .desc("Autoscaling config file, defaults to the one deployed in the cluster.")
              .longOpt("config")
              .build(),
          Option.builder("s")
              .desc("Show calculated suggestions")
              .longOpt("suggestions")
              .build(),
          Option.builder("c")
              .desc("Show ClusterState (collections layout)")
              .longOpt("clusterState")
              .build(),
          Option.builder("d")
              .desc("Show calculated diagnostics")
              .longOpt("diagnostics")
              .build(),
          Option.builder("n")
              .desc("Show sorted nodes with diagnostics")
              .longOpt("sortedNodes")
              .build(),
          Option.builder("r")
              .desc("Redact node and collection names (original names will be consistently randomized)")
              .longOpt("redact")
              .build(),
          Option.builder("stats")
              .desc("Show summarized collection & node statistics.")
              .build(),
          Option.builder("save")
              .desc("Store autoscaling snapshot of the current cluster.")
              .argName("DIR")
              .hasArg()
              .build(),
          Option.builder("load")
              .desc("Load autoscaling snapshot of the cluster instead of using the real one.")
              .argName("DIR")
              .hasArg()
              .build(),
          Option.builder("simulate")
              .desc("Simulate execution of all suggestions.")
              .build(),
          Option.builder("i")
              .desc("Max number of simulation iterations.")
              .argName("NUMBER")
              .hasArg()
              .longOpt("iterations")
              .build(),
          Option.builder("ss")
              .desc("Save autoscaling snapshots at each step of simulated execution.")
              .argName("DIR")
              .longOpt("saveSimulated")
              .hasArg()
              .build(),
          Option.builder("scenario")
              .desc("Execute a scenario from a file (and ignore all other options).")
              .argName("FILE")
              .hasArg()
              .build(),
          Option.builder("all")
              .desc("Turn on all options to get all available information.")
              .build()

      };
    }

    @Override
    public String getName() {
      return "autoscaling";
    }

    protected void runImpl(CommandLine cli) throws Exception {
      raiseLogLevelUnlessVerbose(cli);
      if (cli.hasOption("scenario")) {
        String data = IOUtils.toString(new FileInputStream(cli.getOptionValue("scenario")), "UTF-8");
        try (SimScenario scenario = SimScenario.load(data)) {
          scenario.verbose = verbose;
          scenario.console = System.err;
          scenario.run();
        }
        return;
      }
      SnapshotCloudManager cloudManager;
      AutoScalingConfig config = null;
      String configFile = cli.getOptionValue("a");
      if (configFile != null) {
        System.err.println("- reading autoscaling config from " + configFile);
        config = new AutoScalingConfig(IOUtils.toByteArray(new FileInputStream(configFile)));
      }
      if (cli.hasOption("load")) {
        File sourceDir = new File(cli.getOptionValue("load"));
        System.err.println("- loading autoscaling snapshot from " + sourceDir.getAbsolutePath());
        cloudManager = SnapshotCloudManager.readSnapshot(sourceDir);
        if (config == null) {
          System.err.println("- reading autoscaling config from the snapshot.");
          config = cloudManager.getDistribStateManager().getAutoScalingConfig();
        }
      } else {
        String zkHost = cli.getOptionValue("zkHost", ZK_HOST);

        log.debug("Connecting to Solr cluster: {}", zkHost);
        try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(Collections.singletonList(zkHost), Optional.empty()).build()) {

          String collection = cli.getOptionValue("collection");
          if (collection != null)
            cloudSolrClient.setDefaultCollection(collection);

          cloudSolrClient.connect();
          try (SolrClientCloudManager realCloudManager = new SolrClientCloudManager(NoopDistributedQueueFactory.INSTANCE, cloudSolrClient)) {
            if (config == null) {
              System.err.println("- reading autoscaling config from the cluster.");
              config = realCloudManager.getDistribStateManager().getAutoScalingConfig();
            }
            cloudManager = new SnapshotCloudManager(realCloudManager, config);
          }
        }
      }
      boolean redact = cli.hasOption("r");
      if (cli.hasOption("save")) {
        File targetDir = new File(cli.getOptionValue("save"));
        cloudManager.saveSnapshot(targetDir, true, redact);
        System.err.println("- saved autoscaling snapshot to " + targetDir.getAbsolutePath());
      }
      HashSet<String> liveNodes = new HashSet<>(cloudManager.getClusterStateProvider().getLiveNodes());
      boolean withSuggestions = cli.hasOption("s");
      boolean withDiagnostics = cli.hasOption("d") || cli.hasOption("n");
      boolean withSortedNodes = cli.hasOption("n");
      boolean withClusterState = cli.hasOption("c");
      boolean withStats = cli.hasOption("stats");
      if (cli.hasOption("all")) {
        withSuggestions = true;
        withDiagnostics = true;
        withSortedNodes = true;
        withClusterState = true;
        withStats = true;
      }
      // prepare to redact also host names / IPs in base_url and other properties
      ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
      RedactionUtils.RedactionContext ctx = null;
      if (redact) {
        ctx = SimUtils.getRedactionContext(clusterState);
      }
      if (!withSuggestions && !withDiagnostics) {
        withSuggestions = true;
      }
      Map<String, Object> results = prepareResults(cloudManager, config, withClusterState,
          withStats, withSuggestions, withSortedNodes, withDiagnostics);
      if (cli.hasOption("simulate")) {
        String iterStr = cli.getOptionValue("i", "10");
        String saveSimulated = cli.getOptionValue("saveSimulated");
        int iterations;
        try {
          iterations = Integer.parseInt(iterStr);
        } catch (Exception e) {
          log.warn("Invalid option 'i' value, using default 10:", e);
          iterations = 10;
        }
        Map<String, Object> simulationResults = new HashMap<>();
        simulate(cloudManager, config, simulationResults, saveSimulated, withClusterState,
            withStats, withSuggestions, withSortedNodes, withDiagnostics, iterations, redact);
        results.put("simulation", simulationResults);
      }
      String data = Utils.toJSONString(results);
      if (redact) {
        data = RedactionUtils.redactNames(ctx.getRedactions(), data);
      }
      stdout.println(data);
    }

    private Map<String, Object> prepareResults(SolrCloudManager clientCloudManager,
                                               AutoScalingConfig config,
                                               boolean withClusterState,
                                               boolean withStats,
                                               boolean withSuggestions,
                                               boolean withSortedNodes,
                                               boolean withDiagnostics) throws Exception {
      Policy.Session session = config.getPolicy().createSession(clientCloudManager);
      ClusterState clusterState = clientCloudManager.getClusterStateProvider().getClusterState();
      List<Suggester.SuggestionInfo> suggestions = Collections.emptyList();
      long start, end;
      if (withSuggestions) {
        System.err.println("- calculating suggestions...");
        start = TimeSource.NANO_TIME.getTimeNs();
        suggestions = PolicyHelper.getSuggestions(config, clientCloudManager);
        end = TimeSource.NANO_TIME.getTimeNs();
        System.err.println("  (took " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms)");
      }
      Map<String, Object> diagnostics = Collections.emptyMap();
      if (withDiagnostics) {
        System.err.println("- calculating diagnostics...");
        start = TimeSource.NANO_TIME.getTimeNs();
        MapWriter mw = PolicyHelper.getDiagnostics(session);
        diagnostics = new LinkedHashMap<>();
        mw.toMap(diagnostics);
        end = TimeSource.NANO_TIME.getTimeNs();
        System.err.println("  (took " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms)");
      }
      Map<String, Object> results = new LinkedHashMap<>();
      if (withClusterState) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("znodeVersion", clusterState.getZNodeVersion());
        map.put("liveNodes", new TreeSet<>(clusterState.getLiveNodes()));
        map.put("collections", clusterState.getCollectionsMap());
        results.put("CLUSTERSTATE", map);
      }
      if (withStats) {
        results.put("STATISTICS", SimUtils.calculateStats(clientCloudManager, config, verbose));
      }
      if (withSuggestions) {
        results.put("SUGGESTIONS", suggestions);
      }
      if (!withSortedNodes) {
        diagnostics.remove("sortedNodes");
      }
      if (withDiagnostics) {
        results.put("DIAGNOSTICS", diagnostics);
      }
      return results;
    }


    private void simulate(SolrCloudManager cloudManager,
                          AutoScalingConfig config,
                          Map<String, Object> results,
                          String saveSimulated,
                          boolean withClusterState,
                          boolean withStats,
                          boolean withSuggestions,
                          boolean withSortedNodes,
                          boolean withDiagnostics, int iterations, boolean redact) throws Exception {
      File saveDir = null;
      if (saveSimulated != null) {
        saveDir = new File(saveSimulated);
        if (!saveDir.exists()) {
          if (!saveDir.mkdirs()) {
            throw new Exception("Unable to create 'saveSimulated' directory: " + saveDir.getAbsolutePath());
          }
        } else if (!saveDir.isDirectory()) {
          throw new Exception("'saveSimulated' path exists and is not a directory! " + saveDir.getAbsolutePath());
        }
      }
      int SPEED = 50;
      SimCloudManager simCloudManager = SimCloudManager.createCluster(cloudManager, config, TimeSource.get("simTime:" + SPEED));
      int loop = 0;
      List<Suggester.SuggestionInfo> suggestions = Collections.emptyList();
      Map<String, Object> intermediate = new LinkedHashMap<>();
      results.put("intermediate", intermediate);
      while (loop < iterations) {
        LinkedHashMap<String, Object> perStep = new LinkedHashMap<>();
        long start = TimeSource.NANO_TIME.getTimeNs();
        suggestions = PolicyHelper.getSuggestions(config, simCloudManager);
        System.err.println("-- step " + loop + ", " + suggestions.size() + " suggestions.");
        long end = TimeSource.NANO_TIME.getTimeNs();
        System.err.println("   - calculated in " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms (real time ≈ simulated time)");
        if (suggestions.isEmpty()) {
          break;
        }
        SnapshotCloudManager snapshotCloudManager = new SnapshotCloudManager(simCloudManager, config);
        if (saveDir != null) {
          File target = new File(saveDir, "step" + loop + "_start");
          snapshotCloudManager.saveSnapshot(target, true, redact);
        }
        if (verbose) {
          Map<String, Object> snapshot = snapshotCloudManager.getSnapshot(false, redact);
          snapshot.remove(SnapshotCloudManager.DISTRIB_STATE_KEY);
          snapshot.remove(SnapshotCloudManager.MANAGER_STATE_KEY);
          perStep.put("snapshotStart", snapshot);
        }
        intermediate.put("step" + loop, perStep);
        int unresolvedCount = 0;
        start = TimeSource.NANO_TIME.getTimeNs();
        List<Map<String, Object>> perStepOps = new ArrayList<>(suggestions.size());
        if (withSuggestions) {
          perStep.put("suggestions", suggestions);
          perStep.put("opDetails", perStepOps);
        }
        for (Suggester.SuggestionInfo suggestion : suggestions) {
          SolrRequest<?> operation = suggestion.getOperation();
          if (operation == null) {
            unresolvedCount++;
            if (suggestion.getViolation() == null) {
              System.err.println("   - ignoring suggestion without violation and without operation: " + suggestion);
            }
            continue;
          }
          SolrParams params = operation.getParams();
          if (operation instanceof V2Request) {
            params = SimUtils.v2AdminRequestToV1Params((V2Request)operation);
          }
          Map<String, Object> paramsMap = new LinkedHashMap<>();
          params.toMap(paramsMap);
          ReplicaInfo info = simCloudManager.getSimClusterStateProvider().simGetReplicaInfo(
              params.get(CollectionAdminParams.COLLECTION), params.get("replica"));
          if (info == null) {
            System.err.println("Could not find ReplicaInfo for params: " + params);
          } else if (verbose) {
            paramsMap.put("replicaInfo", info);
          } else if (info.getVariable(Variable.Type.CORE_IDX.tagName) != null) {
            paramsMap.put(Variable.Type.CORE_IDX.tagName, info.getVariable(Variable.Type.CORE_IDX.tagName));
          }
          if (withSuggestions) {
            perStepOps.add(paramsMap);
          }
          try {
            simCloudManager.request(operation);
          } catch (Exception e) {
            System.err.println("Aborting - error executing suggestion " + suggestion + ": " + e);
            Map<String, Object> error = new HashMap<>();
            error.put("suggestion", suggestion);
            error.put("replicaInfo", info);
            error.put("exception", e);
            perStep.put("error", error);
            break;
          }
        }
        end = TimeSource.NANO_TIME.getTimeNs();
        long realTime = TimeUnit.NANOSECONDS.toMillis(end - start);
        long simTime = realTime * SPEED;
        System.err.println("   - executed in " + realTime + " ms (real time), " + simTime + " ms (simulated time)");
        if (unresolvedCount == suggestions.size()) {
          System.err.println("--- aborting simulation, only unresolved violations remain");
          break;
        }
        if (withStats) {
          perStep.put("statsExecutionStop", SimUtils.calculateStats(simCloudManager, config, verbose));
        }
        snapshotCloudManager = new SnapshotCloudManager(simCloudManager, config);
        if (saveDir != null) {
          File target = new File(saveDir, "step" + loop + "_stop");
          snapshotCloudManager.saveSnapshot(target, true, redact);
        }
        if (verbose) {
          Map<String, Object> snapshot = snapshotCloudManager.getSnapshot(false, redact);
          snapshot.remove(SnapshotCloudManager.DISTRIB_STATE_KEY);
          snapshot.remove(SnapshotCloudManager.MANAGER_STATE_KEY);
          perStep.put("snapshotStop", snapshot);
        }
        loop++;
      }
      if (loop == iterations && !suggestions.isEmpty()) {
        System.err.println("### Failed to apply all suggestions in " + iterations + " steps. Remaining suggestions: " + suggestions + "\n");
      }
      results.put("finalState", prepareResults(simCloudManager, config, withClusterState, withStats,
          withSuggestions, withSortedNodes, withDiagnostics));
    }
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

    public Option[] getOptions() {
      return new Option[] {
          Option.builder("solr")
              .argName("URL")
              .hasArg()
              .required(false)
              .desc("Address of the Solr Web application, defaults to: "+ DEFAULT_SOLR_URL + '.')
              .build(),
          Option.builder("maxWaitSecs")
              .argName("SECS")
              .hasArg()
              .required(false)
              .desc("Wait up to the specified number of seconds to see Solr running.")
              .build()
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
          if (exceptionIsAuthRelated(exc)) {
            throw exc;
          }
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
        } catch (SSLPeerUnverifiedException exc) {
          throw exc;
        } catch (Exception exc) {
          if (exceptionIsAuthRelated(exc)) {
            throw exc;
          }
          try {
            Thread.sleep(2000L);
          } catch (InterruptedException interrupted) {
            timeout = 0; // stop looping
          }
        }
      }
      throw new TimeoutException("Did not see Solr at "+solrUrl+" come online within "+maxWaitSecs+" seconds!");
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

    public Option[] getOptions() {
      return new Option[] {
          Option.builder("get")
              .argName("URL")
              .hasArg()
              .required(true)
              .desc("Send a GET request to a Solr API endpoint.")
              .build()
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

  private static final String DEFAULT_CONFIG_SET = "_default";

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
      raiseLogLevelUnlessVerbose(cli);
      String collection = cli.getOptionValue("collection");
      if (collection == null)
        throw new IllegalArgumentException("Must provide a collection to run a healthcheck against!");

      log.debug("Running healthcheck for {}", collection);

      ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();

      ClusterState clusterState = zkStateReader.getClusterState();
      Set<String> liveNodes = clusterState.getLiveNodes();
      final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
      if (docCollection == null || docCollection.getSlices() == null)
        throw new IllegalArgumentException("Collection "+collection+" not found!");

      Collection<Slice> slices = docCollection.getSlices();
      // Test http code using a HEAD request first, fail fast if authentication failure
      String urlForColl = zkStateReader.getLeaderUrl(collection, slices.stream().findFirst().get().getName(), 1000);
      attemptHttpHead(urlForColl, cloudSolrClient.getHttpClient());

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
          log.warn("Failed to get leader for shard {} due to: {}", shardName, exc);
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
            q.set(DISTRIB, "false");
            try (HttpSolrClient solr = new HttpSolrClient.Builder(coreUrl).build()) {

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
              log.error("ERROR: {} when trying to reach: {}", exc, coreUrl);

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
      Option.builder("zkHost")
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc("Address of the ZooKeeper ensemble; defaults to: " + ZK_HOST + '.')
          .build(),
      Option.builder("solrUrl")
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc("Base Solr URL, which can be used to determine the zkHost if that's not known.")
          .build(),
      Option.builder(NAME)
          .argName("NAME")
          .hasArg()
          .required(true)
          .desc("Name of collection to create.")
          .build(),
      Option.builder("shards")
          .argName("#")
          .hasArg()
          .required(false)
          .desc("Number of shards; default is 1.")
          .build(),
      Option.builder("replicationFactor")
          .argName("#")
          .hasArg()
          .required(false)
          .desc("Number of copies of each document across the collection (replicas per shard); default is 1.")
          .build(),
      Option.builder("maxShardsPerNode")
          .argName("#")
          .hasArg()
          .required(false)
          .desc("Maximum number of shards per Solr node; default is determined based on the number of shards, replication factor, and live nodes.")
          .build(),
      Option.builder("confdir")
          .argName("NAME")
          .hasArg()
          .required(false)
          .desc("Configuration directory to copy when creating the new collection; default is "+ DEFAULT_CONFIG_SET + '.')
          .build(),
      Option.builder("confname")
          .argName("NAME")
          .hasArg()
          .required(false)
          .desc("Configuration name; default is the collection name.")
          .build(),
      Option.builder("configsetsDir")
          .argName("DIR")
          .hasArg()
          .required(true)
          .desc("Path to configsets directory on the local system.")
          .build(),
      Option.builder("verbose")
          .required(false)
          .desc("Enable more verbose command output.")
          .build()

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

      try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(Collections.singletonList(zkHost), Optional.empty()).build()) {
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
      @SuppressWarnings("unchecked")
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
      @SuppressWarnings("unchecked")
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
      boolean wait = false;
      final long startWaitAt = System.nanoTime();
      do{
        if (wait) {
          final int clamPeriodForStatusPollMs = 1000;
          Thread.sleep(clamPeriodForStatusPollMs);
        }
        Map<String,Object> existsCheckResult = getJson(coreStatusUrl);
        @SuppressWarnings("unchecked")
        Map<String,Object> status = (Map<String, Object>)existsCheckResult.get("status");
        @SuppressWarnings("unchecked")
        Map<String,Object> coreStatus = (Map<String, Object>)status.get(coreName);
        @SuppressWarnings("unchecked")
        Map<String,Object> failureStatus = (Map<String, Object>)existsCheckResult.get("initFailures");
        String errorMsg = (String) failureStatus.get(coreName);
        final boolean hasName = coreStatus != null && coreStatus.containsKey(NAME);
        exists = hasName || errorMsg != null;
        wait = hasName && errorMsg==null && "true".equals(coreStatus.get("isLoading"));
      }while (wait &&
          System.nanoTime() - startWaitAt < MAX_WAIT_FOR_CORE_LOAD_NANOS);
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

    public Option[] getOptions() {
      return CREATE_COLLECTION_OPTIONS;
    }



    protected void runImpl(CommandLine cli) throws Exception {
      raiseLogLevelUnlessVerbose(cli);
      String zkHost = getZkHost(cli);
      if (zkHost == null) {
        throw new IllegalStateException("Solr at "+cli.getOptionValue("solrUrl")+
            " is running in standalone server mode, please use the create_core command instead;\n" +
            "create_collection can only be used when running in SolrCloud mode.\n");
      }

      try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(Collections.singletonList(zkHost), Optional.empty()).build()) {
        echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost+" ...", cli);
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
      }

      String confname = cli.getOptionValue("confname");
      String confdir = cli.getOptionValue("confdir");
      String configsetsDir = cli.getOptionValue("configsetsDir");

      boolean configExistsInZk = confname != null && !"".equals(confname.trim()) &&
          cloudSolrClient.getZkStateReader().getZkClient().exists("/configs/" + confname, true);

      if (CollectionAdminParams.SYSTEM_COLL.equals(collectionName)) {
        //do nothing
      } else if (configExistsInZk) {
        echo("Re-using existing configuration directory "+confname);
      } else if (confdir != null && !"".equals(confdir.trim())){
        if (confname == null || "".equals(confname.trim())) {
          confname = collectionName;
        }
        Path confPath = ZkConfigManager.getConfigsetPath(confdir,
            configsetsDir);

        echoIfVerbose("Uploading " + confPath.toAbsolutePath().toString() +
            " for config " + confname + " to ZooKeeper at " + cloudSolrClient.getZkHost(), cli);
        ((ZkClientClusterStateProvider) cloudSolrClient.getClusterStateProvider()).uploadConfig(confPath, confname);
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
              "%s/admin/collections?action=CREATE&name=%s&numShards=%d&replicationFactor=%d&maxShardsPerNode=%d",
              baseUrl,
              collectionName,
              numShards,
              replicationFactor,
              maxShardsPerNode);
      if (confname != null && !"".equals(confname.trim())) {
        createCollectionUrl = createCollectionUrl + String.format(Locale.ROOT, "&collection.configName=%s", confname);
      }

      echoIfVerbose("\nCreating new collection '"+collectionName+"' using command:\n"+createCollectionUrl+"\n", cli);

      Map<String,Object> json = null;
      try {
        json = getJson(createCollectionUrl);
      } catch (SolrServerException sse) {
        throw new Exception("Failed to create collection '"+collectionName+"' due to: "+sse.getMessage());
      }

      if (cli.hasOption("verbose")) {
        CharArr arr = new CharArr();
        new JSONWriter(arr, 2).write(json);
        echo(arr.toString());
      } else {
        String endMessage = String.format(Locale.ROOT, "Created collection '%s' with %d shard(s), %d replica(s)",
            collectionName, numShards, replicationFactor);
        if (confname != null && !"".equals(confname.trim())) {
          endMessage += String.format(Locale.ROOT, " with config-set '%s'", confname);
        }

        echo(endMessage);
      }
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

    public Option[] getOptions() {
      return new Option[] {
          Option.builder("solrUrl")
              .argName("URL")
              .hasArg()
              .required(false)
              .desc("Base Solr URL, default is " + DEFAULT_SOLR_URL + '.')
              .build(),
          Option.builder(NAME)
              .argName("NAME")
              .hasArg()
              .required(true)
              .desc("Name of the core to create.")
              .build(),
          Option.builder("confdir")
              .argName("CONFIG")
              .hasArg()
              .required(false)
              .desc("Configuration directory to copy when creating the new core; default is "+ DEFAULT_CONFIG_SET + '.')
              .build(),
          Option.builder("configsetsDir")
              .argName("DIR")
              .hasArg()
              .required(true)
              .desc("Path to configsets directory on the local system.")
              .build(),
          Option.builder("verbose")
              .required(false)
              .desc("Enable more verbose command output.")
              .build()
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
        echoIfVerbose("\nCopying configuration to new core instance directory:\n" + coreInstanceDir.getAbsolutePath(), cli);
      }

      String createCoreUrl =
          String.format(Locale.ROOT,
              "%sadmin/cores?action=CREATE&name=%s&instanceDir=%s",
              solrUrl,
              coreName,
              coreName);

      echoIfVerbose("\nCreating new core '" + coreName + "' using command:\n" + createCoreUrl + "\n", cli);

      try {
        Map<String,Object> json = getJson(createCoreUrl);
        if (cli.hasOption("verbose")) {
          CharArr arr = new CharArr();
          new JSONWriter(arr, 2).write(json);
          echo(arr.toString());
          echo("\n");
        } else {
          echo(String.format(Locale.ROOT, "\nCreated new core '%s'", coreName));
        }
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

    public Option[] getOptions() {
      return CREATE_COLLECTION_OPTIONS;
    }

    protected void runImpl(CommandLine cli) throws Exception {
      raiseLogLevelUnlessVerbose(cli);
      String solrUrl = cli.getOptionValue("solrUrl", DEFAULT_SOLR_URL);
      if (!solrUrl.endsWith("/"))
        solrUrl += "/";

      String systemInfoUrl = solrUrl+"admin/info/system";
      CloseableHttpClient httpClient = getHttpClient();

      ToolBase tool = null;
      try {
        Map<String, Object> systemInfo = getJson(httpClient, systemInfoUrl, 2, true);
        if ("solrcloud".equals(systemInfo.get("mode"))) {
          tool = new CreateCollectionTool(stdout);
        } else {
          tool = new CreateCoreTool(stdout);
        }
        tool.runImpl(cli);
      } finally {
        closeHttpClient(httpClient);
      }
    }

  } // end CreateTool class

  public static class ConfigSetUploadTool extends ToolBase {

    public ConfigSetUploadTool() {
      this(System.out);
    }

    public ConfigSetUploadTool(PrintStream stdout) {
      super(stdout);
    }

    public Option[] getOptions() {
      return new Option[]{
          Option.builder("confname")
              .argName("confname") // Comes out in help message
              .hasArg() // Has one sub-argument
              .required(true) // confname argument must be present
              .desc("Configset name in ZooKeeper.")
              .build(), // passed as -confname value
          Option.builder("confdir")
              .argName("confdir")
              .hasArg()
              .required(true)
              .desc("Local directory with configs.")
              .build(),
          Option.builder("configsetsDir")
              .argName("configsetsDir")
              .hasArg()
              .required(false)
              .desc("Parent directory of example configsets.")
              .build(),
          Option.builder("zkHost")
              .argName("HOST")
              .hasArg()
              .required(true)
              .desc("Address of the ZooKeeper ensemble; defaults to: " + ZK_HOST + '.')
              .build(),
          Option.builder("verbose")
              .required(false)
              .desc("Enable more verbose command output.")
              .build()
      };
    }


    public String getName() {
      return "upconfig";
    }

    protected void runImpl(CommandLine cli) throws Exception {
      raiseLogLevelUnlessVerbose(cli);
      String zkHost = getZkHost(cli);
      if (zkHost == null) {
        throw new IllegalStateException("Solr at " + cli.getOptionValue("solrUrl") +
            " is running in standalone server mode, upconfig can only be used when running in SolrCloud mode.\n");
      }

      String confName = cli.getOptionValue("confname");
      try (SolrZkClient zkClient = new SolrZkClient(zkHost, 30000)) {
        echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
        Path confPath = ZkConfigManager.getConfigsetPath(cli.getOptionValue("confdir"), cli.getOptionValue("configsetsDir"));

        echo("Uploading " + confPath.toAbsolutePath().toString() +
            " for config " + cli.getOptionValue("confname") + " to ZooKeeper at " + zkHost);

        zkClient.upConfig(confPath, confName);
      } catch (Exception e) {
        log.error("Could not complete upconfig operation for reason: ", e);
        throw (e);
      }
    }
  }

  public static class ConfigSetDownloadTool extends ToolBase {

    public ConfigSetDownloadTool() {
      this(System.out);
    }

    public ConfigSetDownloadTool(PrintStream stdout) {
      super(stdout);
    }

    public Option[] getOptions() {
      return new Option[]{
          Option.builder("confname")
              .argName("confname")
              .hasArg()
              .required(true)
              .desc("Configset name in ZooKeeper.")
              .build(),
          Option.builder("confdir")
              .argName("confdir")
              .hasArg()
              .required(true)
              .desc("Local directory with configs.")
              .build(),
          Option.builder("zkHost")
              .argName("HOST")
              .hasArg()
              .required(true)
              .desc("Address of the ZooKeeper ensemble; defaults to: " + ZK_HOST + '.')
              .build(),
          Option.builder("verbose")
              .required(false)
              .desc("Enable more verbose command output.")
              .build()
      };
    }

    public String getName() {
      return "downconfig";
    }

    protected void runImpl(CommandLine cli) throws Exception {
      raiseLogLevelUnlessVerbose(cli);
      String zkHost = getZkHost(cli);
      if (zkHost == null) {
        throw new IllegalStateException("Solr at " + cli.getOptionValue("solrUrl") +
            " is running in standalone server mode, downconfig can only be used when running in SolrCloud mode.\n");
      }


      try (SolrZkClient zkClient = new SolrZkClient(zkHost, 30000)) {
        echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
        String confName = cli.getOptionValue("confname");
        String confDir = cli.getOptionValue("confdir");
        Path configSetPath = Paths.get(confDir);
        // we try to be nice about having the "conf" in the directory, and we create it if it's not there.
        if (configSetPath.endsWith("/conf") == false) {
          configSetPath = Paths.get(configSetPath.toString(), "conf");
        }
        if (Files.exists(configSetPath) == false) {
          Files.createDirectories(configSetPath);
        }
        echo("Downloading configset " + confName + " from ZooKeeper at " + zkHost +
            " to directory " + configSetPath.toAbsolutePath());

        zkClient.downConfig(confName, configSetPath);
      } catch (Exception e) {
        log.error("Could not complete downconfig operation for reason: ", e);
        throw (e);
      }

    }

  } // End ConfigSetDownloadTool class

  public static class ZkRmTool extends ToolBase {

    public ZkRmTool() {
      this(System.out);
    }

    public ZkRmTool(PrintStream stdout) {
      super(stdout);
    }

    public Option[] getOptions() {
      return new Option[]{
          Option.builder("path")
              .argName("path")
              .hasArg()
              .required(true)
              .desc("Path to remove.")
              .build(),
          Option.builder("recurse")
              .argName("recurse")
              .hasArg()
              .required(false)
              .desc("Recurse (true|false), default is false.")
              .build(),
          Option.builder("zkHost")
              .argName("HOST")
              .hasArg()
              .required(true)
              .desc("Address of the ZooKeeper ensemble; defaults to: " + ZK_HOST + '.')
              .build(),
          Option.builder("verbose")
              .required(false)
              .desc("Enable more verbose command output.")
              .build()
      };
    }

    public String getName() {
      return "rm";
    }

    protected void runImpl(CommandLine cli) throws Exception {
      raiseLogLevelUnlessVerbose(cli);
      String zkHost = getZkHost(cli);

      if (zkHost == null) {
        throw new IllegalStateException("Solr at " + cli.getOptionValue("zkHost") +
            " is running in standalone server mode, 'zk rm' can only be used when running in SolrCloud mode.\n");
      }
      String target = cli.getOptionValue("path");
      Boolean recurse = Boolean.parseBoolean(cli.getOptionValue("recurse"));

      String znode = target;
      if (target.toLowerCase(Locale.ROOT).startsWith("zk:")) {
        znode = target.substring(3);
      }
      if (znode.equals("/")) {
        throw new SolrServerException("You may not remove the root ZK node ('/')!");
      }
      echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
      try (SolrZkClient zkClient = new SolrZkClient(zkHost, 30000)) {
        if (recurse == false && zkClient.getChildren(znode, null, true).size() != 0) {
          throw new SolrServerException("ZooKeeper node " + znode + " has children and recurse has NOT been specified.");
        }
        echo("Removing ZooKeeper node " + znode + " from ZooKeeper at " + zkHost +
            " recurse: " + Boolean.toString(recurse));
        zkClient.clean(znode);
      } catch (Exception e) {
        log.error("Could not complete rm operation for reason: ", e);
        throw (e);
      }

    }

  } // End RmTool class

  public static class ZkLsTool extends ToolBase {

    public ZkLsTool() {
      this(System.out);
    }

    public ZkLsTool(PrintStream stdout) {
      super(stdout);
    }

    public Option[] getOptions() {
      return new Option[]{
          Option.builder("path")
              .argName("path")
              .hasArg()
              .required(true)
              .desc("Path to list.")
              .build(),
          Option.builder("recurse")
              .argName("recurse")
              .hasArg()
              .required(false)
              .desc("Recurse (true|false), default is false.")
              .build(),
          Option.builder("zkHost")
              .argName("HOST")
              .hasArg()
              .required(true)
              .desc("Address of the ZooKeeper ensemble; defaults to: " + ZK_HOST + '.')
              .build(),
          Option.builder("verbose")
              .required(false)
              .desc("Enable more verbose command output.")
              .build()
      };
    }

    public String getName() {
      return "ls";
    }

    protected void runImpl(CommandLine cli) throws Exception {
      raiseLogLevelUnlessVerbose(cli);
      String zkHost = getZkHost(cli);

      if (zkHost == null) {
        throw new IllegalStateException("Solr at " + cli.getOptionValue("zkHost") +
            " is running in standalone server mode, 'zk ls' can only be used when running in SolrCloud mode.\n");
      }


      try (SolrZkClient zkClient = new SolrZkClient(zkHost, 30000)) {
        echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);

        String znode = cli.getOptionValue("path");
        Boolean recurse = Boolean.parseBoolean(cli.getOptionValue("recurse"));
        echoIfVerbose("Getting listing for ZooKeeper node " + znode + " from ZooKeeper at " + zkHost +
            " recurse: " + Boolean.toString(recurse), cli);
        stdout.print(zkClient.listZnode(znode, recurse));
      } catch (Exception e) {
        log.error("Could not complete ls operation for reason: ", e);
        throw (e);
      }
    }
  } // End zkLsTool class


  public static class ZkMkrootTool extends ToolBase {

    public ZkMkrootTool() {
      this(System.out);
    }

    public ZkMkrootTool(PrintStream stdout) {
      super(stdout);
    }

    public Option[] getOptions() {
      return new Option[]{
          Option.builder("path")
              .argName("path")
              .hasArg()
              .required(true)
              .desc("Path to create.")
              .build(),
          Option.builder("zkHost")
              .argName("HOST")
              .hasArg()
              .required(true)
              .desc("Address of the ZooKeeper ensemble; defaults to: " + ZK_HOST + '.')
              .build(),
          Option.builder("verbose")
              .required(false)
              .desc("Enable more verbose command output.")
              .build()
      };
    }

    public String getName() {
      return "mkroot";
    }

    protected void runImpl(CommandLine cli) throws Exception {
      raiseLogLevelUnlessVerbose(cli);
      String zkHost = getZkHost(cli);

      if (zkHost == null) {
        throw new IllegalStateException("Solr at " + cli.getOptionValue("zkHost") +
            " is running in standalone server mode, 'zk mkroot' can only be used when running in SolrCloud mode.\n");
      }


      try (SolrZkClient zkClient = new SolrZkClient(zkHost, 30000)) {
        echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);

        String znode = cli.getOptionValue("path");
        echo("Creating ZooKeeper path " + znode + " on ZooKeeper at " + zkHost);
        zkClient.makePath(znode, true);
      } catch (Exception e) {
        log.error("Could not complete mkroot operation for reason: ", e);
        throw (e);
      }
    }
  } // End zkMkrootTool class




  public static class ZkCpTool extends ToolBase {

    public ZkCpTool() {
      this(System.out);
    }

    public ZkCpTool(PrintStream stdout) {
      super(stdout);
    }

    public Option[] getOptions() {
      return new Option[]{
          Option.builder("src")
              .argName("src")
              .hasArg()
              .required(true)
              .desc("Source file or directory, may be local or a Znode.")
              .build(),
          Option.builder("dst")
              .argName("dst")
              .hasArg()
              .required(true)
              .desc("Destination of copy, may be local or a Znode.")
              .build(),
          Option.builder("recurse")
              .argName("recurse")
              .hasArg()
              .required(false)
              .desc("Recurse (true|false), default is false.")
              .build(),
          Option.builder("zkHost")
              .argName("HOST")
              .hasArg()
              .required(true)
              .desc("Address of the ZooKeeper ensemble; defaults to: " + ZK_HOST + '.')
              .build(),
          Option.builder("verbose")
              .required(false)
              .desc("Enable more verbose command output.")
              .build()
      };
    }

    public String getName() {
      return "cp";
    }

    protected void runImpl(CommandLine cli) throws Exception {
      raiseLogLevelUnlessVerbose(cli);
      String zkHost = getZkHost(cli);
      if (zkHost == null) {
        throw new IllegalStateException("Solr at " + cli.getOptionValue("solrUrl") +
            " is running in standalone server mode, cp can only be used when running in SolrCloud mode.\n");
      }

      try (SolrZkClient zkClient = new SolrZkClient(zkHost, 30000)) {
        echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
        String src = cli.getOptionValue("src");
        String dst = cli.getOptionValue("dst");
        Boolean recurse = Boolean.parseBoolean(cli.getOptionValue("recurse"));
        echo("Copying from '" + src + "' to '" + dst + "'. ZooKeeper at " + zkHost);

        boolean srcIsZk = src.toLowerCase(Locale.ROOT).startsWith("zk:");
        boolean dstIsZk = dst.toLowerCase(Locale.ROOT).startsWith("zk:");

        String srcName = src;
        if (srcIsZk) {
          srcName = src.substring(3);
        } else if (srcName.toLowerCase(Locale.ROOT).startsWith("file:")) {
          srcName = srcName.substring(5);
        }

        String dstName = dst;
        if (dstIsZk) {
          dstName = dst.substring(3);
        } else {
          if (dstName.toLowerCase(Locale.ROOT).startsWith("file:")) {
            dstName = dstName.substring(5);
          }
        }
        zkClient.zkTransfer(srcName, srcIsZk, dstName, dstIsZk, recurse);
      } catch (Exception e) {
        log.error("Could not complete the zk operation for reason: ", e);
        throw (e);
      }
    }
  } // End CpTool class


  public static class ZkMvTool extends ToolBase {

    public ZkMvTool() {
      this(System.out);
    }

    public ZkMvTool(PrintStream stdout) {
      super(stdout);
    }

    public Option[] getOptions() {
      return new Option[]{
          Option.builder("src")
              .argName("src")
              .hasArg()
              .required(true)
              .desc("Source Znode to move from.")
              .build(),
          Option.builder("dst")
              .argName("dst")
              .hasArg()
              .required(true)
              .desc("Destination Znode to move to.")
              .build(),
          Option.builder("zkHost")
              .argName("HOST")
              .hasArg()
              .required(true)
              .desc("Address of the ZooKeeper ensemble; defaults to: " + ZK_HOST + '.')
              .build(),
          Option.builder("verbose")
              .required(false)
              .desc("Enable more verbose command output.")
              .build()
      };
    }

    public String getName() {
      return "mv";
    }

    protected void runImpl(CommandLine cli) throws Exception {
      raiseLogLevelUnlessVerbose(cli);
      String zkHost = getZkHost(cli);
      if (zkHost == null) {
        throw new IllegalStateException("Solr at " + cli.getOptionValue("solrUrl") +
            " is running in standalone server mode, downconfig can only be used when running in SolrCloud mode.\n");
      }


      try (SolrZkClient zkClient = new SolrZkClient(zkHost, 30000)) {
        echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
        String src = cli.getOptionValue("src");
        String dst = cli.getOptionValue("dst");

        if (src.toLowerCase(Locale.ROOT).startsWith("file:") || dst.toLowerCase(Locale.ROOT).startsWith("file:")) {
          throw new SolrServerException("mv command operates on znodes and 'file:' has been specified.");
        }
        String source = src;
        if (src.toLowerCase(Locale.ROOT).startsWith("zk")) {
          source = src.substring(3);
        }

        String dest = dst;
        if (dst.toLowerCase(Locale.ROOT).startsWith("zk")) {
          dest = dst.substring(3);
        }

        echo("Moving Znode " + source + " to " + dest + " on ZooKeeper at " + zkHost);
        zkClient.moveZnode(source, dest);
      } catch (Exception e) {
        log.error("Could not complete mv operation for reason: ", e);
        throw (e);
      }

    }
  } // End MvTool class



  public static class DeleteTool extends ToolBase {

    public DeleteTool() { this(System.out); }
    public DeleteTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "delete";
    }

    public Option[] getOptions() {
      return new Option[]{
          Option.builder("solrUrl")
              .argName("URL")
              .hasArg()
              .required(false)
              .desc("Base Solr URL, default is " + DEFAULT_SOLR_URL + '.')
              .build(),
          Option.builder(NAME)
              .argName("NAME")
              .hasArg()
              .required(true)
              .desc("Name of the core / collection to delete.")
              .build(),
          Option.builder("deleteConfig")
              .argName("true|false")
              .hasArg()
              .required(false)
              .desc("Flag to indicate if the underlying configuration directory for a collection should also be deleted; default is true.")
              .build(),
          Option.builder("forceDeleteConfig")
              .required(false)
              .desc("Skip safety checks when deleting the configuration directory used by a collection.")
              .build(),
          Option.builder("zkHost")
              .argName("HOST")
              .hasArg()
              .required(false)
              .desc("Address of the ZooKeeper ensemble; defaults to: "+ ZK_HOST + '.')
              .build(),
          Option.builder("verbose")
              .required(false)
              .desc("Enable more verbose command output.")
              .build()
      };
    }

    protected void runImpl(CommandLine cli) throws Exception {
      raiseLogLevelUnlessVerbose(cli);
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
      try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(Collections.singletonList(zkHost), Optional.empty()).withSocketTimeout(30000).withConnectionTimeout(15000).build()) {
        echoIfVerbose("Connecting to ZooKeeper at " + zkHost, cli);
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
          log.warn("Skipping safety checks, configuration directory {} will be deleted with impunity.", configName);
        } else {
          // need to scan all Collections to see if any are using the config
          Set<String> collections = zkStateReader.getClusterState().getCollectionsMap().keySet();

          // give a little note to the user if there are many collections in case it takes a while
          if (collections.size() > 50)
            if (log.isInfoEnabled()) {
              log.info("Scanning {} to ensure no other collections are using config {}", collections.size(), configName);
            }

          for (String next : collections) {
            if (collectionName.equals(next))
              continue; // don't check the collection we're deleting

            if (configName.equals(zkStateReader.readConfigName(next))) {
              deleteConfig = false;
              log.warn("Configuration directory {} is also being used by {}{}"
                  , configName, next
                  , "; configuration will not be deleted from ZooKeeper. You can pass the -forceDeleteConfig flag to force delete.");
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

      echoIfVerbose("\nDeleting collection '" + collectionName + "' using command:\n" + deleteCollectionUrl + "\n", cli);

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
          echo("\nWARNING: Failed to delete configuration directory "+configZnode+" in ZooKeeper due to: "+
              exc.getMessage()+"\nYou'll need to manually delete this znode using the zkcli script.");
        }
      }

      if (json != null) {
        CharArr arr = new CharArr();
        new JSONWriter(arr, 2).write(json);
        echo(arr.toString());
        echo("\n");
      }

      echo("Deleted collection '" + collectionName + "' using command:\n" + deleteCollectionUrl);
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
        echoIfVerbose(arr.toString(), cli);
        echoIfVerbose("\n", cli);
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

    @Override
    public Option[] getOptions() {
      Option[] configOptions = new Option[] {
          Option.builder("action")
              .argName("ACTION")
              .hasArg()
              .required(false)
              .desc("Config API action, one of: set-property, unset-property; default is 'set-property'.")
              .build(),
          Option.builder("property")
              .argName("PROP")
              .hasArg()
              .required(true)
              .desc("Name of the Config API property to apply the action to, such as: 'updateHandler.autoSoftCommit.maxTime'.")
              .build(),
          Option.builder("value")
              .argName("VALUE")
              .hasArg()
              .required(false)
              .desc("Set the property to this value; accepts JSON objects and strings.")
              .build(),
          Option.builder("solrUrl")
              .argName("HOST")
              .hasArg()
              .required(false)
              .desc("Base Solr URL, which can be used to determine the zkHost if that's not known.")
              .build(),
          Option.builder("z")
              .argName("HOST")
              .hasArg()
              .required(false)
              .desc("Address of the ZooKeeper ensemble.")
              .longOpt("zkHost")
              .build(),
          Option.builder("p")
              .argName("PORT")
              .hasArg()
              .required(false)
              .desc("The port of the Solr node to use when applying configuration change.")
              .longOpt("port")
              .build(),
          Option.builder("s")
              .argName("SCHEME")
              .hasArg()
              .required(false)
              .desc("The scheme for accessing Solr.  Accepted values: http or https.  Default is 'http'")
              .longOpt("scheme")
              .build()
      };
      return joinOptions(configOptions, cloudOptions);
    }

    protected void runImpl(CommandLine cli) throws Exception {
      String solrUrl;
      try {
        solrUrl = resolveSolrUrl(cli);
      } catch (IllegalStateException e) {
        // Fallback to using the provided scheme and port
        final String scheme = cli.getOptionValue("scheme", "http");
        if (cli.hasOption("port")) {
          solrUrl = scheme + "://localhost:" + cli.getOptionValue("port", "8983") + "/solr";
        } else {
          throw e;
        }
      }

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

      try (SolrClient solrClient = new HttpSolrClient.Builder(solrUrl).build()) {
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

    public Option[] getOptions() {
      return new Option[] {
          Option.builder("noprompt")
              .required(false)
              .desc("Don't prompt for input; accept all defaults when running examples that accept user input.")
              .build(),
          Option.builder("e")
              .argName("NAME")
              .hasArg()
              .required(true)
              .desc("Name of the example to launch, one of: cloud, techproducts, dih, schemaless")
              .longOpt("example")
              .build(),
          Option.builder("script")
              .argName("PATH")
              .hasArg()
              .required(false)
              .desc("Path to the bin/solr script.")
              .build(),
          Option.builder("d")
              .argName("DIR")
              .hasArg()
              .required(true)
              .desc("Path to the Solr server directory.")
              .longOpt("serverDir")
              .build(),
          Option.builder("force")
              .argName("FORCE")
              .desc("Force option in case Solr is run as root.")
              .build(),
          Option.builder("exampleDir")
              .argName("DIR")
              .hasArg()
              .required(false)
              .desc("Path to the Solr example directory; if not provided, ${serverDir}/../example is expected to exist.")
              .build(),
          Option.builder("urlScheme")
              .argName("SCHEME")
              .hasArg()
              .required(false)
              .desc("Solr URL scheme: http or https, defaults to http if not specified.")
              .build(),
          Option.builder("p")
              .argName("PORT")
              .hasArg()
              .required(false)
              .desc("Specify the port to start the Solr HTTP listener on; default is 8983.")
              .longOpt("port")
              .build(),
          Option.builder("h")
              .argName("HOSTNAME")
              .hasArg()
              .required(false)
              .desc("Specify the hostname for this Solr instance.")
              .longOpt("host")
              .build(),
          Option.builder("z")
              .argName("ZKHOST")
              .hasArg()
              .required(false)
              .desc("ZooKeeper connection string; only used when running in SolrCloud mode using -c.")
              .longOpt("zkhost")
              .build(),
          Option.builder("c")
              .required(false)
              .desc("Start Solr in SolrCloud mode; if -z not supplied, an embedded ZooKeeper instance is started on Solr port+1000, such as 9983 if Solr is bound to 8983.")
              .longOpt("cloud")
              .build(),
          Option.builder("m")
              .argName("MEM")
              .hasArg()
              .required(false)
              .desc("Sets the min (-Xms) and max (-Xmx) heap size for the JVM, such as: -m 4g results in: -Xms4g -Xmx4g; by default, this script sets the heap size to 512m.")
              .longOpt("memory")
              .build(),
          Option.builder("a")
              .argName("OPTS")
              .hasArg()
              .required(false)
              .desc("Additional options to be passed to the JVM when starting example Solr server(s).")
              .longOpt("addlopts")
              .build()
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
          "techproducts".equals(exampleName) ? "sample_techproducts_configs" : "_default";

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
            createTool.runTool(processCommandLineArgs(createTool.getName(), joinCommonAndToolOptions(createTool.getOptions()), createArgs));
        if (createCode != 0)
          throw new Exception("Failed to create "+collectionName+" using command: "+ Arrays.asList(createArgs));
      }

      if ("techproducts".equals(exampleName) && !alreadyExists) {

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
        @SuppressWarnings("unchecked")
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
        configTool.runTool(processCommandLineArgs(configTool.getName(), joinCommonAndToolOptions(configTool.getOptions()), configArgs));
      } catch (Exception exc) {
        System.err.println("Failed to update '"+propName+"' property due to: "+exc);
      }
    }

    protected void waitToSeeLiveNodes(int maxWaitSecs, String zkHost, int numNodes) {
      CloudSolrClient cloudClient = null;
      try {
        cloudClient = new CloudSolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
            .build();
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
      String forceArg = cli.hasOption("force") ? " -force" : "";

      String addlOpts = cli.getOptionValue('a');
      String addlOptsArg = (addlOpts != null) ? " -a \""+addlOpts+"\"" : "";

      File cwd = new File(System.getProperty("user.dir"));
      File binDir = (new File(script)).getParentFile();

      boolean isWindows = (OS.isFamilyDOS() || OS.isFamilyWin9x() || OS.isFamilyWindows());
      String callScript = (!isWindows && cwd.equals(binDir.getParentFile())) ? "bin/solr" : script;

      String cwdPath = cwd.getAbsolutePath();
      String solrHome = solrHomeDir.getAbsolutePath();

      // don't display a huge path for solr home if it is relative to the cwd
      if (!isWindows && cwdPath.length() > 1 && solrHome.startsWith(cwdPath))
        solrHome = solrHome.substring(cwdPath.length()+1);

      String startCmd =
          String.format(Locale.ROOT, "\"%s\" start %s -p %d -s \"%s\" %s %s %s %s %s %s",
              callScript, cloudModeArg, port, solrHome, hostArg, zkHostArg, memArg, forceArg, extraArgs, addlOptsArg);
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
          for (Map.Entry<String, String> entry : procEnv.entrySet()) {
            String envVar = entry.getKey();
            String envVarVal = entry.getValue();
            if (envVarVal != null && !"EXAMPLE".equals(envVar) && !envVar.startsWith("SOLR_")) {
              startEnv.put(envVar, envVarVal);
            }
          }
        }
        DefaultExecuteResultHandler handler = new DefaultExecuteResultHandler();
        executor.execute(org.apache.commons.exec.CommandLine.parse(startCmd), startEnv, handler);

        // wait for execution.
        try {
          handler.waitFor(3000);
        } catch (InterruptedException ie) {
          // safe to ignore ...
          Thread.interrupted();
        }
        if (handler.hasResult() && handler.getExitValue() != 0) {
          throw new Exception("Failed to start Solr using command: "+startCmd+" Exception : "+handler.getException());
        }
      } else {
        try {
          code = executor.execute(org.apache.commons.exec.CommandLine.parse(startCmd));
        } catch(ExecuteException e){
          throw new Exception("Failed to start Solr using command: "+startCmd+" Exception : "+ e);
        }
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
      String cloudConfig = "_default";
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
        String validConfigs = "_default or sample_techproducts_configs ["+cloudConfig+"] ";
        cloudConfig = prompt(readInput, validConfigs, cloudConfig);

        // validate the cloudConfig name
        while (!isValidConfig(configsetsDir, cloudConfig)) {
          echo(cloudConfig+" is not a valid configuration directory! Please choose a configuration for the "+collectionName+" collection, available options are:");
          cloudConfig = prompt(readInput, validConfigs, cloudConfig);
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
              processCommandLineArgs(createCollectionTool.getName(), joinCommonAndToolOptions(createCollectionTool.getOptions()), createArgs));

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
            inputAsInt = Integer.valueOf(value);

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

  /**
   * Asserts various conditions and exists with error code if fails, else continues with no output
   */
  public static class AssertTool extends ToolBase {

    private static String message = null;
    private static boolean useExitCode = false;
    private static Optional<Long> timeoutMs = Optional.empty();

    public AssertTool() { this(System.out); }
    public AssertTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "assert";
    }

    public Option[] getOptions() {
      return new Option[] {
          Option.builder("R")
              .desc("Asserts that we are NOT the root user.")
              .longOpt("not-root")
              .build(),
          Option.builder("r")
              .desc("Asserts that we are the root user.")
              .longOpt("root")
              .build(),
          Option.builder("S")
              .desc("Asserts that Solr is NOT running on a certain URL. Default timeout is 1000ms.")
              .longOpt("not-started")
              .hasArg(true)
              .argName("url")
              .build(),
          Option.builder("s")
              .desc("Asserts that Solr is running on a certain URL. Default timeout is 1000ms.")
              .longOpt("started")
              .hasArg(true)
              .argName("url")
              .build(),
          Option.builder("u")
              .desc("Asserts that we run as same user that owns <directory>.")
              .longOpt("same-user")
              .hasArg(true)
              .argName("directory")
              .build(),
          Option.builder("x")
              .desc("Asserts that directory <directory> exists.")
              .longOpt("exists")
              .hasArg(true)
              .argName("directory")
              .build(),
          Option.builder("X")
              .desc("Asserts that directory <directory> does NOT exist.")
              .longOpt("not-exists")
              .hasArg(true)
              .argName("directory")
              .build(),
          Option.builder("c")
              .desc("Asserts that Solr is running in cloud mode.  Also fails if Solr not running.  URL should be for root Solr path.")
              .longOpt("cloud")
              .hasArg(true)
              .argName("url")
              .build(),
          Option.builder("C")
              .desc("Asserts that Solr is not running in cloud mode.  Also fails if Solr not running.  URL should be for root Solr path.")
              .longOpt("not-cloud")
              .hasArg(true)
              .argName("url")
              .build(),
          Option.builder("m")
              .desc("Exception message to be used in place of the default error message.")
              .longOpt("message")
              .hasArg(true)
              .argName("message")
              .build(),
          Option.builder("t")
              .desc("Timeout in ms for commands supporting a timeout.")
              .longOpt("timeout")
              .hasArg(true)
              .type(Long.class)
              .argName("ms")
              .build(),
          Option.builder("e")
              .desc("Return an exit code instead of printing error message on assert fail.")
              .longOpt("exitcode")
              .build()
      };
    }

    public int runTool(CommandLine cli) throws Exception {
      verbose = cli.hasOption("verbose");

      int toolExitStatus = 0;
      try {
        toolExitStatus = runAssert(cli);
      } catch (Exception exc) {
        // since this is a CLI, spare the user the stacktrace
        String excMsg = exc.getMessage();
        if (excMsg != null) {
          System.err.println("\nERROR: " + excMsg + "\n");
          if (verbose) {
            exc.printStackTrace(System.err);
          }
          toolExitStatus = 100; // Exit >= 100 means error, else means number of tests that failed
        } else {
          throw exc;
        }
      }
      return toolExitStatus;
    }

    @Override
    protected void runImpl(CommandLine cli) throws Exception {
      runAssert(cli);
    }

    /**
     * Custom run method which may return exit code
     * @param cli the command line object
     * @return 0 on success, or a number corresponding to number of tests that failed
     * @throws Exception if a tool failed, e.g. authentication failure
     */
    protected int runAssert(CommandLine cli) throws Exception {
      if (cli.getOptions().length == 0 || cli.getArgs().length > 0 || cli.hasOption("h")) {
        new HelpFormatter().printHelp("bin/solr assert [-m <message>] [-e] [-rR] [-s <url>] [-S <url>] [-c <url>] [-C <url>] [-u <dir>] [-x <dir>] [-X <dir>]", getToolOptions(this));
        return 1;
      }
      if (cli.hasOption("m")) {
        message = cli.getOptionValue("m");
      }
      if (cli.hasOption("t")) {
        timeoutMs = Optional.of(Long.parseLong(cli.getOptionValue("t")));
      }
      if (cli.hasOption("e")) {
        useExitCode = true;
      }

      int ret = 0;
      if (cli.hasOption("r")) {
        ret += assertRootUser();
      }
      if (cli.hasOption("R")) {
        ret += assertNotRootUser();
      }
      if (cli.hasOption("x")) {
        ret += assertFileExists(cli.getOptionValue("x"));
      }
      if (cli.hasOption("X")) {
        ret += assertFileNotExists(cli.getOptionValue("X"));
      }
      if (cli.hasOption("u")) {
        ret += sameUser(cli.getOptionValue("u"));
      }
      if (cli.hasOption("s")) {
        ret += assertSolrRunning(cli.getOptionValue("s"));
      }
      if (cli.hasOption("S")) {
        ret += assertSolrNotRunning(cli.getOptionValue("S"));
      }
      if (cli.hasOption("c")) {
        ret += assertSolrRunningInCloudMode(cli.getOptionValue("c"));
      }
      if (cli.hasOption("C")) {
        ret += assertSolrNotRunningInCloudMode(cli.getOptionValue("C"));
      }
      return ret;
    }

    public static int assertSolrRunning(String url) throws Exception {
      StatusTool status = new StatusTool();
      try {
        status.waitToSeeSolrUp(url, timeoutMs.orElse(1000L).intValue() / 1000);
      } catch (Exception se) {
        if (exceptionIsAuthRelated(se)) {
          throw se;
        }
        return exitOrException("Solr is not running on url " + url + " after " + timeoutMs.orElse(1000L) / 1000 + "s");
      }
      return 0;
    }

    public static int assertSolrNotRunning(String url) throws Exception {
      StatusTool status = new StatusTool();
      long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutMs.orElse(1000L), TimeUnit.MILLISECONDS);
      try {
        attemptHttpHead(url, getHttpClient());
      } catch (SolrException se) {
        throw se; // Auth error
      } catch (IOException e) {
        log.debug("Opening connection to {} failed, Solr does not seem to be running", url, e);
        return 0;
      }
      while (System.nanoTime() < timeout) {
        try {
          status.waitToSeeSolrUp(url, 1);
          try {
            log.debug("Solr still up. Waiting before trying again to see if it was stopped");
            Thread.sleep(1000L);
          } catch (InterruptedException interrupted) {
            timeout = 0; // stop looping
          }
        } catch (Exception se) {
          if (exceptionIsAuthRelated(se)) {
            throw se;
          }
          return exitOrException(se.getMessage());
        }
      }
      return exitOrException("Solr is still running at " + url + " after " + timeoutMs.orElse(1000L) / 1000 + "s");
    }

    public static int assertSolrRunningInCloudMode(String url) throws Exception {
      if (! isSolrRunningOn(url)) {
        return exitOrException("Solr is not running on url " + url + " after " + timeoutMs.orElse(1000L) / 1000 + "s");
      }

      if (! runningSolrIsCloud(url)) {
        return exitOrException("Solr is not running in cloud mode on " + url);
      }
      return 0;
    }

    public static int assertSolrNotRunningInCloudMode(String url) throws Exception {
      if (! isSolrRunningOn(url)) {
        return exitOrException("Solr is not running on url " + url + " after " + timeoutMs.orElse(1000L) / 1000 + "s");
      }

      if (runningSolrIsCloud(url)) {
        return exitOrException("Solr is not running in standalone mode on " + url);
      }
      return 0;
    }

    public static int sameUser(String directory) throws Exception {
      if (Files.exists(Paths.get(directory))) {
        String userForDir = userForDir(Paths.get(directory));
        if (!currentUser().equals(userForDir)) {
          return exitOrException("Must run as user " + userForDir + ". We are " + currentUser());
        }
      } else {
        return exitOrException("Directory " + directory + " does not exist.");
      }
      return 0;
    }

    public static int assertFileExists(String directory) throws Exception {
      if (! Files.exists(Paths.get(directory))) {
        return exitOrException("Directory " + directory + " does not exist.");
      }
      return 0;
    }

    public static int assertFileNotExists(String directory) throws Exception {
      if (Files.exists(Paths.get(directory))) {
        return exitOrException("Directory " + directory + " should not exist.");
      }
      return 0;
    }

    public static int assertRootUser() throws Exception {
      if (!currentUser().equals("root")) {
        return exitOrException("Must run as root user");
      }
      return 0;
    }

    public static int assertNotRootUser() throws Exception {
      if (currentUser().equals("root")) {
        return exitOrException("Not allowed to run as root user");
      }
      return 0;
    }

    public static String currentUser() {
      return System.getProperty("user.name");
    }

    public static String userForDir(Path pathToDir) {
      try {
        FileOwnerAttributeView ownerAttributeView = Files.getFileAttributeView(pathToDir, FileOwnerAttributeView.class);
        return ownerAttributeView.getOwner().getName();
      } catch (IOException e) {
        return "N/A";
      }
    }

    private static int exitOrException(String msg) throws AssertionFailureException {
      if (useExitCode) {
        return 1;
      } else {
        throw new AssertionFailureException(message != null ? message : msg);
      }
    }

    private static boolean isSolrRunningOn(String url) throws Exception {
      StatusTool status = new StatusTool();
      try {
        status.waitToSeeSolrUp(url, timeoutMs.orElse(1000L).intValue() / 1000);
        return true;
      } catch (Exception se) {
        if (exceptionIsAuthRelated(se)) {
          throw se;
        }
        return false;
      }
    }

    private static boolean runningSolrIsCloud(String url) throws Exception {
      try (final HttpSolrClient client = new HttpSolrClient.Builder(url).build()) {
        final SolrRequest<CollectionAdminResponse> request = new CollectionAdminRequest.ClusterStatus();
        final CollectionAdminResponse response = request.process(client);
        return response != null;
      } catch (Exception e) {
        if (exceptionIsAuthRelated(e)) {
          throw e;
        }
        return false;
      }
    }
  } // end AssertTool class

  public static class AssertionFailureException extends Exception {
    public AssertionFailureException(String message) {
      super(message);
    }
  }

  // Authentication tool
  public static class AuthTool extends ToolBase {
    public AuthTool() { this(System.out); }
    public AuthTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "auth";
    }

    List<String> authenticationVariables = Arrays.asList("SOLR_AUTHENTICATION_CLIENT_BUILDER", "SOLR_AUTH_TYPE", "SOLR_AUTHENTICATION_OPTS");

    public Option[] getOptions() {
      return new Option[]{
          Option.builder("type")
              .argName("type")
              .hasArg()
              .desc("The authentication mechanism to enable (basicAuth or kerberos). Defaults to 'basicAuth'.")
              .build(),
          Option.builder("credentials")
              .argName("credentials")
              .hasArg()
              .desc("Credentials in the format username:password. Example: -credentials solr:SolrRocks")
              .build(),
          Option.builder("prompt")
              .argName("prompt")
              .hasArg()
              .desc("Prompts the user to provide the credentials. Use either -credentials or -prompt, not both.")
              .build(),
          Option.builder("config")
              .argName("config")
              .hasArgs()
              .desc("Configuration parameters (Solr startup parameters). Required for Kerberos authentication.")
              .build(),
          Option.builder("blockUnknown")
              .argName("blockUnknown")
              .desc("Blocks all access for unknown users (requires authentication for all endpoints).")
              .hasArg()
              .build(),
          Option.builder("solrIncludeFile")
              .argName("solrIncludeFile")
              .hasArg()
              .desc("The Solr include file which contains overridable environment variables for configuring Solr configurations.")
              .build(),
          Option.builder("updateIncludeFileOnly")
              .argName("updateIncludeFileOnly")
              .desc("Only update the solr.in.sh or solr.in.cmd file, and skip actual enabling/disabling"
                  + " authentication (i.e. don't update security.json).")
              .hasArg()
              .build(),
          Option.builder("authConfDir")
              .argName("authConfDir")
              .hasArg()
              .required()
              .desc("This is where any authentication related configuration files, if any, would be placed.")
              .build(),
          Option.builder("solrUrl")
              .argName("solrUrl")
              .hasArg()
              .desc("Solr URL.")
              .build(),
          Option.builder("zkHost")
              .argName("zkHost")
              .hasArg()
              .desc("ZooKeeper host to connect to.")
              .build(),
          Option.builder("verbose")
              .required(false)
              .desc("Enable more verbose command output.")
              .build()
      };
    }

    private void ensureArgumentIsValidBooleanIfPresent(CommandLine cli, String argName) {
      if (cli.hasOption(argName)) {
        final String value = cli.getOptionValue(argName);
        final Boolean parsedBoolean = BooleanUtils.toBooleanObject(value);
        if (parsedBoolean == null) {
          echo("Argument [" + argName + "] must be either true or false, but was [" + value + "]");
          exit(1);
        }
      }
    }

    @Override
    public int runTool(CommandLine cli) throws Exception {
      raiseLogLevelUnlessVerbose(cli);
      if (cli.getOptions().length == 0 || cli.getArgs().length == 0 || cli.getArgs().length > 1 || cli.hasOption("h")) {
        new HelpFormatter().printHelp("bin/solr auth <enable|disable> [OPTIONS]", getToolOptions(this));
        return 1;
      }

      ensureArgumentIsValidBooleanIfPresent(cli, "blockUnknown");
      ensureArgumentIsValidBooleanIfPresent(cli, "updateIncludeFileOnly");

      String type = cli.getOptionValue("type", "basicAuth");
      switch (type) {
        case "basicAuth":
          return handleBasicAuth(cli);
        case "kerberos":
          return handleKerberos(cli);
        default:
          System.out.println("Only type=basicAuth or kerberos supported at the moment.");
          exit(1);
      }
      return 1;
    }

    private int handleKerberos(CommandLine cli) throws Exception {
      String cmd = cli.getArgs()[0];
      boolean updateIncludeFileOnly = Boolean.parseBoolean(cli.getOptionValue("updateIncludeFileOnly", "false"));
      String securityJson = "{" +
          "\n  \"authentication\":{" +
          "\n   \"class\":\"solr.KerberosPlugin\"" +
          "\n  }" +
          "\n}";


      switch (cmd) {
        case "enable":
          String zkHost = null;
          boolean zkInaccessible = false;

          if (!updateIncludeFileOnly) {
            try {
              zkHost = getZkHost(cli);
            } catch (Exception ex) {
              System.out.println("Unable to access ZooKeeper. Please add the following security.json to ZooKeeper (in case of SolrCloud):\n"
                  + securityJson + "\n");
              zkInaccessible = true;
            }
            if (zkHost == null) {
              if (zkInaccessible == false) {
                System.out.println("Unable to access ZooKeeper. Please add the following security.json to ZooKeeper (in case of SolrCloud):\n"
                    + securityJson + "\n");
                zkInaccessible = true;
              }
            }

            // check if security is already enabled or not
            if (!zkInaccessible) {
              try (SolrZkClient zkClient = new SolrZkClient(zkHost, 10000)) {
                if (zkClient.exists("/security.json", true)) {
                  byte oldSecurityBytes[] = zkClient.getData("/security.json", null, null, true);
                  if (!"{}".equals(new String(oldSecurityBytes, StandardCharsets.UTF_8).trim())) {
                    System.out.println("Security is already enabled. You can disable it with 'bin/solr auth disable'. Existing security.json: \n"
                        + new String(oldSecurityBytes, StandardCharsets.UTF_8));
                    exit(1);
                  }
                }
              } catch (Exception ex) {
                if (zkInaccessible == false) {
                  System.out.println("Unable to access ZooKeeper. Please add the following security.json to ZooKeeper (in case of SolrCloud):\n"
                      + securityJson + "\n");
                  zkInaccessible = true;
                }
              }
            }
          }

          if (!updateIncludeFileOnly) {
            if (!zkInaccessible) {
              echoIfVerbose("Uploading following security.json: " + securityJson, cli);
              try (SolrZkClient zkClient = new SolrZkClient(zkHost, 10000)) {
                zkClient.setData("/security.json", securityJson.getBytes(StandardCharsets.UTF_8), true);
              } catch (Exception ex) {
                if (zkInaccessible == false) {
                  System.out.println("Unable to access ZooKeeper. Please add the following security.json to ZooKeeper (in case of SolrCloud):\n"
                      + securityJson);
                  zkInaccessible = true;
                }
              }
            }
          }

          String config = StrUtils.join(Arrays.asList(cli.getOptionValues("config")), ' ');
          // config is base64 encoded (to get around parsing problems), decode it
          config = config.replaceAll(" ", "");
          config = new String(Base64.getDecoder()
              .decode(config.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
          config = config.replaceAll("\n", "").replaceAll("\r", "");

          String solrIncludeFilename = cli.getOptionValue("solrIncludeFile");
          File includeFile = new File(solrIncludeFilename);
          if (includeFile.exists() == false || includeFile.canWrite() == false) {
            System.out.println("Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
            printAuthEnablingInstructions(config);
            System.exit(0);
          }

          // update the solr.in.sh file to contain the necessary authentication lines
          updateIncludeFileEnableAuth(includeFile, null, config, cli);
          echo("Successfully enabled Kerberos authentication; please restart any running Solr nodes.");
          return 0;

        case "disable":
          if (!updateIncludeFileOnly) {
            zkHost = getZkHost(cli);
            if (zkHost == null) {
              stdout.print("ZK Host not found. Solr should be running in cloud mode.");
              exit(1);
            }

            echoIfVerbose("Uploading following security.json: {}", cli);

            try (SolrZkClient zkClient = new SolrZkClient(zkHost, 10000)) {
              zkClient.setData("/security.json", "{}".getBytes(StandardCharsets.UTF_8), true);
            }
          }

          solrIncludeFilename = cli.getOptionValue("solrIncludeFile");
          includeFile = new File(solrIncludeFilename);
          if (!includeFile.exists() || !includeFile.canWrite()) {
            System.out.println("Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
            System.out.println("Security has been disabled. Please remove any SOLR_AUTH_TYPE or SOLR_AUTHENTICATION_OPTS configuration from solr.in.sh/solr.in.cmd.\n");
            System.exit(0);
          }

          // update the solr.in.sh file to comment out the necessary authentication lines
          updateIncludeFileDisableAuth(includeFile, cli);
          return 0;

        default:
          System.out.println("Valid auth commands are: enable, disable");
          exit(1);
      }

      System.out.println("Options not understood.");
      new HelpFormatter().printHelp("bin/solr auth <enable|disable> [OPTIONS]", getToolOptions(this));
      return 1;
    }
    private int handleBasicAuth(CommandLine cli) throws Exception {
      String cmd = cli.getArgs()[0];
      boolean prompt = Boolean.parseBoolean(cli.getOptionValue("prompt", "false"));
      boolean updateIncludeFileOnly = Boolean.parseBoolean(cli.getOptionValue("updateIncludeFileOnly", "false"));
      switch (cmd) {
        case "enable":
          if (!prompt && !cli.hasOption("credentials")) {
            System.out.println("Option -credentials or -prompt is required with enable.");
            new HelpFormatter().printHelp("bin/solr auth <enable|disable> [OPTIONS]", getToolOptions(this));
            exit(1);
          } else if (!prompt && (cli.getOptionValue("credentials") == null || !cli.getOptionValue("credentials").contains(":"))) {
            System.out.println("Option -credentials is not in correct format.");
            new HelpFormatter().printHelp("bin/solr auth <enable|disable> [OPTIONS]", getToolOptions(this));
            exit(1);
          }

          String zkHost = null;

          if (!updateIncludeFileOnly) {
            try {
              zkHost = getZkHost(cli);
            } catch (Exception ex) {
              if (cli.hasOption("zkHost")) {
                System.out.println("Couldn't get ZooKeeper host. Please make sure that ZooKeeper is running and the correct zkHost has been passed in.");
              } else {
                System.out.println("Couldn't get ZooKeeper host. Please make sure Solr is running in cloud mode, or a zkHost has been passed in.");
              }
              exit(1);
            }
            if (zkHost == null) {
              if (cli.hasOption("zkHost")) {
                System.out.println("Couldn't get ZooKeeper host. Please make sure that ZooKeeper is running and the correct zkHost has been passed in.");
              } else {
                System.out.println("Couldn't get ZooKeeper host. Please make sure Solr is running in cloud mode, or a zkHost has been passed in.");
              }
              exit(1);
            }

            // check if security is already enabled or not
            try (SolrZkClient zkClient = new SolrZkClient(zkHost, 10000)) {
              if (zkClient.exists("/security.json", true)) {
                byte oldSecurityBytes[] = zkClient.getData("/security.json", null, null, true);
                if (!"{}".equals(new String(oldSecurityBytes, StandardCharsets.UTF_8).trim())) {
                  System.out.println("Security is already enabled. You can disable it with 'bin/solr auth disable'. Existing security.json: \n"
                      + new String(oldSecurityBytes, StandardCharsets.UTF_8));
                  exit(1);
                }
              }
            }
          }

          String username, password;
          if (cli.hasOption("credentials")) {
            String credentials = cli.getOptionValue("credentials");
            username = credentials.split(":")[0];
            password = credentials.split(":")[1];
          } else {
            Console console = System.console();
            // keep prompting until they've entered a non-empty username & password
            do {
              username = console.readLine("Enter username: ");
            } while (username == null || username.trim().length() == 0);
            username = username.trim();

            do {
              password = new String(console.readPassword("Enter password: "));
            } while (password.length() == 0);
          }

          boolean blockUnknown = Boolean.valueOf(cli.getOptionValue("blockUnknown", "false"));

          String securityJson = "{" +
              "\n  \"authentication\":{" +
              "\n   \"blockUnknown\": " + blockUnknown + "," +
              "\n   \"class\":\"solr.BasicAuthPlugin\"," +
              "\n   \"credentials\":{\"" + username + "\":\"" + Sha256AuthenticationProvider.getSaltedHashedValue(password) + "\"}" +
              "\n  }," +
              "\n  \"authorization\":{" +
              "\n   \"class\":\"solr.RuleBasedAuthorizationPlugin\"," +
              "\n   \"permissions\":[" +
              "\n {\"name\":\"security-edit\", \"role\":\"admin\"}," +
              "\n {\"name\":\"security-read\", \"role\":\"admin\"}," +
              "\n {\"name\":\"config-edit\", \"role\":\"admin\"}," +
              "\n {\"name\":\"config-read\", \"role\":\"admin\"}," +
              "\n {\"name\":\"collection-admin-edit\", \"role\":\"admin\"}," +
              "\n {\"name\":\"collection-admin-read\", \"role\":\"admin\"}," +
              "\n {\"name\":\"core-admin-edit\", \"role\":\"admin\"}," +
              "\n {\"name\":\"core-admin-read\", \"role\":\"admin\"}," +
              "\n {\"name\":\"all\", \"role\":\"admin\"}" +
              "\n   ]," +
              "\n   \"user-role\":{\"" + username + "\":\"admin\"}" +
              "\n  }" +
              "\n}";

          if (!updateIncludeFileOnly) {
            echoIfVerbose("Uploading following security.json: " + securityJson, cli);
            try (SolrZkClient zkClient = new SolrZkClient(zkHost, 10000)) {
              zkClient.setData("/security.json", securityJson.getBytes(StandardCharsets.UTF_8), true);
            }
          }

          String solrIncludeFilename = cli.getOptionValue("solrIncludeFile");
          File includeFile = new File(solrIncludeFilename);
          if (includeFile.exists() == false || includeFile.canWrite() == false) {
            System.out.println("Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
            printAuthEnablingInstructions(username, password);
            System.exit(0);
          }
          String authConfDir = cli.getOptionValue("authConfDir");
          File basicAuthConfFile = new File(authConfDir + File.separator + "basicAuth.conf");

          if (basicAuthConfFile.getParentFile().canWrite() == false) {
            System.out.println("Cannot write to file: " + basicAuthConfFile.getAbsolutePath());
            printAuthEnablingInstructions(username, password);
            System.exit(0);
          }

          FileUtils.writeStringToFile(basicAuthConfFile,
              "httpBasicAuthUser=" + username + "\nhttpBasicAuthPassword=" + password, StandardCharsets.UTF_8);

          // update the solr.in.sh file to contain the necessary authentication lines
          updateIncludeFileEnableAuth(includeFile, basicAuthConfFile.getAbsolutePath(), null, cli);
          final String successMessage = String.format(Locale.ROOT,
              "Successfully enabled basic auth with username [%s] and password [%s].", username, password);
          echo(successMessage);
          return 0;

        case "disable":
          if (!updateIncludeFileOnly) {
            zkHost = getZkHost(cli);
            if (zkHost == null) {
              stdout.print("ZK Host not found. Solr should be running in cloud mode.");
              exit(1);
            }

            echoIfVerbose("Uploading following security.json: {}", cli);

            try (SolrZkClient zkClient = new SolrZkClient(zkHost, 10000)) {
              zkClient.setData("/security.json", "{}".getBytes(StandardCharsets.UTF_8), true);
            }
          }

          solrIncludeFilename = cli.getOptionValue("solrIncludeFile");
          includeFile = new File(solrIncludeFilename);
          if (!includeFile.exists() || !includeFile.canWrite()) {
            System.out.println("Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
            System.out.println("Security has been disabled. Please remove any SOLR_AUTH_TYPE or SOLR_AUTHENTICATION_OPTS configuration from solr.in.sh/solr.in.cmd.\n");
            System.exit(0);
          }

          // update the solr.in.sh file to comment out the necessary authentication lines
          updateIncludeFileDisableAuth(includeFile, cli);
          return 0;

        default:
          System.out.println("Valid auth commands are: enable, disable");
          exit(1);
      }

      System.out.println("Options not understood.");
      new HelpFormatter().printHelp("bin/solr auth <enable|disable> [OPTIONS]", getToolOptions(this));
      return 1;
    }
    private void printAuthEnablingInstructions(String username, String password) {
      if (SystemUtils.IS_OS_WINDOWS) {
        System.out.println("\nAdd the following lines to the solr.in.cmd file so that the solr.cmd script can use subsequently.\n");
        System.out.println("set SOLR_AUTH_TYPE=basic\n"
            + "set SOLR_AUTHENTICATION_OPTS=\"-Dbasicauth=" + username + ":" + password + "\"\n");
      } else {
        System.out.println("\nAdd the following lines to the solr.in.sh file so that the ./solr script can use subsequently.\n");
        System.out.println("SOLR_AUTH_TYPE=\"basic\"\n"
            + "SOLR_AUTHENTICATION_OPTS=\"-Dbasicauth=" + username + ":" + password + "\"\n");
      }
    }
    private void printAuthEnablingInstructions(String kerberosConfig) {
      if (SystemUtils.IS_OS_WINDOWS) {
        System.out.println("\nAdd the following lines to the solr.in.cmd file so that the solr.cmd script can use subsequently.\n");
        System.out.println("set SOLR_AUTH_TYPE=kerberos\n"
            + "set SOLR_AUTHENTICATION_OPTS=\"" + kerberosConfig + "\"\n");
      } else {
        System.out.println("\nAdd the following lines to the solr.in.sh file so that the ./solr script can use subsequently.\n");
        System.out.println("SOLR_AUTH_TYPE=\"kerberos\"\n"
            + "SOLR_AUTHENTICATION_OPTS=\"" + kerberosConfig + "\"\n");
      }
    }

    /**
     * This will update the include file (e.g. solr.in.sh / solr.in.cmd) with the authentication parameters.
     * @param includeFile The include file
     * @param basicAuthConfFile  If basicAuth, the path of the file containing credentials. If not, null.
     * @param kerberosConfig If kerberos, the config string containing startup parameters. If not, null.
     */
    private void updateIncludeFileEnableAuth(File includeFile, String basicAuthConfFile, String kerberosConfig, CommandLine cli) throws IOException {
      assert !(basicAuthConfFile != null && kerberosConfig != null); // only one of the two needs to be populated
      List<String> includeFileLines = FileUtils.readLines(includeFile, StandardCharsets.UTF_8);
      for (int i=0; i<includeFileLines.size(); i++) {
        String line = includeFileLines.get(i);
        if (authenticationVariables.contains(line.trim().split("=")[0].trim())) { // Non-Windows
          includeFileLines.set(i, "# " + line);
        }
        if (line.trim().split("=")[0].trim().startsWith("set ")
            && authenticationVariables.contains(line.trim().split("=")[0].trim().substring(4))) { // Windows
          includeFileLines.set(i, "REM " + line);
        }
      }
      includeFileLines.add(""); // blank line

      if (basicAuthConfFile != null) { // for basicAuth
        if (SystemUtils.IS_OS_WINDOWS) {
          includeFileLines.add("REM The following lines added by solr.cmd for enabling BasicAuth");
          includeFileLines.add("set SOLR_AUTH_TYPE=basic");
          includeFileLines.add("set SOLR_AUTHENTICATION_OPTS=\"-Dsolr.httpclient.config=" + basicAuthConfFile + "\"");
        } else {
          includeFileLines.add("# The following lines added by ./solr for enabling BasicAuth");
          includeFileLines.add("SOLR_AUTH_TYPE=\"basic\"");
          includeFileLines.add("SOLR_AUTHENTICATION_OPTS=\"-Dsolr.httpclient.config=" + basicAuthConfFile + "\"");
        }
      } else { // for kerberos
        if (SystemUtils.IS_OS_WINDOWS) {
          includeFileLines.add("REM The following lines added by solr.cmd for enabling BasicAuth");
          includeFileLines.add("set SOLR_AUTH_TYPE=kerberos");
          includeFileLines.add("set SOLR_AUTHENTICATION_OPTS=\"-Dsolr.httpclient.config=" + basicAuthConfFile + "\"");
        } else {
          includeFileLines.add("# The following lines added by ./solr for enabling BasicAuth");
          includeFileLines.add("SOLR_AUTH_TYPE=\"kerberos\"");
          includeFileLines.add("SOLR_AUTHENTICATION_OPTS=\"" + kerberosConfig + "\"");
        }
      }
      FileUtils.writeLines(includeFile, StandardCharsets.UTF_8.name(), includeFileLines);

      if (basicAuthConfFile != null) {
        echoIfVerbose("Written out credentials file: " + basicAuthConfFile, cli);
      }
      echoIfVerbose("Updated Solr include file: " + includeFile.getAbsolutePath(), cli);
    }

    private void updateIncludeFileDisableAuth(File includeFile, CommandLine cli) throws IOException {
      List<String> includeFileLines = FileUtils.readLines(includeFile, StandardCharsets.UTF_8);
      boolean hasChanged = false;
      for (int i=0; i<includeFileLines.size(); i++) {
        String line = includeFileLines.get(i);
        if (authenticationVariables.contains(line.trim().split("=")[0].trim())) { // Non-Windows
          includeFileLines.set(i, "# " + line);
          hasChanged = true;
        }
        if (line.trim().split("=")[0].trim().startsWith("set ")
            && authenticationVariables.contains(line.trim().split("=")[0].trim().substring(4))) { // Windows
          includeFileLines.set(i, "REM " + line);
          hasChanged = true;
        }
      }
      if (hasChanged) {
        FileUtils.writeLines(includeFile, StandardCharsets.UTF_8.name(), includeFileLines);
        echoIfVerbose("Commented out necessary lines from " + includeFile.getAbsolutePath(), cli);
      }
    }
    @Override
    protected void runImpl(CommandLine cli) throws Exception {}
  }

  public static class UtilsTool extends ToolBase {
    private Path serverPath;
    private Path logsPath;
    private boolean beQuiet;

    public UtilsTool() { this(System.out); }
    public UtilsTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "utils";
    }

    public Option[] getOptions() {
      return new Option[]{
          Option.builder("s")
              .argName("path")
              .hasArg()
              .desc("Path to server dir. Required if logs path is relative.")
              .build(),
          Option.builder("l")
              .argName("path")
              .hasArg()
              .desc("Path to logs dir. If relative, also provide server dir with -s.")
              .build(),
          Option.builder("q")
              .desc("Be quiet, don't print to stdout, only return exit codes.")
              .build(),
          Option.builder("remove_old_solr_logs")
              .argName("daysToKeep")
              .hasArg()
              .type(Integer.class)
              .desc("Path to logs directory.")
              .build(),
          Option.builder("rotate_solr_logs")
              .argName("generations")
              .hasArg()
              .type(Integer.class)
              .desc("Rotate solr.log to solr.log.1 etc.")
              .build(),
          Option.builder("archive_gc_logs")
              .desc("Archive old garbage collection logs into archive/.")
              .build(),
          Option.builder("archive_console_logs")
              .desc("Archive old console logs into archive/.")
              .build()
      };
    }

    @Override
    public int runTool(CommandLine cli) throws Exception {
      if (cli.getOptions().length == 0 || cli.getArgs().length > 0 || cli.hasOption("h")) {
        new HelpFormatter().printHelp("bin/solr utils [OPTIONS]", getToolOptions(this));
        return 1;
      }
      if (cli.hasOption("s")) {
        serverPath = Paths.get(cli.getOptionValue("s"));
      }
      if (cli.hasOption("l")) {
        logsPath = Paths.get(cli.getOptionValue("l"));
      }
      if (cli.hasOption("q")) {
        beQuiet = cli.hasOption("q");
      }
      if (cli.hasOption("remove_old_solr_logs")) {
        if (removeOldSolrLogs(Integer.parseInt(cli.getOptionValue("remove_old_solr_logs"))) > 0) return 1;
      }
      if (cli.hasOption("rotate_solr_logs")) {
        if (rotateSolrLogs(Integer.parseInt(cli.getOptionValue("rotate_solr_logs"))) > 0) return 1;
      }
      if (cli.hasOption("archive_gc_logs")) {
        if (archiveGcLogs() > 0) return 1;
      }
      if (cli.hasOption("archive_console_logs")) {
        if (archiveConsoleLogs() > 0) return 1;
      }
      return 0;
    }

    /**
     * Moves gc logs into archived/
     * @return 0 on success
     * @throws Exception on failure
     */
    public int archiveGcLogs() throws Exception {
      prepareLogsPath();
      Path archivePath = logsPath.resolve("archived");
      if (!archivePath.toFile().exists()) {
        Files.createDirectories(archivePath);
      }
      List<Path> archived = Files.find(archivePath, 1, (f, a)
          -> a.isRegularFile() && String.valueOf(f.getFileName()).matches("^solr_gc[_.].+"))
          .collect(Collectors.toList());
      for (Path p : archived) {
        Files.delete(p);
      }
      List<Path> files = Files.find(logsPath, 1, (f, a)
          -> a.isRegularFile() && String.valueOf(f.getFileName()).matches("^solr_gc[_.].+"))
          .collect(Collectors.toList());
      if (files.size() > 0) {
        out("Archiving " + files.size() + " old GC log files to " + archivePath);
        for (Path p : files) {
          Files.move(p, archivePath.resolve(p.getFileName()), StandardCopyOption.REPLACE_EXISTING);
        }
      }
      return 0;
    }

    /**
     * Moves console log(s) into archiced/
     * @return 0 on success
     * @throws Exception on failure
     */
    public int archiveConsoleLogs() throws Exception {
      prepareLogsPath();
      Path archivePath = logsPath.resolve("archived");
      if (!archivePath.toFile().exists()) {
        Files.createDirectories(archivePath);
      }
      List<Path> archived = Files.find(archivePath, 1, (f, a)
          -> a.isRegularFile() && String.valueOf(f.getFileName()).endsWith("-console.log"))
          .collect(Collectors.toList());
      for (Path p : archived) {
        Files.delete(p);
      }
      List<Path> files = Files.find(logsPath, 1, (f, a)
          -> a.isRegularFile() && String.valueOf(f.getFileName()).endsWith("-console.log"))
          .collect(Collectors.toList());
      if (files.size() > 0) {
        out("Archiving " + files.size() + " console log files to " + archivePath);
        for (Path p : files) {
          Files.move(p, archivePath.resolve(p.getFileName()), StandardCopyOption.REPLACE_EXISTING);
        }
      }
      return 0;
    }

    /**
     * Rotates solr.log before starting Solr. Mimics log4j2 behavior, i.e. with generations=9:
     * <pre>
     *   solr.log.9 (and higher) are deleted
     *   solr.log.8 -&gt; solr.log.9
     *   solr.log.7 -&gt; solr.log.8
     *   ...
     *   solr.log   -&gt; solr.log.1
     * </pre>
     * @param generations number of generations to keep. Should agree with setting in log4j2.xml
     * @return 0 if success
     * @throws Exception if problems
     */
    public int rotateSolrLogs(int generations) throws Exception {
      prepareLogsPath();
      if (logsPath.toFile().exists() && logsPath.resolve("solr.log").toFile().exists()) {
        out("Rotating solr logs, keeping a max of "+generations+" generations.");
        try (Stream<Path> files = Files.find(logsPath, 1,
            (f, a) -> a.isRegularFile() && String.valueOf(f.getFileName()).startsWith("solr.log."))
            .sorted((b,a) -> Integer.valueOf(a.getFileName().toString().substring(9))
                .compareTo(Integer.valueOf(b.getFileName().toString().substring(9))))) {
          files.forEach(p -> {
            try {
              int number = Integer.parseInt(p.getFileName().toString().substring(9));
              if (number >= generations) {
                Files.delete(p);
              } else {
                Path renamed = p.getParent().resolve("solr.log." + (number + 1));
                Files.move(p, renamed);
              }
            } catch (IOException e) {
              out("Problem during rotation of log files: " + e.getMessage());
            }
          });
        } catch (NumberFormatException nfe) {
          throw new Exception("Do not know how to rotate solr.log.<ext> with non-numeric extension. Rotate aborted.", nfe);
        }
        Files.move(logsPath.resolve("solr.log"), logsPath.resolve("solr.log.1"));
      }

      return 0;
    }

    /**
     * Deletes time-stamped old solr logs, if older than n days
     * @param daysToKeep number of days logs to keep before deleting
     * @return 0 on success
     * @throws Exception on failure
     */
    public int removeOldSolrLogs(int daysToKeep) throws Exception {
      prepareLogsPath();
      if (logsPath.toFile().exists()) {
        try (Stream<Path> stream = Files.find(logsPath, 2, (f, a) -> a.isRegularFile()
            && Instant.now().minus(Period.ofDays(daysToKeep)).isAfter(a.lastModifiedTime().toInstant())
            && String.valueOf(f.getFileName()).startsWith("solr_log_"))) {
          List<Path> files = stream.collect(Collectors.toList());
          if (files.size() > 0) {
            out("Deleting "+files.size() + " solr_log_* files older than " + daysToKeep + " days.");
            for (Path p : files) {
              Files.delete(p);
            }
          }
        }
      }
      return 0;
    }

    // Private methods to follow

    private void out(String message) {
      if (!beQuiet) {
        stdout.print(message + "\n");
      }
    }

    private void prepareLogsPath() throws Exception {
      if (logsPath == null) {
        throw new Exception("Command requires the -l <log-directory> option");
      }
      if (!logsPath.isAbsolute()) {
        if (serverPath != null && serverPath.isAbsolute() && Files.exists(serverPath)) {
          logsPath = serverPath.resolve(logsPath);
        } else {
          throw new Exception("Logs directory must be an absolute path, or -s must be supplied.");
        }
      }
    }

    @Override
    protected void runImpl(CommandLine cli) throws Exception {
    }

    public void setLogPath(Path logsPath) {
      this.logsPath = logsPath;
    }

    public void setServerPath(Path serverPath) {
      this.serverPath = serverPath;
    }

    public void setQuiet(boolean shouldPrintStdout) {
      this.beQuiet = shouldPrintStdout;
    }
  } // end UtilsTool class
}

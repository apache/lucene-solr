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
import java.net.MalformedURLException;
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
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.Row;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
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
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.common.SolrException.ErrorCode.FORBIDDEN;
import static org.apache.solr.common.SolrException.ErrorCode.UNAUTHORIZED;
import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * Command-line utility for working with Solr.
 */
public class SolrCLI implements CLIO {
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
      this(CLIO.getOutStream());
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
          CLIO.err("\nERROR: " + excMsg + "\n");
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

      log.debug("Connecting to Solr cluster: " + zkHost);
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
        .withLongOpt("collection")
        .create("c"),
    OptionBuilder
        .isRequired(false)
        .withDescription("Enable more verbose command output.")
        .create("verbose")
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
      CLIO.err("Invalid command-line args! Must pass the name of a tool to run.\n"
          + "Supported tools:\n");
      displayToolOptions();
      exit(1);
    }

    if (args.length == 1 && Arrays.asList("-v","-version","version").contains(args[0])) {
      // Simple version tool, no need for its own class
      CLIO.out(Version.LATEST.toString());
      exit(0);
    }

    SSLConfigurationsFactory.current().init();

    Tool tool = findTool(args);
    CommandLine cli = parseCmdLine(args, tool.getOptions());
    System.exit(tool.runTool(cli));
  }

  public static Tool findTool(String[] args) throws Exception {
    String toolType = args[0].trim().toLowerCase(Locale.ROOT);
    return newTool(toolType);
  }

  public static CommandLine parseCmdLine(String[] args, Option[] toolOptions) throws Exception {
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
        processCommandLineArgs(joinCommonAndToolOptions(toolOptions), toolArgs);

    List argList = cli.getArgList();
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
      CLIO.err("WARNING: "+sysProp+" file "+keyStore+
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

    // If you add a built-in tool to this class, add it here to avoid
    // classpath scanning

    for (Class<Tool> next : findToolClassesInPackage("org.apache.solr.util")) {
      Tool tool = next.newInstance();
      if (toolType.equals(tool.getName()))
        return tool;
    }

    throw new IllegalArgumentException(toolType + " not supported!");
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
        CLIO.err("Failed to parse command-line arguments due to: "
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
      ClassLoader classLoader = SolrCLI.class.getClassLoader();
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


  public static class AutoscalingTool extends SolrCloudTool {
    static final String NODE_REDACTION_PREFIX = "N_";
    static final String COLL_REDACTION_PREFIX = "COLL_";

    public AutoscalingTool() {
      this(CLIO.getOutStream());
    }

    public AutoscalingTool(PrintStream stdout) {
      super(stdout);
    }

    @Override
    public Option[] getOptions() {
      return new Option[] {
          OptionBuilder
              .withArgName("HOST")
              .hasArg()
              .isRequired(false)
              .withDescription("Address of the Zookeeper ensemble; defaults to: "+ZK_HOST)
              .create("zkHost"),
          OptionBuilder
              .withArgName("CONFIG")
              .hasArg()
              .isRequired(false)
              .withDescription("Autoscaling config file, defaults to the one deployed in the cluster.")
              .withLongOpt("config")
              .create("a"),
          OptionBuilder
              .withDescription("Show calculated suggestions")
              .withLongOpt("suggestions")
              .create("s"),
          OptionBuilder
              .withDescription("Show ClusterState (collections layout)")
              .withLongOpt("clusterState")
              .create("c"),
          OptionBuilder
              .withDescription("Show calculated diagnostics")
              .withLongOpt("diagnostics")
              .create("d"),
          OptionBuilder
              .withDescription("Show sorted nodes with diagnostics")
              .withLongOpt("sortedNodes")
              .create("n"),
          OptionBuilder
              .withDescription("Redact node and collection names (original names will be consistently randomized)")
              .withLongOpt("redact")
              .create("r"),
          OptionBuilder
              .withDescription("Show summarized collection & node statistics.")
              .create("stats"),
          OptionBuilder
              .withDescription("Turn on all options to get all available information.")
              .create("all")

      };
    }

    @Override
    public String getName() {
      return "autoscaling";
    }

    @Override
    protected void runCloudTool(CloudSolrClient cloudSolrClient, CommandLine cli) throws Exception {
      DistributedQueueFactory dummmyFactory = new DistributedQueueFactory() {
        @Override
        public DistributedQueue makeQueue(String path) throws IOException {
          throw new UnsupportedOperationException("makeQueue");
        }

        @Override
        public void removeQueue(String path) throws IOException {
          throw new UnsupportedOperationException("removeQueue");
        }
      };
      try (SolrClientCloudManager clientCloudManager = new SolrClientCloudManager(dummmyFactory, cloudSolrClient)) {
        AutoScalingConfig config = null;
        HashSet<String> liveNodes = new HashSet<>();
        String configFile = cli.getOptionValue("a");
        if (configFile != null) {
          log.info("- reading autoscaling config from " + configFile);
          config = new AutoScalingConfig(IOUtils.toByteArray(new FileInputStream(configFile)));
        } else {
          log.info("- reading autoscaling config from the cluster.");
          config = clientCloudManager.getDistribStateManager().getAutoScalingConfig();
        }
        log.info("- calculating suggestions...");
        long start = TimeSource.NANO_TIME.getTimeNs();
        // collect live node names for optional redaction
        liveNodes.addAll(clientCloudManager.getClusterStateProvider().getLiveNodes());
        List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(config, clientCloudManager);
        long end = TimeSource.NANO_TIME.getTimeNs();
        log.info("  (took " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms)");
        log.info("- calculating diagnostics...");
        start = TimeSource.NANO_TIME.getTimeNs();
        // update the live nodes
        liveNodes.addAll(clientCloudManager.getClusterStateProvider().getLiveNodes());
        Policy.Session session = config.getPolicy().createSession(clientCloudManager);
        MapWriter mw = PolicyHelper.getDiagnostics(session);
        Map<String, Object> diagnostics = new LinkedHashMap<>();
        mw.toMap(diagnostics);
        end = TimeSource.NANO_TIME.getTimeNs();
        log.info("  (took " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms)");
        boolean withSuggestions = cli.hasOption("s");
        boolean withDiagnostics = cli.hasOption("d") || cli.hasOption("n");
        boolean withSortedNodes = cli.hasOption("n");
        boolean withClusterState = cli.hasOption("c");
        boolean withStats = cli.hasOption("stats");
        boolean redact = cli.hasOption("r");
        if (cli.hasOption("all")) {
          withSuggestions = true;
          withDiagnostics = true;
          withSortedNodes = true;
          withClusterState = true;
          withStats = true;
        }
        // prepare to redact also host names / IPs in base_url and other properties
        Set<String> redactNames = new HashSet<>();
        for (String nodeName : liveNodes) {
          String urlString = Utils.getBaseUrlForNodeName(nodeName, "http");
          try {
            URL u = new URL(urlString);
            // protocol format
            redactNames.add(u.getHost() + ":" + u.getPort());
            // node name format
            redactNames.add(u.getHost() + "_" + u.getPort() + "_");
          } catch (MalformedURLException e) {
            log.warn("Invalid URL for node name " + nodeName + ", replacing including protocol and path", e);
            redactNames.add(urlString);
            redactNames.add(Utils.getBaseUrlForNodeName(nodeName, "https"));
          }
        }
        // redact collection names too
        Set<String> redactCollections = new HashSet<>();
        ClusterState clusterState = clientCloudManager.getClusterStateProvider().getClusterState();
        clusterState.forEachCollection(coll -> redactCollections.add(coll.getName()));
        if (!withSuggestions && !withDiagnostics) {
          withSuggestions = true;
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
          Map<String, Map<String, Number>> collStats = new TreeMap<>();
          clusterState.forEachCollection(coll -> {
            Map<String, Number> perColl = collStats.computeIfAbsent(coll.getName(), n -> new LinkedHashMap<>());
            AtomicInteger numCores = new AtomicInteger();
            HashMap<String, Map<String, AtomicInteger>> nodes = new HashMap<>();
            coll.getSlices().forEach(s -> {
              numCores.addAndGet(s.getReplicas().size());
              s.getReplicas().forEach(r -> {
                nodes.computeIfAbsent(r.getNodeName(), n -> new HashMap<>())
                    .computeIfAbsent(s.getName(), slice -> new AtomicInteger()).incrementAndGet();
              });
            });
            int maxCoresPerNode = 0;
            int minCoresPerNode = 0;
            int maxActualShardsPerNode = 0;
            int minActualShardsPerNode = 0;
            int maxShardReplicasPerNode = 0;
            int minShardReplicasPerNode = 0;
            if (!nodes.isEmpty()) {
              minCoresPerNode = Integer.MAX_VALUE;
              minActualShardsPerNode = Integer.MAX_VALUE;
              minShardReplicasPerNode = Integer.MAX_VALUE;
              for (Map<String, AtomicInteger> counts : nodes.values()) {
                int total = counts.values().stream().mapToInt(c -> c.get()).sum();
                for (AtomicInteger count : counts.values()) {
                  if (count.get() > maxShardReplicasPerNode) {
                    maxShardReplicasPerNode = count.get();
                  }
                  if (count.get() < minShardReplicasPerNode) {
                    minShardReplicasPerNode = count.get();
                  }
                }
                if (total > maxCoresPerNode) {
                  maxCoresPerNode = total;
                }
                if (total < minCoresPerNode) {
                  minCoresPerNode = total;
                }
                if (counts.size() > maxActualShardsPerNode) {
                  maxActualShardsPerNode = counts.size();
                }
                if (counts.size() < minActualShardsPerNode) {
                  minActualShardsPerNode = counts.size();
                }
              }
            }
            perColl.put("activeShards", coll.getActiveSlices().size());
            perColl.put("inactiveShards", coll.getSlices().size() - coll.getActiveSlices().size());
            perColl.put("rf", coll.getReplicationFactor());
            perColl.put("maxShardsPerNode", coll.getMaxShardsPerNode());
            perColl.put("maxActualShardsPerNode", maxActualShardsPerNode);
            perColl.put("minActualShardsPerNode", minActualShardsPerNode);
            perColl.put("maxShardReplicasPerNode", maxShardReplicasPerNode);
            perColl.put("minShardReplicasPerNode", minShardReplicasPerNode);
            perColl.put("numCores", numCores.get());
            perColl.put("numNodes", nodes.size());
            perColl.put("maxCoresPerNode", maxCoresPerNode);
            perColl.put("minCoresPerNode", minCoresPerNode);
          });
          Map<String, Map<String, Object>> nodeStats = new TreeMap<>();
          Map<Integer, AtomicInteger> coreStats = new TreeMap<>();
          for (Row row : session.getSortedNodes()) {
            Map<String, Object> nodeStat = nodeStats.computeIfAbsent(row.node, n -> new LinkedHashMap<>());
            nodeStat.put("isLive", row.isLive());
            nodeStat.put("freedisk", row.getVal("freedisk", 0));
            nodeStat.put("totaldisk", row.getVal("totaldisk", 0));
            int cores = ((Number)row.getVal("cores", 0)).intValue();
            nodeStat.put("cores", cores);
            coreStats.computeIfAbsent(cores, num -> new AtomicInteger()).incrementAndGet();
            Map<String, Map<String, Map<String, Object>>> collReplicas = new TreeMap<>();
            row.forEachReplica(ri -> {
              Map<String, Object> perReplica = collReplicas.computeIfAbsent(ri.getCollection(), c -> new TreeMap<>())
                  .computeIfAbsent(ri.getCore().substring(ri.getCollection().length() + 1), core -> new LinkedHashMap<>());
              perReplica.put("INDEX.sizeInGB", ri.getVariable("INDEX.sizeInGB"));
              perReplica.put("coreNode", ri.getName());
              if (ri.getBool("leader", false)) {
                perReplica.put("leader", true);
                Double totalSize = (Double)collStats.computeIfAbsent(ri.getCollection(), c -> new HashMap<>())
                    .computeIfAbsent("avgShardSize", size -> 0.0);
                Number riSize = (Number)ri.getVariable("INDEX.sizeInGB");
                if (riSize != null) {
                  totalSize += riSize.doubleValue();
                  collStats.get(ri.getCollection()).put("avgShardSize", totalSize);
                  Double max = (Double)collStats.get(ri.getCollection()).get("maxShardSize");
                  if (max == null) max = 0.0;
                  if (riSize.doubleValue() > max) {
                    collStats.get(ri.getCollection()).put("maxShardSize", riSize.doubleValue());
                  }
                  Double min = (Double)collStats.get(ri.getCollection()).get("minShardSize");
                  if (min == null) min = Double.MAX_VALUE;
                  if (riSize.doubleValue() < min) {
                    collStats.get(ri.getCollection()).put("minShardSize", riSize.doubleValue());
                  }
                }
              }
              nodeStat.put("replicas", collReplicas);
            });
          }

          // calculate average per shard
          for (Map<String, Number> perColl : collStats.values()) {
            Double avg = (Double)perColl.get("avgShardSize");
            if (avg != null) {
              avg = avg / ((Number)perColl.get("activeShards")).doubleValue();
              perColl.put("avgShardSize", avg);
            }
          }
          Map<String, Object> stats = new LinkedHashMap<>();
          results.put("STATISTICS", stats);
          stats.put("coresPerNodes", coreStats);
          stats.put("nodeStats", nodeStats);
          stats.put("collectionStats", collStats);
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
        String data = Utils.toJSONString(results);
        if (redact) {
          data = RedactionUtils.redactNames(redactCollections, COLL_REDACTION_PREFIX, data);
          data = RedactionUtils.redactNames(redactNames, NODE_REDACTION_PREFIX, data);
        }
        stdout.println(data);
      }
    }
  }

  /**
   * Get the status of a Solr server.
   */
  public static class StatusTool extends ToolBase {

    public StatusTool() { this(CLIO.getOutStream()); }
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
          if (exceptionIsAuthRelated(exc)) {
            throw exc;
          }
          if (checkCommunicationError(exc)) {
            // this is not actually an error from the tool as it's ok if Solr is not online.
            CLIO.err("Solr at "+solrUrl+" not online.");
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

    public ApiTool() { this(CLIO.getOutStream()); }
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

    public HealthcheckTool() { this(CLIO.getOutStream()); }
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

      log.debug("Running healthcheck for "+collection);

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
            .create("configsetsDir"),
        OptionBuilder
            .isRequired(false)
            .withDescription("Enable more verbose command output.")
            .create("verbose")

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
      boolean wait = false;
      final long startWaitAt = System.nanoTime();
      do{
        if (wait) {
          final int clamPeriodForStatusPollMs = 1000;
          Thread.sleep(clamPeriodForStatusPollMs);
        }
        Map<String,Object> existsCheckResult = getJson(coreStatusUrl);
        Map<String,Object> status = (Map<String, Object>)existsCheckResult.get("status");
        Map<String,Object> coreStatus = (Map<String, Object>)status.get(coreName);
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
      this(CLIO.getOutStream());
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

    public CreateCoreTool() { this(CLIO.getOutStream()); }
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
              .create("configsetsDir"),
              OptionBuilder
              .isRequired(false)
              .withDescription("Enable more verbose command output.")
              .create("verbose")
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

    public CreateTool() { this(CLIO.getOutStream()); }
    public CreateTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "create";
    }

    @SuppressWarnings("static-access")
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
      this(CLIO.getOutStream());
    }

    public ConfigSetUploadTool(PrintStream stdout) {
      super(stdout);
    }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[]{
          OptionBuilder
              .withArgName("confname") // Comes out in help message
              .hasArg() // Has one sub-argument
              .isRequired(true) // confname argument must be present
              .withDescription("Configset name on Zookeeper")
              .create("confname"), // passed as -confname value
          OptionBuilder
              .withArgName("confdir")
              .hasArg()
              .isRequired(true)
              .withDescription("Local directory with configs")
              .create("confdir"),
          OptionBuilder
              .withArgName("configsetsDir")
              .hasArg()
              .isRequired(false)
              .withDescription("Parent directory of example configsets")
              .create("configsetsDir"),
          OptionBuilder
              .withArgName("HOST")
              .hasArg()
              .isRequired(true)
              .withDescription("Address of the Zookeeper ensemble; defaults to: " + ZK_HOST)
              .create("zkHost"),
          OptionBuilder
              .isRequired(false)
              .withDescription("Enable more verbose command output.")
              .create("verbose")
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
        log.error("Could not complete upconfig operation for reason: " + e.getMessage());
        throw (e);
      }
    }
  }

  public static class ConfigSetDownloadTool extends ToolBase {

    public ConfigSetDownloadTool() {
      this(CLIO.getOutStream());
    }

    public ConfigSetDownloadTool(PrintStream stdout) {
      super(stdout);
    }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[]{
          OptionBuilder
              .withArgName("confname")
              .hasArg()
              .isRequired(true)
              .withDescription("Configset name on Zookeeper")
              .create("confname"),
          OptionBuilder
              .withArgName("confdir")
              .hasArg()
              .isRequired(true)
              .withDescription("Local directory with configs")
              .create("confdir"),
          OptionBuilder
              .withArgName("HOST")
              .hasArg()
              .isRequired(true)
              .withDescription("Address of the Zookeeper ensemble; defaults to: " + ZK_HOST)
              .create("zkHost"),
          OptionBuilder
              .isRequired(false)
              .withDescription("Enable more verbose command output.")
              .create("verbose")
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
        log.error("Could not complete downconfig operation for reason: " + e.getMessage());
        throw (e);
      }

    }

  } // End ConfigSetDownloadTool class

  public static class ZkRmTool extends ToolBase {

    public ZkRmTool() {
      this(CLIO.getOutStream());
      }

    public ZkRmTool(PrintStream stdout) {
      super(stdout);
      }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[]{
          OptionBuilder
              .withArgName("path")
              .hasArg()
              .isRequired(true)
              .withDescription("Path to remove")
              .create("path"),
          OptionBuilder
              .withArgName("recurse")
              .hasArg()
              .isRequired(false)
              .withDescription("Recurse (true|false, default is false)")
              .create("recurse"),
          OptionBuilder
              .withArgName("HOST")
              .hasArg()
              .isRequired(true)
              .withDescription("Address of the Zookeeper ensemble; defaults to: " + ZK_HOST)
              .create("zkHost"),
          OptionBuilder
              .isRequired(false)
              .withDescription("Enable more verbose command output.")
              .create("verbose")
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
          throw new SolrServerException("Zookeeper node " + znode + " has children and recurse has NOT been specified");
        }
        echo("Removing Zookeeper node " + znode + " from ZooKeeper at " + zkHost +
            " recurse: " + Boolean.toString(recurse));
        zkClient.clean(znode);
      } catch (Exception e) {
        log.error("Could not complete rm operation for reason: " + e.getMessage());
        throw (e);
      }

    }

  } // End RmTool class

  public static class ZkLsTool extends ToolBase {

    public ZkLsTool() {
      this(CLIO.getOutStream());
    }

    public ZkLsTool(PrintStream stdout) {
      super(stdout);
    }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[]{
          OptionBuilder
              .withArgName("path")
              .hasArg()
              .isRequired(true)
              .withDescription("Path to list")
              .create("path"),
          OptionBuilder
              .withArgName("recurse")
              .hasArg()
              .isRequired(false)
              .withDescription("Recurse (true|false, default is false)")
              .create("recurse"),
          OptionBuilder
              .withArgName("HOST")
              .hasArg()
              .isRequired(true)
              .withDescription("Address of the Zookeeper ensemble; defaults to: " + ZK_HOST)
              .create("zkHost"),
          OptionBuilder
              .isRequired(false)
              .withDescription("Enable more verbose command output.")
              .create("verbose")
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
        echoIfVerbose("Getting listing for Zookeeper node " + znode + " from ZooKeeper at " + zkHost +
            " recurse: " + Boolean.toString(recurse), cli);
        stdout.print(zkClient.listZnode(znode, recurse));
      } catch (Exception e) {
        log.error("Could not complete ls operation for reason: " + e.getMessage());
        throw (e);
      }
    }
  } // End zkLsTool class


  public static class ZkMkrootTool extends ToolBase {

    public ZkMkrootTool() {
      this(CLIO.getOutStream());
    }

    public ZkMkrootTool(PrintStream stdout) {
      super(stdout);
    }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[]{
          OptionBuilder
              .withArgName("path")
              .hasArg()
              .isRequired(true)
              .withDescription("Path to create")
              .create("path"),
          OptionBuilder
              .withArgName("HOST")
              .hasArg()
              .isRequired(true)
              .withDescription("Address of the Zookeeper ensemble; defaults to: " + ZK_HOST)
              .create("zkHost"),
          OptionBuilder
              .isRequired(false)
              .withDescription("Enable more verbose command output.")
              .create("verbose")
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
        echo("Creating Zookeeper path " + znode + " on ZooKeeper at " + zkHost);
        zkClient.makePath(znode, true);
      } catch (Exception e) {
        log.error("Could not complete mkroot operation for reason: " + e.getMessage());
        throw (e);
      }
    }
  } // End zkMkrootTool class




  public static class ZkCpTool extends ToolBase {

    public ZkCpTool() {
      this(CLIO.getOutStream());
    }

    public ZkCpTool(PrintStream stdout) {
      super(stdout);
    }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[]{
          OptionBuilder
              .withArgName("src")
              .hasArg()
              .isRequired(true)
              .withDescription("Source file or directory, may be local or a Znode")
              .create("src"),
          OptionBuilder
              .withArgName("dst")
              .hasArg()
              .isRequired(true)
              .withDescription("Destination of copy, may be local or a Znode.")
              .create("dst"),
          OptionBuilder
              .withArgName("recurse")
              .hasArg()
              .isRequired(false)
              .withDescription("Recurse (true|false, default is false)")
              .create("recurse"),
          OptionBuilder
              .withArgName("HOST")
              .hasArg()
              .isRequired(true)
              .withDescription("Address of the Zookeeper ensemble; defaults to: " + ZK_HOST)
              .create("zkHost"),
          OptionBuilder
              .isRequired(false)
              .withDescription("Enable more verbose command output.")
              .create("verbose")
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
        log.error("Could not complete the zk operation for reason: " + e.getMessage());
        throw (e);
      }
    }
  } // End CpTool class


  public static class ZkMvTool extends ToolBase {

    public ZkMvTool() {
      this(CLIO.getOutStream());
    }

    public ZkMvTool(PrintStream stdout) {
      super(stdout);
    }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[]{
          OptionBuilder
              .withArgName("src")
              .hasArg()
              .isRequired(true)
              .withDescription("Source Znode to movej from.")
              .create("src"),
          OptionBuilder
              .withArgName("dst")
              .hasArg()
              .isRequired(true)
              .withDescription("Destination Znode to move to.")
              .create("dst"),
          OptionBuilder
              .withArgName("HOST")
              .hasArg()
              .isRequired(true)
              .withDescription("Address of the Zookeeper ensemble; defaults to: " + ZK_HOST)
              .create("zkHost"),
          OptionBuilder
              .isRequired(false)
              .withDescription("Enable more verbose command output.")
              .create("verbose")
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
        log.error("Could not complete mv operation for reason: " + e.getMessage());
        throw (e);
      }

    }
  } // End MvTool class



  public static class DeleteTool extends ToolBase {

    public DeleteTool() { this(CLIO.getOutStream()); }
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
              .create("zkHost"),
          OptionBuilder
              .isRequired(false)
              .withDescription("Enable more verbose command output.")
              .create("verbose")
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
          log.warn("Skipping safety checks, configuration directory "+configName+" will be deleted with impunity.");
        } else {
          // need to scan all Collections to see if any are using the config
          Set<String> collections = zkStateReader.getClusterState().getCollectionsMap().keySet();

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

    public ConfigTool() { this(CLIO.getOutStream()); }
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
              .create("solrUrl"),
          OptionBuilder
              .withArgName("HOST")
              .hasArg()
              .isRequired(false)
              .withDescription("Address of the Zookeeper ensemble")
              .withLongOpt("zkHost")
              .create('z'),
          OptionBuilder
              .withArgName("PORT")
              .hasArg()
              .isRequired(false)
              .withDescription("The port of the Solr node to use when applying configuration change")
              .withLongOpt("port")
              .create('p'),
          OptionBuilder
              .withArgName("SCHEME")
              .hasArg()
              .isRequired(false)
              .withDescription("The scheme for accessing Solr.  Accepted values: http or https.  Default: http")
              .withLongOpt("scheme")
              .create('s')
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

      try (SolrClient solrClient = new Builder(solrUrl).build()) {
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
      this(null, System.in, CLIO.getOutStream());
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
            .withArgName("FORCE")
            .withDescription("Force option in case Solr is run as root")
            .create("force"),
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
            createTool.runTool(processCommandLineArgs(joinCommonAndToolOptions(createTool.getOptions()), createArgs));
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

      Scanner readInput = prompt ? new Scanner(userInput, UTF_8.name()) : null;
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
        CLIO.err("Failed to update '"+propName+"' property due to: "+exc);
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
        CLIO.err("Failed to see if "+numNodes+" joined the SolrCloud cluster due to: "+exc);
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
          for (String envVar : procEnv.keySet()) {
            String envVarVal = procEnv.get(envVar);
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

    public AssertTool() { this(CLIO.getOutStream()); }
    public AssertTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "assert";
    }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[] {
          OptionBuilder
              .withDescription("Asserts that we are NOT the root user")
              .withLongOpt("not-root")
              .create("R"),
          OptionBuilder
              .withDescription("Asserts that we are the root user")
              .withLongOpt("root")
              .create("r"),
          OptionBuilder
              .withDescription("Asserts that Solr is NOT running on a certain URL. Default timeout is 1000ms")
              .withLongOpt("not-started")
              .hasArg(true)
              .withArgName("url")
              .create("S"),
          OptionBuilder
              .withDescription("Asserts that Solr is running on a certain URL. Default timeout is 1000ms")
              .withLongOpt("started")
              .hasArg(true)
              .withArgName("url")
              .create("s"),
          OptionBuilder
              .withDescription("Asserts that we run as same user that owns <directory>")
              .withLongOpt("same-user")
              .hasArg(true)
              .withArgName("directory")
              .create("u"),
          OptionBuilder
              .withDescription("Asserts that directory <directory> exists")
              .withLongOpt("exists")
              .hasArg(true)
              .withArgName("directory")
              .create("x"),
          OptionBuilder
              .withDescription("Asserts that directory <directory> does NOT exist")
              .withLongOpt("not-exists")
              .hasArg(true)
              .withArgName("directory")
              .create("X"),
          OptionBuilder
              .withDescription("Asserts that Solr is running in cloud mode.  Also fails if Solr not running.  URL should be for root Solr path.")
              .withLongOpt("cloud")
              .hasArg(true)
              .withArgName("url")
              .create("c"),
          OptionBuilder
              .withDescription("Asserts that Solr is not running in cloud mode.  Also fails if Solr not running.  URL should be for root Solr path.")
              .withLongOpt("not-cloud")
              .hasArg(true)
              .withArgName("url")
              .create("C"),
          OptionBuilder
              .withDescription("Exception message to be used in place of the default error message")
              .withLongOpt("message")
              .hasArg(true)
              .withArgName("message")
              .create("m"),
          OptionBuilder
              .withDescription("Timeout in ms for commands supporting a timeout")
              .withLongOpt("timeout")
              .hasArg(true)
              .withType(Long.class)
              .withArgName("ms")
              .create("t"),
          OptionBuilder
              .withDescription("Return an exit code instead of printing error message on assert fail.")
              .withLongOpt("exitcode")
              .create("e")
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
          CLIO.err("\nERROR: " + excMsg + "\n");
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
        log.debug("Opening connection to " + url + " failed, Solr does not seem to be running", e);
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
    public AuthTool() { this(CLIO.getOutStream()); }
    public AuthTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "auth";
    }

    List<String> authenticationVariables = Arrays.asList("SOLR_AUTHENTICATION_CLIENT_BUILDER", "SOLR_AUTH_TYPE", "SOLR_AUTHENTICATION_OPTS");

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[]{
          OptionBuilder
          .withArgName("type")
          .hasArg()
          .withDescription("The authentication mechanism to enable (basicAuth or kerberos). Defaults to 'basicAuth'.")
          .create("type"),
          OptionBuilder
          .withArgName("credentials")
          .hasArg()
          .withDescription("Credentials in the format username:password. Example: -credentials solr:SolrRocks")
          .create("credentials"),
          OptionBuilder
          .withArgName("prompt")
          .hasArg()
          .withDescription("Prompts the user to provide the credentials. Use either -credentials or -prompt, not both")
          .create("prompt"),
          OptionBuilder
          .withArgName("config")
          .hasArgs()
          .withDescription("Configuration parameters (Solr startup parameters). Required for Kerberos authentication")
          .create("config"),
          OptionBuilder
          .withArgName("blockUnknown")
          .withDescription("Blocks all access for unknown users (requires authentication for all endpoints)")
          .hasArg()
          .create("blockUnknown"),
          OptionBuilder
          .withArgName("solrIncludeFile")
          .hasArg()
          .withDescription("The Solr include file which contains overridable environment variables for configuring Solr configurations")
          .create("solrIncludeFile"),
          OptionBuilder
          .withArgName("updateIncludeFileOnly")
          .withDescription("Only update the solr.in.sh or solr.in.cmd file, and skip actual enabling/disabling"
              + " authentication (i.e. don't update security.json)")
          .hasArg()
          .create("updateIncludeFileOnly"),
          OptionBuilder
          .withArgName("authConfDir")
          .hasArg()
          .isRequired()
          .withDescription("This is where any authentication related configuration files, if any, would be placed.")
          .create("authConfDir"),
          OptionBuilder
          .withArgName("solrUrl")
          .hasArg()
          .withDescription("Solr URL")
          .create("solrUrl"),
          OptionBuilder
          .withArgName("zkHost")
          .hasArg()
          .withDescription("ZooKeeper host")
          .create("zkHost"),
          OptionBuilder
          .isRequired(false)
          .withDescription("Enable more verbose command output.")
          .create("verbose")
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
          CLIO.out("Only type=basicAuth or kerberos supported at the moment.");
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
              CLIO.out("Unable to access ZooKeeper. Please add the following security.json to ZooKeeper (in case of SolrCloud):\n"
                    + securityJson + "\n");
              zkInaccessible = true;
            }
            if (zkHost == null) {
              if (zkInaccessible == false) {
                CLIO.out("Unable to access ZooKeeper. Please add the following security.json to ZooKeeper (in case of SolrCloud):\n"
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
                    CLIO.out("Security is already enabled. You can disable it with 'bin/solr auth disable'. Existing security.json: \n"
                        + new String(oldSecurityBytes, StandardCharsets.UTF_8));
                    exit(1);
                  }
                }
              } catch (Exception ex) {
                if (zkInaccessible == false) {
                  CLIO.out("Unable to access ZooKeeper. Please add the following security.json to ZooKeeper (in case of SolrCloud):\n"
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
                  CLIO.out("Unable to access ZooKeeper. Please add the following security.json to ZooKeeper (in case of SolrCloud):\n"
                      + securityJson);
                  zkInaccessible = true;
                }
              }
            }
          }

          String config = StrUtils.join(Arrays.asList(cli.getOptionValues("config")), ' ');
          // config is base64 encoded (to get around parsing problems), decode it
          config = config.replaceAll(" ", "");
          config = new String(Base64.getDecoder().decode(config.getBytes("UTF-8")), "UTF-8");
          config = config.replaceAll("\n", "").replaceAll("\r", "");

          String solrIncludeFilename = cli.getOptionValue("solrIncludeFile");
          File includeFile = new File(solrIncludeFilename);
          if (includeFile.exists() == false || includeFile.canWrite() == false) {
            CLIO.out("Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
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
              stdout.print("ZK Host not found. Solr should be running in cloud mode");
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
            CLIO.out("Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
            CLIO.out("Security has been disabled. Please remove any SOLR_AUTH_TYPE or SOLR_AUTHENTICATION_OPTS configuration from solr.in.sh/solr.in.cmd.\n");
            System.exit(0);
          }

          // update the solr.in.sh file to comment out the necessary authentication lines
          updateIncludeFileDisableAuth(includeFile, cli);
          return 0;

        default:
          CLIO.out("Valid auth commands are: enable, disable");
          exit(1);
      }

      CLIO.out("Options not understood.");
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
            CLIO.out("Option -credentials or -prompt is required with enable.");
            new HelpFormatter().printHelp("bin/solr auth <enable|disable> [OPTIONS]", getToolOptions(this));
            exit(1);
          } else if (!prompt && (cli.getOptionValue("credentials") == null || !cli.getOptionValue("credentials").contains(":"))) {
            CLIO.out("Option -credentials is not in correct format.");
            new HelpFormatter().printHelp("bin/solr auth <enable|disable> [OPTIONS]", getToolOptions(this));
            exit(1);
          }

          String zkHost = null;

          if (!updateIncludeFileOnly) {
            try {
              zkHost = getZkHost(cli);
            } catch (Exception ex) {
              if (cli.hasOption("zkHost")) {
                CLIO.out("Couldn't get ZooKeeper host. Please make sure that ZooKeeper is running and the correct zkHost has been passed in.");
              } else {
                CLIO.out("Couldn't get ZooKeeper host. Please make sure Solr is running in cloud mode, or a zkHost has been passed in.");
              }
              exit(1);
            }
            if (zkHost == null) {
              if (cli.hasOption("zkHost")) {
                CLIO.out("Couldn't get ZooKeeper host. Please make sure that ZooKeeper is running and the correct zkHost has been passed in.");
              } else {
                CLIO.out("Couldn't get ZooKeeper host. Please make sure Solr is running in cloud mode, or a zkHost has been passed in.");
              }
              exit(1);
            }

            // check if security is already enabled or not
            try (SolrZkClient zkClient = new SolrZkClient(zkHost, 10000)) {
              if (zkClient.exists("/security.json", true)) {
                byte oldSecurityBytes[] = zkClient.getData("/security.json", null, null, true);
                if (!"{}".equals(new String(oldSecurityBytes, StandardCharsets.UTF_8).trim())) {
                  CLIO.out("Security is already enabled. You can disable it with 'bin/solr auth disable'. Existing security.json: \n"
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
            username = console.readLine("Enter username: ");
            password = new String(console.readPassword("Enter password: "));
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
              "\n {\"name\":\"collection-admin-edit\", \"role\":\"admin\"}," +
              "\n {\"name\":\"core-admin-edit\", \"role\":\"admin\"}" +
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
            CLIO.out("Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
            printAuthEnablingInstructions(username, password);
            System.exit(0);
          }
          String authConfDir = cli.getOptionValue("authConfDir");
          File basicAuthConfFile = new File(authConfDir + File.separator + "basicAuth.conf");

          if (basicAuthConfFile.getParentFile().canWrite() == false) {
            CLIO.out("Cannot write to file: " + basicAuthConfFile.getAbsolutePath());
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
              stdout.print("ZK Host not found. Solr should be running in cloud mode");
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
            CLIO.out("Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
            CLIO.out("Security has been disabled. Please remove any SOLR_AUTH_TYPE or SOLR_AUTHENTICATION_OPTS configuration from solr.in.sh/solr.in.cmd.\n");
            System.exit(0);
          }

          // update the solr.in.sh file to comment out the necessary authentication lines
          updateIncludeFileDisableAuth(includeFile, cli);
          return 0;

        default:
          CLIO.out("Valid auth commands are: enable, disable");
          exit(1);
      }

      CLIO.out("Options not understood.");
      new HelpFormatter().printHelp("bin/solr auth <enable|disable> [OPTIONS]", getToolOptions(this));
      return 1;
    }
    private void printAuthEnablingInstructions(String username, String password) {
      if (SystemUtils.IS_OS_WINDOWS) {
        CLIO.out("\nAdd the following lines to the solr.in.cmd file so that the solr.cmd script can use subsequently.\n");
        CLIO.out("set SOLR_AUTH_TYPE=basic\n"
            + "set SOLR_AUTHENTICATION_OPTS=\"-Dbasicauth=" + username + ":" + password + "\"\n");
      } else {
        CLIO.out("\nAdd the following lines to the solr.in.sh file so that the ./solr script can use subsequently.\n");
        CLIO.out("SOLR_AUTH_TYPE=\"basic\"\n"
            + "SOLR_AUTHENTICATION_OPTS=\"-Dbasicauth=" + username + ":" + password + "\"\n");
      }
    }
    private void printAuthEnablingInstructions(String kerberosConfig) {
      if (SystemUtils.IS_OS_WINDOWS) {
        CLIO.out("\nAdd the following lines to the solr.in.cmd file so that the solr.cmd script can use subsequently.\n");
        CLIO.out("set SOLR_AUTH_TYPE=kerberos\n"
            + "set SOLR_AUTHENTICATION_OPTS=\"" + kerberosConfig + "\"\n");
      } else {
        CLIO.out("\nAdd the following lines to the solr.in.sh file so that the ./solr script can use subsequently.\n");
        CLIO.out("SOLR_AUTH_TYPE=\"kerberos\"\n"
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

    public UtilsTool() { this(CLIO.getOutStream()); }
    public UtilsTool(PrintStream stdout) { super(stdout); }

    public String getName() {
      return "utils";
    }

    @SuppressWarnings("static-access")
    public Option[] getOptions() {
      return new Option[]{
          OptionBuilder
              .withArgName("path")
              .hasArg()
              .withDescription("Path to server dir. Required if logs path is relative")
              .create("s"),
          OptionBuilder
              .withArgName("path")
              .hasArg()
              .withDescription("Path to logs dir. If relative, also provide server dir with -s")
              .create("l"),
          OptionBuilder
              .withDescription("Be quiet, don't print to stdout, only return exit codes")
              .create("q"),
          OptionBuilder
              .withArgName("daysToKeep")
              .hasArg()
              .withType(Integer.class)
              .withDescription("Path to logs directory")
              .create("remove_old_solr_logs"),
          OptionBuilder
              .withArgName("generations")
              .hasArg()
              .withType(Integer.class)
              .withDescription("Rotate solr.log to solr.log.1 etc")
              .create("rotate_solr_logs"),
          OptionBuilder
              .withDescription("Archive old garbage collection logs into archive/")
              .create("archive_gc_logs"),
          OptionBuilder
              .withDescription("Archive old console logs into archive/")
              .create("archive_console_logs")
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
        out("Rotating solr logs, keeping a max of "+generations+" generations");
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
          throw new Exception("Logs directory must be an absolute path, or -s must be supplied");
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

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

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A simple utility class for posting raw updates to a Solr server, 
 * has a main method so it can be run on the command line.
 * View this not as a best-practice code example, but as a standalone 
 * example built with an explicit purpose of not having external
 * jar dependencies.
 */
public class SimplePostTool {
  private static final String DEFAULT_POST_HOST = "localhost";
  private static final String DEFAULT_POST_PORT = "8983";
  private static final String VERSION_OF_THIS_TOOL = "5.0.0";  // TODO: hardcoded for now, but eventually to sync with actual Solr version

  private static final String DEFAULT_COMMIT = "yes";
  private static final String DEFAULT_OPTIMIZE = "no";
  private static final String DEFAULT_OUT = "no";
  private static final String DEFAULT_AUTO = "no";
  private static final String DEFAULT_RECURSIVE = "0";
  private static final int DEFAULT_WEB_DELAY = 10;
  private static final int MAX_WEB_DEPTH = 10;
  private static final String DEFAULT_CONTENT_TYPE = "application/xml";
  private static final String DEFAULT_FILE_TYPES = "xml,json,jsonl,csv,pdf,doc,docx,ppt,pptx,xls,xlsx,odt,odp,ods,ott,otp,ots,rtf,htm,html,txt,log";
  private static final String BASIC_AUTH = "basicauth";

  static final String DATA_MODE_FILES = "files";
  static final String DATA_MODE_ARGS = "args";
  static final String DATA_MODE_STDIN = "stdin";
  static final String DATA_MODE_WEB = "web";
  static final String DEFAULT_DATA_MODE = DATA_MODE_FILES;

  // Input args
  boolean auto = false;
  int recursive = 0;
  int delay = 0;
  String fileTypes;
  URL solrUrl;
  OutputStream out = null;
  String type;
  String format;
  String mode;
  boolean commit;
  boolean optimize;
  String[] args;

  private int currentDepth;

  static HashMap<String,String> mimeMap;
  FileFilter fileFilter;
  // Backlog for crawling
  List<LinkedHashSet<URL>> backlog = new ArrayList<>();
  Set<URL> visited = new HashSet<>();
  
  static final Set<String> DATA_MODES = new HashSet<>();
  static final String USAGE_STRING_SHORT =
      "Usage: java [SystemProperties] -jar post.jar [-h|-] [<file|folder|url|arg> [<file|folder|url|arg>...]]";

  // Used in tests to avoid doing actual network traffic
  static boolean mockMode = false;
  static PageFetcher pageFetcher;

  static {
    DATA_MODES.add(DATA_MODE_FILES);
    DATA_MODES.add(DATA_MODE_ARGS);
    DATA_MODES.add(DATA_MODE_STDIN);
    DATA_MODES.add(DATA_MODE_WEB);
    
    mimeMap = new HashMap<>();
    mimeMap.put("xml", "application/xml");
    mimeMap.put("csv", "text/csv");
    mimeMap.put("json", "application/json");
    mimeMap.put("jsonl", "application/json");
    mimeMap.put("pdf", "application/pdf");
    mimeMap.put("rtf", "text/rtf");
    mimeMap.put("html", "text/html");
    mimeMap.put("htm", "text/html");
    mimeMap.put("doc", "application/msword");
    mimeMap.put("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
    mimeMap.put("ppt", "application/vnd.ms-powerpoint");
    mimeMap.put("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation");
    mimeMap.put("xls", "application/vnd.ms-excel");
    mimeMap.put("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    mimeMap.put("odt", "application/vnd.oasis.opendocument.text");
    mimeMap.put("ott", "application/vnd.oasis.opendocument.text");
    mimeMap.put("odp", "application/vnd.oasis.opendocument.presentation");
    mimeMap.put("otp", "application/vnd.oasis.opendocument.presentation");
    mimeMap.put("ods", "application/vnd.oasis.opendocument.spreadsheet");
    mimeMap.put("ots", "application/vnd.oasis.opendocument.spreadsheet");
    mimeMap.put("txt", "text/plain");
    mimeMap.put("log", "text/plain");
  }
  
  /**
   * See usage() for valid command line usage
   * @param args the params on the command line
   */
  public static void main(String[] args) {
    info("SimplePostTool version " + VERSION_OF_THIS_TOOL);
    if (0 < args.length && ("-help".equals(args[0]) || "--help".equals(args[0]) || "-h".equals(args[0]))) {
      usage();
    } else {
      final SimplePostTool t = parseArgsAndInit(args);
      t.execute();
    }
  }

  /**
   * After initialization, call execute to start the post job.
   * This method delegates to the correct mode method.
   */
  public void execute() {
    final RTimer timer = new RTimer();
    if (DATA_MODE_FILES.equals(mode) && args.length > 0) {
      doFilesMode();
    } else if(DATA_MODE_ARGS.equals(mode) && args.length > 0) {
      doArgsMode();
    } else if(DATA_MODE_WEB.equals(mode) && args.length > 0) {
      doWebMode();
    } else if(DATA_MODE_STDIN.equals(mode)) {
      doStdinMode();
    } else {
      usageShort();
      return;
    }
    
    if (commit)   commit();
    if (optimize) optimize();
    displayTiming((long) timer.getTime());
  }
  
  /**
   * Pretty prints the number of milliseconds taken to post the content to Solr
   * @param millis the time in milliseconds
   */
  private void displayTiming(long millis) {
    SimpleDateFormat df = new SimpleDateFormat("H:mm:ss.SSS", Locale.getDefault());
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    System.out.println("Time spent: "+df.format(new Date(millis)));
  }

  /**
   * Parses incoming arguments and system params and initializes the tool
   * @param args the incoming cmd line args
   * @return an instance of SimplePostTool
   */
  protected static SimplePostTool parseArgsAndInit(String[] args) {
    String urlStr = null;
    try {
      // Parse args
      final String mode = System.getProperty("data", DEFAULT_DATA_MODE);
      if (! DATA_MODES.contains(mode)) {
        fatal("System Property 'data' is not valid for this tool: " + mode);
      }
      
      String params = System.getProperty("params", "");

      String host = System.getProperty("host", DEFAULT_POST_HOST);
      String port = System.getProperty("port", DEFAULT_POST_PORT);
      String core = System.getProperty("c");
      
      urlStr = System.getProperty("url");
      
      if (urlStr == null && core == null) {
        fatal("Specifying either url or core/collection is mandatory.\n" + USAGE_STRING_SHORT);
      }
      
      if(urlStr == null) {
        urlStr = String.format(Locale.ROOT, "http://%s:%s/solr/%s/update", host, port, core);
      }
      urlStr = SimplePostTool.appendParam(urlStr, params);
      URL url = new URL(urlStr);
      String user = null;
      if (url.getUserInfo() != null && url.getUserInfo().trim().length() > 0) {
        user = url.getUserInfo().split(":")[0];
      } else if (System.getProperty(BASIC_AUTH) != null) {
        user = System.getProperty(BASIC_AUTH).trim().split(":")[0];
      }
      if (user != null)
        info("Basic Authentication enabled, user=" + user);
      
      boolean auto = isOn(System.getProperty("auto", DEFAULT_AUTO));
      String type = System.getProperty("type");
      String format = System.getProperty("format");
      // Recursive
      int recursive = 0;
      String r = System.getProperty("recursive", DEFAULT_RECURSIVE);
      try {
        recursive = Integer.parseInt(r);
      } catch(Exception e) {
        if (isOn(r))
          recursive = DATA_MODE_WEB.equals(mode)?1:999;
      }
      // Delay
      int delay = DATA_MODE_WEB.equals(mode) ? DEFAULT_WEB_DELAY : 0;
      try {
        delay = Integer.parseInt(System.getProperty("delay", ""+delay));
      } catch(Exception e) { }
      OutputStream out = isOn(System.getProperty("out", DEFAULT_OUT)) ? System.out : null;
      String fileTypes = System.getProperty("filetypes", DEFAULT_FILE_TYPES);
      boolean commit = isOn(System.getProperty("commit",DEFAULT_COMMIT));
      boolean optimize = isOn(System.getProperty("optimize",DEFAULT_OPTIMIZE));
      
      return new SimplePostTool(mode, url, auto, type, format, recursive, delay, fileTypes, out, commit, optimize, args);
    } catch (MalformedURLException e) {
      fatal("System Property 'url' is not a valid URL: " + urlStr);
      return null;
    }
  }

  /**
   * Constructor which takes in all mandatory input for the tool to work.
   * Also see usage() for further explanation of the params.
   * @param mode whether to post files, web pages, params or stdin
   * @param url the Solr base Url to post to, should end with /update
   * @param auto if true, we'll guess type and add resourcename/url
   * @param type content-type of the data you are posting
   * @param recursive number of levels for file/web mode, or 0 if one file only
   * @param delay if recursive then delay will be the wait time between posts
   * @param fileTypes a comma separated list of file-name endings to accept for file/web
   * @param out an OutputStream to write output to, e.g. stdout to print to console
   * @param commit if true, will commit at end of posting
   * @param optimize if true, will optimize at end of posting
   * @param args a String[] of arguments, varies between modes
   */
  public SimplePostTool(String mode, URL url, boolean auto, String type, String format,
      int recursive, int delay, String fileTypes, OutputStream out, 
      boolean commit, boolean optimize, String[] args) {
    this.mode = mode;
    this.solrUrl = url;
    this.auto = auto;
    this.type = type;
    this.format = format;
    this.recursive = recursive;
    this.delay = delay;
    this.fileTypes = fileTypes;
    this.fileFilter = getFileFilterFromFileTypes(fileTypes);
    this.out = out;
    this.commit = commit;
    this.optimize = optimize;
    this.args = args;
    pageFetcher = new PageFetcher();
  }

  public SimplePostTool() {}
  
  //
  // Do some action depending on which mode we have
  //
  private void doFilesMode() {
    currentDepth = 0;
    // Skip posting files if special param "-" given  
    if (!args[0].equals("-")) {
      info("Posting files to [base] url " + solrUrl + (!auto?" using content-type "+(type==null?DEFAULT_CONTENT_TYPE:type):"")+"...");
      if(auto)
        info("Entering auto mode. File endings considered are "+fileTypes);
      if(recursive > 0)
        info("Entering recursive mode, max depth="+recursive+", delay="+delay+"s"); 
      int numFilesPosted = postFiles(args, 0, out, type);
      info(numFilesPosted + " files indexed.");
    }
  }

  private void doArgsMode() {
    info("POSTing args to " + solrUrl + "...");
    for (String a : args) {
      postData(stringToStream(a), null, out, type, solrUrl);
    }
  }

  private int doWebMode() {
    reset();
    int numPagesPosted = 0;
    try {
      if(type != null) {
        fatal("Specifying content-type with \"-Ddata=web\" is not supported");
      }
      if (args[0].equals("-")) {
        // Skip posting url if special param "-" given  
        return 0;
      }
      // Set Extracting handler as default
      solrUrl = appendUrlPath(solrUrl, "/extract");
      
      info("Posting web pages to Solr url "+solrUrl);
      auto=true;
      info("Entering auto mode. Indexing pages with content-types corresponding to file endings "+fileTypes);
      if(recursive > 0) {
        if(recursive > MAX_WEB_DEPTH) {
          recursive = MAX_WEB_DEPTH;
          warn("Too large recursion depth for web mode, limiting to "+MAX_WEB_DEPTH+"...");
        }
        if(delay < DEFAULT_WEB_DELAY)
          warn("Never crawl an external web site faster than every 10 seconds, your IP will probably be blocked");
        info("Entering recursive mode, depth="+recursive+", delay="+delay+"s");
      }
      numPagesPosted = postWebPages(args, 0, out);
      info(numPagesPosted + " web pages indexed.");
    } catch(MalformedURLException e) {
      fatal("Wrong URL trying to append /extract to "+solrUrl);
    }
    return numPagesPosted;
  }

  private void doStdinMode() {
    info("POSTing stdin to " + solrUrl + "...");
    postData(System.in, null, out, type, solrUrl);    
  }

  private void reset() {
    backlog = new ArrayList<>();
    visited = new HashSet<>();
  }


  //
  // USAGE
  //
  private static void usageShort() {
    System.out.println(USAGE_STRING_SHORT+"\n"+
        "       Please invoke with -h option for extended usage help.");
  }

  private static void usage() {
    System.out.println
    (USAGE_STRING_SHORT+"\n\n" +
     "Supported System Properties and their defaults:\n"+
     "  -Dc=<core/collection>\n"+
     "  -Durl=<base Solr update URL> (overrides -Dc option if specified)\n"+
     "  -Ddata=files|web|args|stdin (default=" + DEFAULT_DATA_MODE + ")\n"+
     "  -Dtype=<content-type> (default=" + DEFAULT_CONTENT_TYPE + ")\n"+
     "  -Dhost=<host> (default: " + DEFAULT_POST_HOST+ ")\n"+
     "  -Dport=<port> (default: " + DEFAULT_POST_PORT+ ")\n"+
     "  -Dbasicauth=<user:pass> (sets Basic Authentication credentials)\n"+
     "  -Dauto=yes|no (default=" + DEFAULT_AUTO + ")\n"+
     "  -Drecursive=yes|no|<depth> (default=" + DEFAULT_RECURSIVE + ")\n"+
     "  -Ddelay=<seconds> (default=0 for files, 10 for web)\n"+
     "  -Dfiletypes=<type>[,<type>,...] (default=" + DEFAULT_FILE_TYPES + ")\n"+
     "  -Dparams=\"<key>=<value>[&<key>=<value>...]\" (values must be URL-encoded)\n"+
     "  -Dcommit=yes|no (default=" + DEFAULT_COMMIT + ")\n"+
     "  -Doptimize=yes|no (default=" + DEFAULT_OPTIMIZE + ")\n"+
     "  -Dout=yes|no (default=" + DEFAULT_OUT + ")\n\n"+
     "This is a simple command line tool for POSTing raw data to a Solr port.\n"+
     "NOTE: Specifying the url/core/collection name is mandatory.\n" +
     "Data can be read from files specified as commandline args,\n"+
     "URLs specified as args, as raw commandline arg strings or via STDIN.\n"+
     "Examples:\n"+
     "  java -Dc=gettingstarted -jar post.jar *.xml\n"+
     "  java -Ddata=args -Dc=gettingstarted -jar post.jar '<delete><id>42</id></delete>'\n"+
     "  java -Ddata=stdin -Dc=gettingstarted -jar post.jar < hd.xml\n"+
     "  java -Ddata=web -Dc=gettingstarted -jar post.jar http://example.com/\n"+
     "  java -Dtype=text/csv -Dc=gettingstarted -jar post.jar *.csv\n"+
     "  java -Dtype=application/json -Dc=gettingstarted -jar post.jar *.json\n"+
     "  java -Durl=http://localhost:8983/solr/techproducts/update/extract -Dparams=literal.id=pdf1 -jar post.jar solr-word.pdf\n"+
     "  java -Dauto -Dc=gettingstarted -jar post.jar *\n"+
     "  java -Dauto -Dc=gettingstarted -Drecursive -jar post.jar afolder\n"+
     "  java -Dauto -Dc=gettingstarted -Dfiletypes=ppt,html -jar post.jar afolder\n"+
     "The options controlled by System Properties include the Solr\n"+
     "URL to POST to, the Content-Type of the data, whether a commit\n"+
     "or optimize should be executed, and whether the response should\n"+
     "be written to STDOUT. If auto=yes the tool will try to set type\n"+
     "automatically from file name. When posting rich documents the\n"+
     "file name will be propagated as \"resource.name\" and also used\n"+
     "as \"literal.id\". You may override these or any other request parameter\n"+
     "through the -Dparams property. To do a commit only, use \"-\" as argument.\n"+
     "The web mode is a simple crawler following links within domain, default delay=10s.");
  }

  /** Post all filenames provided in args
   * @param args array of file names
   * @param startIndexInArgs offset to start
   * @param out output stream to post data to
   * @param type default content-type to use when posting (may be overridden in auto mode)
   * @return number of files posted
   * */
  public int postFiles(String [] args,int startIndexInArgs, OutputStream out, String type) {
    reset();
    int filesPosted = 0;
    for (int j = startIndexInArgs; j < args.length; j++) {
      File srcFile = new File(args[j]);
      if(srcFile.isDirectory() && srcFile.canRead()) {
        filesPosted += postDirectory(srcFile, out, type);
      } else if (srcFile.isFile() && srcFile.canRead()) {
        filesPosted += postFiles(new File[] {srcFile}, out, type);
      } else {
        File parent = srcFile.getParentFile();
        if(parent == null) parent = new File(".");
        String fileGlob = srcFile.getName();
        GlobFileFilter ff = new GlobFileFilter(fileGlob, false);
        File[] files = parent.listFiles(ff);
        if(files == null || files.length == 0) {
          warn("No files or directories matching "+srcFile);
          continue;          
        }
        filesPosted += postFiles(parent.listFiles(ff), out, type);
      }
    }
    return filesPosted;
  }
  
  /** Post all filenames provided in args
   * @param files array of Files
   * @param startIndexInArgs offset to start
   * @param out output stream to post data to
   * @param type default content-type to use when posting (may be overridden in auto mode)
   * @return number of files posted
   * */
  public int postFiles(File[] files, int startIndexInArgs, OutputStream out, String type) {
    reset();
    int filesPosted = 0;
    for (File srcFile : files) {
      if(srcFile.isDirectory() && srcFile.canRead()) {
        filesPosted += postDirectory(srcFile, out, type);
      } else if (srcFile.isFile() && srcFile.canRead()) {
        filesPosted += postFiles(new File[] {srcFile}, out, type);
      } else {
        File parent = srcFile.getParentFile();
        if(parent == null) parent = new File(".");
        String fileGlob = srcFile.getName();
        GlobFileFilter ff = new GlobFileFilter(fileGlob, false);
        File[] fileList = parent.listFiles(ff);
        if(fileList == null || fileList.length == 0) {
          warn("No files or directories matching "+srcFile);
          continue;          
        }
        filesPosted += postFiles(fileList, out, type);
      }
    }
    return filesPosted;
  }
  
  /**
   * Posts a whole directory
   * @return number of files posted total
   */
  private int postDirectory(File dir, OutputStream out, String type) {
    if(dir.isHidden() && !dir.getName().equals("."))
      return(0);
    info("Indexing directory "+dir.getPath()+" ("+dir.listFiles(fileFilter).length+" files, depth="+currentDepth+")");
    int posted = 0;
    posted += postFiles(dir.listFiles(fileFilter), out, type);
    if(recursive > currentDepth) {
      for(File d : dir.listFiles()) {
        if(d.isDirectory()) {
          currentDepth++;
          posted += postDirectory(d, out, type);
          currentDepth--;
        }
      }
    }
    return posted;
  }

  /**
   * Posts a list of file names
   * @return number of files posted
   */
  int postFiles(File[] files, OutputStream out, String type) {
    int filesPosted = 0;
    for(File srcFile : files) {
      try {
        if(!srcFile.isFile() || srcFile.isHidden())
          continue;
        postFile(srcFile, out, type);
        Thread.sleep(delay * 1000);
        filesPosted++;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return filesPosted;
  }

  /**
   * This method takes as input a list of start URL strings for crawling,
   * adds each one to a backlog and then starts crawling
   * @param args the raw input args from main()
   * @param startIndexInArgs offset for where to start
   * @param out outputStream to write results to
   * @return the number of web pages posted
   */
  public int postWebPages(String[] args, int startIndexInArgs, OutputStream out) {
    reset();
    LinkedHashSet<URL> s = new LinkedHashSet<>();
    for (int j = startIndexInArgs; j < args.length; j++) {
      try {
        URL u = new URL(normalizeUrlEnding(args[j]));
        s.add(u);
      } catch(MalformedURLException e) {
        warn("Skipping malformed input URL: "+args[j]);
      }
    }
    // Add URLs to level 0 of the backlog and start recursive crawling
    backlog.add(s);
    return webCrawl(0, out);
  }

  /**
   * Normalizes a URL string by removing anchor part and trailing slash
   * @return the normalized URL string
   */
  protected static String normalizeUrlEnding(String link) {
    if(link.indexOf("#") > -1)
      link = link.substring(0,link.indexOf("#"));
    if(link.endsWith("?"))
      link = link.substring(0,link.length()-1);
    if(link.endsWith("/"))
      link = link.substring(0,link.length()-1);
    return link;
  }

  /**
   * A very simple crawler, pulling URLs to fetch from a backlog and then
   * recurses N levels deep if recursive&gt;0. Links are parsed from HTML
   * through first getting an XHTML version using SolrCell with extractOnly,
   * and followed if they are local. The crawler pauses for a default delay
   * of 10 seconds between each fetch, this can be configured in the delay
   * variable. This is only meant for test purposes, as it does not respect
   * robots or anything else fancy :)
   * @param level which level to crawl
   * @param out output stream to write to
   * @return number of pages crawled on this level and below
   */
  protected int webCrawl(int level, OutputStream out) {
    int numPages = 0;
    LinkedHashSet<URL> stack = backlog.get(level);
    int rawStackSize = stack.size();
    stack.removeAll(visited);
    int stackSize = stack.size();
    LinkedHashSet<URL> subStack = new LinkedHashSet<>();
    info("Entering crawl at level "+level+" ("+rawStackSize+" links total, "+stackSize+" new)");
    for(URL u : stack) {
      try {
        visited.add(u);
        PageFetcherResult result = pageFetcher.readPageFromUrl(u);
        if(result.httpStatus == 200) {
          u = (result.redirectUrl != null) ? result.redirectUrl : u;
          URL postUrl = new URL(appendParam(solrUrl.toString(), 
              "literal.id="+URLEncoder.encode(u.toString(),"UTF-8") +
              "&literal.url="+URLEncoder.encode(u.toString(),"UTF-8")));
          boolean success = postData(new ByteArrayInputStream(result.content.array(), result.content.arrayOffset(),result.content.limit() ), null, out, result.contentType, postUrl);
          if (success) {
            info("POSTed web resource "+u+" (depth: "+level+")");
            Thread.sleep(delay * 1000);
            numPages++;
            // Pull links from HTML pages only
            if(recursive > level && result.contentType.equals("text/html")) {
              Set<URL> children = pageFetcher.getLinksFromWebPage(u, new ByteArrayInputStream(result.content.array(), result.content.arrayOffset(), result.content.limit()), result.contentType, postUrl);
              subStack.addAll(children);
            }
          } else {
            warn("An error occurred while posting "+u);
          }
        } else {
          warn("The URL "+u+" returned a HTTP result status of "+result.httpStatus);
        }
      } catch (IOException e) {
        warn("Caught exception when trying to open connection to "+u+": "+e.getMessage());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    if(!subStack.isEmpty()) {
      backlog.add(subStack);
      numPages += webCrawl(level+1, out);
    }
    return numPages;    
  }
  public static class BAOS extends ByteArrayOutputStream {
    public ByteBuffer getByteBuffer() {
      return ByteBuffer.wrap(super.buf,0,super.count);
    }
  }
  public static ByteBuffer inputStreamToByteArray(InputStream is) throws IOException {
    return inputStreamToByteArray(is,Integer.MAX_VALUE);

  }

  /**
   * Reads an input stream into a byte array
   *
   * @param is the input stream
   * @return the byte array
   * @throws IOException If there is a low-level I/O error.
   */
  public static ByteBuffer inputStreamToByteArray(InputStream is, long maxSize) throws IOException {
    BAOS bos =  new BAOS();
    long sz = 0;
    int next = is.read();
    while (next > -1) {
      if(++sz > maxSize) throw new BufferOverflowException();
      bos.write(next);
      next = is.read();
    }
    bos.flush();
    is.close();
    return bos.getByteBuffer();
  }

  /**
   * Computes the full URL based on a base url and a possibly relative link found
   * in the href param of an HTML anchor.
   * @param baseUrl the base url from where the link was found
   * @param link the absolute or relative link
   * @return the string version of the full URL
   */
  protected String computeFullUrl(URL baseUrl, String link) {
    if(link == null || link.length() == 0) {
      return null;
    }
    if(!link.startsWith("http")) {
      if(link.startsWith("/")) {
        link = baseUrl.getProtocol() + "://" + baseUrl.getAuthority() + link;
      } else {
        if(link.contains(":")) {
          return null; // Skip non-relative URLs
        }
        String path = baseUrl.getPath();
        if(!path.endsWith("/")) {
          int sep = path.lastIndexOf("/");
          String file = path.substring(sep+1);
          if(file.contains(".") || file.contains("?"))
            path = path.substring(0,sep);
        }
        link = baseUrl.getProtocol() + "://" + baseUrl.getAuthority() + path + "/" + link;
      }
    }
    link = normalizeUrlEnding(link);
    String l = link.toLowerCase(Locale.ROOT);
    // Simple brute force skip images
    if(l.endsWith(".jpg") || l.endsWith(".jpeg") || l.endsWith(".png") || l.endsWith(".gif")) {
      return null; // Skip images
    }
    return link;
  }

  /**
   * Uses the mime-type map to reverse lookup whether the file ending for our type
   * is supported by the fileTypes option
   * @param type what content-type to lookup
   * @return true if this is a supported content type
   */
  protected boolean typeSupported(String type) {
    for(String key : mimeMap.keySet()) {
      if(mimeMap.get(key).equals(type)) {
        if(fileTypes.contains(key))
          return true;
      }
    }
    return false;
  }

  /**
   * Tests if a string is either "true", "on", "yes" or "1"
   * @param property the string to test
   * @return true if "on"
   */
  protected static boolean isOn(String property) {
    return("true,on,yes,1".indexOf(property) > -1);
  }
  
  static void warn(String msg) {
    System.err.println("SimplePostTool: WARNING: " + msg);
  }

  static void info(String msg) {
    System.out.println(msg);
  }

  static void fatal(String msg) {
    System.err.println("SimplePostTool: FATAL: " + msg);
    System.exit(2);
  }

  /**
   * Does a simple commit operation 
   */
  public void commit() {
    info("COMMITting Solr index changes to " + solrUrl + "...");
    doGet(appendParam(solrUrl.toString(), "commit=true"));
  }

  /**
   * Does a simple optimize operation 
   */
  public void optimize() {
    info("Performing an OPTIMIZE to " + solrUrl + "...");
    doGet(appendParam(solrUrl.toString(), "optimize=true"));
  }

  /**
   * Appends a URL query parameter to a URL 
   * @param url the original URL
   * @param param the parameter(s) to append, separated by "&amp;"
   * @return the string version of the resulting URL
   */
  public static String appendParam(String url, String param) {
    String[] pa = param.split("&");
    for(String p : pa) {
      if(p.trim().length() == 0) continue;
      String[] kv = p.split("=");
      if(kv.length == 2) {
        url = url + (url.indexOf('?')>0 ? "&" : "?") + kv[0] +"="+ kv[1];
      } else {
        warn("Skipping param "+p+" which is not on form key=value");
      }
    }
    return url;
  }

  /**
   * Opens the file and posts its contents to the solrUrl,
   * writes to response to output. 
   */
  public void postFile(File file, OutputStream output, String type) {
    InputStream is = null;
    try {
      URL url = solrUrl;
      String suffix = "";
      if(auto) {
        if(type == null) {
          type = guessType(file);
        }
        // TODO: Add a flag that disables /update and sends all to /update/extract, to avoid CSV, JSON, and XML files
        // TODO: from being interpreted as Solr documents internally
        if (type.equals("application/json") && !"solr".equals(format))  {
          suffix = "/json/docs";
          String urlStr = appendUrlPath(solrUrl, suffix).toString();
          url = new URL(urlStr);
        } else if (type.equals("application/xml") || type.equals("text/csv") || type.equals("application/json")) {
          // Default handler
        } else {
          // SolrCell
          suffix = "/extract";
          String urlStr = appendUrlPath(solrUrl, suffix).toString();
          if(urlStr.indexOf("resource.name")==-1)
            urlStr = appendParam(urlStr, "resource.name=" + URLEncoder.encode(file.getAbsolutePath(), "UTF-8"));
          if(urlStr.indexOf("literal.id")==-1)
            urlStr = appendParam(urlStr, "literal.id=" + URLEncoder.encode(file.getAbsolutePath(), "UTF-8"));
          url = new URL(urlStr);
        }
      } else {
        if(type == null) type = DEFAULT_CONTENT_TYPE;
      }
      info("POSTing file " + file.getName() + (auto?" ("+type+")":"") + " to [base]" + suffix);
      is = new FileInputStream(file);
      postData(is, file.length(), output, type, url);
    } catch (IOException e) {
      e.printStackTrace();
      warn("Can't open/read file: " + file);
    } finally {
      try {
        if(is!=null) is.close();
      } catch (IOException e) {
        fatal("IOException while closing file: "+ e);
      }
    }
  }

  /**
   * Appends to the path of the URL
   * @param url the URL
   * @param append the path to append
   * @return the final URL version 
   */
  protected static URL appendUrlPath(URL url, String append) throws MalformedURLException {
    return new URL(url.getProtocol() + "://" + url.getAuthority() + url.getPath() + append + (url.getQuery() != null ? "?"+url.getQuery() : ""));
  }

  /**
   * Guesses the type of a file, based on file name suffix
   * Returns "application/octet-stream" if no corresponding mimeMap type.
   * @param file the file
   * @return the content-type guessed
   */
  protected static String guessType(File file) {
    String name = file.getName();
    String suffix = name.substring(name.lastIndexOf(".")+1);
    String type = mimeMap.get(suffix.toLowerCase(Locale.ROOT));
    return (type != null) ? type : "application/octet-stream";
  }

  /**
   * Performs a simple get on the given URL
   */
  public static void doGet(String url) {
    try {
      doGet(new URL(url));
    } catch (MalformedURLException e) {
      warn("The specified URL "+url+" is not a valid URL. Please check");
    }
  }
  
  /**
   * Performs a simple get on the given URL
   */
  public static void doGet(URL url) {
    try {
      if(mockMode) return;
      HttpURLConnection urlc = (HttpURLConnection) url.openConnection();
      basicAuth(urlc);
      urlc.connect();
      checkResponseCode(urlc);
    } catch (IOException e) {
      warn("An error occurred getting data from "+url+". Please check that Solr is running.");
    } catch (Exception e) {
      warn("An error occurred getting data from "+url+". Message: " + e.getMessage());
    }
  }

  /**
   * Reads data from the data stream and posts it to solr,
   * writes to the response to output
   * @return true if success
   */
  public boolean postData(InputStream data, Long length, OutputStream output, String type, URL url) {
    if(mockMode) return true;
    boolean success = true;
    if(type == null)
      type = DEFAULT_CONTENT_TYPE;
    HttpURLConnection urlc = null;
    try {
      try {
        urlc = (HttpURLConnection) url.openConnection();
        try {
          urlc.setRequestMethod("POST");
        } catch (ProtocolException e) {
          fatal("Shouldn't happen: HttpURLConnection doesn't support POST??"+e);
        }
        urlc.setDoOutput(true);
        urlc.setDoInput(true);
        urlc.setUseCaches(false);
        urlc.setAllowUserInteraction(false);
        urlc.setRequestProperty("Content-type", type);
        basicAuth(urlc);
        if (null != length) {
          urlc.setFixedLengthStreamingMode(length);
        } else {
          urlc.setChunkedStreamingMode(-1);//use JDK default chunkLen, 4k in Java 8.
        }
        urlc.connect();
      } catch (IOException e) {
        fatal("Connection error (is Solr running at " + solrUrl + " ?): " + e);
        success = false;
      } catch (Exception e) {
        fatal("POST failed with error " + e.getMessage());
      }

      try (final OutputStream out = urlc.getOutputStream()) {
        pipe(data, out);
      } catch (IOException e) {
        fatal("IOException while posting data: " + e);
      }
      
      try {
        success &= checkResponseCode(urlc);
        try (final InputStream in = urlc.getInputStream()) {
          pipe(in, output);
        }
      } catch (IOException e) {
        warn("IOException while reading response: " + e);
        success = false;
      } catch (GeneralSecurityException e) {
        fatal("Looks like Solr is secured and would not let us in. Try with another user in '-u' parameter");
      }
    } finally {
      if (urlc!=null) urlc.disconnect();
    }
    return success;
  }

  private static void basicAuth(HttpURLConnection urlc) throws Exception {
    if (urlc.getURL().getUserInfo() != null) {
      String encoding = Base64.getEncoder().encodeToString(urlc.getURL().getUserInfo().getBytes(US_ASCII));
      urlc.setRequestProperty("Authorization", "Basic " + encoding);
    } else if (System.getProperty(BASIC_AUTH) != null) {
      String basicauth = System.getProperty(BASIC_AUTH).trim();
      if (!basicauth.contains(":")) {
        throw new Exception("System property '"+BASIC_AUTH+"' must be of format user:pass");
      }
      urlc.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(basicauth.getBytes(UTF_8)));
    }
  }

  private static boolean checkResponseCode(HttpURLConnection urlc) throws IOException, GeneralSecurityException {
    if (urlc.getResponseCode() >= 400) {
      warn("Solr returned an error #" + urlc.getResponseCode() + 
            " (" + urlc.getResponseMessage() + ") for url: " + urlc.getURL());
      Charset charset = StandardCharsets.ISO_8859_1;
      final String contentType = urlc.getContentType();
      // code cloned from ContentStreamBase, but post.jar should be standalone!
      if (contentType != null) {
        int idx = contentType.toLowerCase(Locale.ROOT).indexOf("charset=");
        if (idx > 0) {
          charset = Charset.forName(contentType.substring(idx + "charset=".length()).trim());
        }
      }
      // Print the response returned by Solr
      try (InputStream errStream = urlc.getErrorStream()) {
        if (errStream != null) {
          BufferedReader br = new BufferedReader(new InputStreamReader(errStream, charset));
          final StringBuilder response = new StringBuilder("Response: ");
          int ch;
          while ((ch = br.read()) != -1) {
            response.append((char) ch);
          }
          warn(response.toString().trim());
        }
      }
      if (urlc.getResponseCode() == 401) {
        throw new GeneralSecurityException("Solr requires authentication (response 401). Please try again with '-u' option");
      }
      if (urlc.getResponseCode() == 403) {
        throw new GeneralSecurityException("You are not authorized to perform this action against Solr. (response 403)");
      }
      return false;
    }
    return true;
  }

  /**
   * Converts a string to an input stream 
   * @param s the string
   * @return the input stream
   */
  public static InputStream stringToStream(String s) {
    return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Pipes everything from the source to the dest.  If dest is null, 
   * then everything is read from source and thrown away.
   */
  private static void pipe(InputStream source, OutputStream dest) throws IOException {
    byte[] buf = new byte[1024];
    int read = 0;
    while ( (read = source.read(buf) ) >= 0) {
      if (null != dest) dest.write(buf, 0, read);
    }
    if (null != dest) dest.flush();
  }

  public FileFilter getFileFilterFromFileTypes(String fileTypes) {
    String glob;
    if(fileTypes.equals("*"))
      glob = ".*";
    else
      glob = "^.*\\.(" + fileTypes.replace(",", "|") + ")$";
    return new GlobFileFilter(glob, true);
  }

  //
  // Utility methods for XPath handing
  //
  
  /**
   * Gets all nodes matching an XPath
   */
  public static NodeList getNodesFromXP(Node n, String xpath) throws XPathExpressionException {
    XPathFactory factory = XPathFactory.newInstance();
    XPath xp = factory.newXPath();
    XPathExpression expr = xp.compile(xpath);
    return (NodeList) expr.evaluate(n, XPathConstants.NODESET);
  }
  
  /**
   * Gets the string content of the matching an XPath
   * @param n the node (or doc)
   * @param xpath the xpath string
   * @param concatAll if true, text from all matching nodes will be concatenated, else only the first returned
   */
  public static String getXP(Node n, String xpath, boolean concatAll)
      throws XPathExpressionException {
    NodeList nodes = getNodesFromXP(n, xpath);
    StringBuilder sb = new StringBuilder();
    if (nodes.getLength() > 0) {
      for(int i = 0; i < nodes.getLength() ; i++) {
        sb.append(nodes.item(i).getNodeValue() + " ");
        if(!concatAll) break;
      }
      return sb.toString().trim();
    } else
      return "";
  }
  
  /**
   * Takes a string as input and returns a DOM 
   */
  public static Document makeDom(byte[] in) throws SAXException, IOException,
  ParserConfigurationException {
    InputStream is = new ByteArrayInputStream(in);
    Document dom = DocumentBuilderFactory.newInstance()
        .newDocumentBuilder().parse(is);
    return dom;
  }

  /**
   * Inner class to filter files based on glob wildcards
   */
  class GlobFileFilter implements FileFilter
  {
    private String _pattern;
    private Pattern p;
    
    public GlobFileFilter(String pattern, boolean isRegex)
    {
      _pattern = pattern;
      if(!isRegex) {
        _pattern = _pattern
            .replace("^", "\\^")
            .replace("$", "\\$")
            .replace(".", "\\.")
            .replace("(", "\\(")
            .replace(")", "\\)")
            .replace("+", "\\+")
            .replace("*", ".*")
            .replace("?", ".");
        _pattern = "^" + _pattern + "$";
      }
      
      try {
        p = Pattern.compile(_pattern,Pattern.CASE_INSENSITIVE);
      } catch(PatternSyntaxException e) {
        fatal("Invalid type list "+pattern+". "+e.getDescription());
      }
    }
    
    @Override
    public boolean accept(File file)
    {
      return p.matcher(file.getName()).find();
    }
  }
  
  //
  // Simple crawler class which can fetch a page and check for robots.txt
  //
  class PageFetcher {
    Map<String, List<String>> robotsCache;
    final String DISALLOW = "Disallow:";
    
    public PageFetcher() {
      robotsCache = new HashMap<>();
    }
    
    public PageFetcherResult readPageFromUrl(URL u) {
      PageFetcherResult res = new PageFetcherResult();
      try {
        if (isDisallowedByRobots(u)) {
          warn("The URL "+u+" is disallowed by robots.txt and will not be crawled.");
          res.httpStatus = 403;
          visited.add(u);
          return res;
        }
        res.httpStatus = 404;
        HttpURLConnection conn = (HttpURLConnection) u.openConnection();
        conn.setRequestProperty("User-Agent", "SimplePostTool-crawler/"+VERSION_OF_THIS_TOOL+" (http://lucene.apache.org/solr/)");
        conn.setRequestProperty("Accept-Encoding", "gzip, deflate");
        conn.connect();
        res.httpStatus = conn.getResponseCode();
        if(!normalizeUrlEnding(conn.getURL().toString()).equals(normalizeUrlEnding(u.toString()))) {
          info("The URL "+u+" caused a redirect to "+conn.getURL());
          u = conn.getURL();
          res.redirectUrl = u;
          visited.add(u);
        }
        if(res.httpStatus == 200) {
          // Raw content type of form "text/html; encoding=utf-8"
          String rawContentType = conn.getContentType();
          String type = rawContentType.split(";")[0];
          if(typeSupported(type) || "*".equals(fileTypes)) {
            String encoding = conn.getContentEncoding();
            InputStream is;
            if (encoding != null && encoding.equalsIgnoreCase("gzip")) {
              is = new GZIPInputStream(conn.getInputStream());
            } else if (encoding != null && encoding.equalsIgnoreCase("deflate")) {
              is = new InflaterInputStream(conn.getInputStream(), new Inflater(true));
            } else {
              is = conn.getInputStream();
            }
            
            // Read into memory, so that we later can pull links from the page without re-fetching 
            res.content = inputStreamToByteArray(is);
            is.close();
          } else {
            warn("Skipping URL with unsupported type "+type);
            res.httpStatus = 415;
          }
        }
      } catch(IOException e) {
        warn("IOException when reading page from url "+u+": "+e.getMessage());
      }
      return res;
    }
    
    public boolean isDisallowedByRobots(URL url) {
      String host = url.getHost();
      String strRobot = url.getProtocol() + "://" + host + "/robots.txt";
      List<String> disallows = robotsCache.get(host);
      if(disallows == null) {
        disallows = new ArrayList<>();
        URL urlRobot;
        try { 
          urlRobot = new URL(strRobot);
          disallows = parseRobotsTxt(urlRobot.openStream());
        } catch (MalformedURLException e) {
          return true; // We cannot trust this robots URL, should not happen
        } catch (IOException e) {
          // There is no robots.txt, will cache an empty disallow list
        }
      }
      
      robotsCache.put(host, disallows);

      String strURL = url.getFile();
      for (String path : disallows) {
        if (path.equals("/") || strURL.indexOf(path) == 0)
          return true;
      }
      return false;
    }

    /**
     * Very simple robots.txt parser which obeys all Disallow lines regardless
     * of user agent or whether there are valid Allow: lines.
     * @param is Input stream of the robots.txt file
     * @return a list of disallow paths
     * @throws IOException if problems reading the stream
     */
    protected List<String> parseRobotsTxt(InputStream is) throws IOException {
      List<String> disallows = new ArrayList<>();
      BufferedReader r = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
      String l;
      while((l = r.readLine()) != null) {
        String[] arr = l.split("#");
        if(arr.length == 0) continue;
        l = arr[0].trim();
        if(l.startsWith(DISALLOW)) {
          l = l.substring(DISALLOW.length()).trim();
          if(l.length() == 0) continue;
          disallows.add(l);
        }
      }
      is.close();
      return disallows;
    }

    /**
     * Finds links on a web page, using /extract?extractOnly=true
     * @param u the URL of the web page
     * @param is the input stream of the page
     * @param type the content-type
     * @param postUrl the URL (typically /solr/extract) in order to pull out links
     * @return a set of URLs parsed from the page
     */
    protected Set<URL> getLinksFromWebPage(URL u, InputStream is, String type, URL postUrl) {
      Set<URL> l = new HashSet<>();
      URL url = null;
      try {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        URL extractUrl = new URL(appendParam(postUrl.toString(), "extractOnly=true"));
        boolean success = postData(is, null, os, type, extractUrl);
        if(success) {
          Document d = makeDom(os.toByteArray());
          String innerXml = getXP(d, "/response/str/text()[1]", false);
          d = makeDom(innerXml.getBytes(StandardCharsets.UTF_8));
          NodeList links = getNodesFromXP(d, "/html/body//a/@href");
          for(int i = 0; i < links.getLength(); i++) {
            String link = links.item(i).getTextContent();
            link = computeFullUrl(u, link);
            if(link == null)
              continue;
            url = new URL(link);
            if(url.getAuthority() == null || !url.getAuthority().equals(u.getAuthority()))
              continue;
            l.add(url);
          }
        }
      } catch (MalformedURLException e) {
        warn("Malformed URL "+url);
      } catch (IOException e) {
        warn("IOException opening URL "+url+": "+e.getMessage());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return l;
    }
  }
    
  /**
   * Utility class to hold the result form a page fetch
   */
  public class PageFetcherResult {
    int httpStatus = 200;
    String contentType = "text/html";
    URL redirectUrl = null;
    ByteBuffer content;
  }
}

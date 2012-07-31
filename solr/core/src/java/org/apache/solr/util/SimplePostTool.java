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
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Locale;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;

/**
 * A simple utility class for posting raw updates to a Solr server, 
 * has a main method so it can be run on the command line.
 */
public class SimplePostTool {
  public static final String DEFAULT_POST_URL = "http://localhost:8983/solr/update";
  public static final String VERSION_OF_THIS_TOOL = "1.5";

  private static final String DEFAULT_COMMIT = "yes";
  private static final String DEFAULT_OPTIMIZE = "no";
  private static final String DEFAULT_OUT = "no";
  private static final String DEFAULT_AUTO = "no";
  private static final String DEFAULT_RECURSIVE = "no";

  private static final String DEFAULT_CONTENT_TYPE = "application/xml";
  private static final String DEFAULT_FILE_TYPES = "xml,json,csv,pdf,doc,docx,ppt,pptx,xls,xlsx,odt,odp,ods,ott,otp,ots,rtf,htm,html,txt,log"; 

  private static final String DATA_MODE_FILES = "files";
  private static final String DATA_MODE_ARGS = "args";
  private static final String DATA_MODE_STDIN = "stdin";
  private static final String DEFAULT_DATA_MODE = DATA_MODE_FILES;

  private static final String TRUE_STRINGS = "true,on,yes,1"; 

  private boolean auto = false;
  private boolean recursive = false;
  private String fileTypes;
  
  private static HashMap<String,String> mimeMap;
  private GlobFileFilter globFileFilter;
  
  private static final Set<String> DATA_MODES = new HashSet<String>();
  private static final String USAGE_STRING_SHORT =
      "Usage: java [SystemProperties] -jar post.jar [-h|-] [<file|folder|arg> [<file|folder|arg>...]]";

  static {
    DATA_MODES.add(DATA_MODE_FILES);
    DATA_MODES.add(DATA_MODE_ARGS);
    DATA_MODES.add(DATA_MODE_STDIN);
    
    mimeMap = new HashMap<String,String>();
    mimeMap.put("xml", "text/xml");
    mimeMap.put("csv", "text/csv");
    mimeMap.put("json", "application/json");
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

  protected URL solrUrl;
  
  public static void main(String[] args) {
    info("SimplePostTool version " + VERSION_OF_THIS_TOOL);

    if (0 < args.length && ("-help".equals(args[0]) || "--help".equals(args[0]) || "-h".equals(args[0]))) {
      usage();
      return;
    }
    
    OutputStream out = null;
    final String type = System.getProperty("type");

    final String params = System.getProperty("params", "");

    URL u = null;
    try {
      u = new URL(System.getProperty("url", SimplePostTool.appendParam(DEFAULT_POST_URL, params)));
    } catch (MalformedURLException e) {
      fatal("System Property 'url' is not a valid URL: " + u);
    }
    final SimplePostTool t = new SimplePostTool(u);

    if (isOn(System.getProperty("auto", DEFAULT_AUTO))) {
      t.setAuto(true);
    }
    
    if (isOn(System.getProperty("recursive", DEFAULT_RECURSIVE))) {
      t.setRecursive(true);
    }

    final String mode = System.getProperty("data", DEFAULT_DATA_MODE);
    if (! DATA_MODES.contains(mode)) {
      fatal("System Property 'data' is not valid for this tool: " + mode);
    }

    if (isOn(System.getProperty("out", DEFAULT_OUT))) {
      out = System.out;
    }
    
    t.setFileTypes(System.getProperty("filetypes", DEFAULT_FILE_TYPES));

    int numFilesPosted = 0;
    
    try {
      if (DATA_MODE_FILES.equals(mode)) {
        if (0 < args.length) {
          // Skip posting files if special param "-" given  
          if (!args[0].equals("-")) {
            info("Posting files to base url " + u + (!t.auto?" using content-type "+(type==null?DEFAULT_CONTENT_TYPE:type):"")+"..");
            if(t.auto)
              info("Entering auto mode. File endings considered are "+t.getFileTypes());
            if(t.recursive)
              info("Entering recursive mode"); 
            numFilesPosted = t.postFiles(args, 0, out, type);
            info(numFilesPosted + " files indexed.");
          }
        } else {
            usageShort();
            return;
        }
      } else if (DATA_MODE_ARGS.equals(mode)) {
        if (0 < args.length) {
          info("POSTing args to " + u + "..");
          for (String a : args) {
            t.postData(SimplePostTool.stringToStream(a), null, out, type);
          }
        } else {
          usageShort();
          return;
        }
      } else if (DATA_MODE_STDIN.equals(mode)) {
        info("POSTing stdin to " + u + "..");
        t.postData(System.in, null, out, type);
      }
      if (isOn(System.getProperty("commit",DEFAULT_COMMIT))) {
        info("COMMITting Solr index changes to " + u + "..");
        t.commit();
      }
      if (isOn(System.getProperty("optimize",DEFAULT_OPTIMIZE))) {
        info("Performing an OPTIMIZE to " + u + "..");
        t.optimize();
      }
    
    } catch(RuntimeException e) {
      e.printStackTrace();
      fatal("RuntimeException " + e);
    }
  }

  private static void usageShort() {
    System.out.println(USAGE_STRING_SHORT+"\n"+
        "       Please invoke with -h option for extended usage help.");
  }

  private static void usage() {
    System.out.println
    (USAGE_STRING_SHORT+"\n\n" +
     "Supported System Properties and their defaults:\n"+
     "  -Ddata=files|args|stdin (default=" + DEFAULT_DATA_MODE + ")\n"+
     "  -Dtype=<content-type> (default=" + DEFAULT_CONTENT_TYPE + ")\n"+
     "  -Durl=<solr-update-url> (default=" + DEFAULT_POST_URL + ")\n"+
     "  -Dauto=yes|no (default=" + DEFAULT_AUTO + ")\n"+
     "  -Drecursive=yes|no (default=" + DEFAULT_RECURSIVE + ")\n"+
     "  -Dfiletypes=<type>[,<type>,...] (default=" + DEFAULT_FILE_TYPES + ")\n"+
     "  -Dparams=\"<key>=<value>[&<key>=<value>...]\" (values must be URL-encoded)\n"+
     "  -Dcommit=yes|no (default=" + DEFAULT_COMMIT + ")\n"+
     "  -Doptimize=yes|no (default=" + DEFAULT_OPTIMIZE + ")\n"+
     "  -Dout=yes|no (default=" + DEFAULT_OUT + ")\n\n"+
     "This is a simple command line tool for POSTing raw data to a Solr\n"+
     "port.  Data can be read from files specified as commandline args,\n"+
     "as raw commandline arg strings, or via STDIN.\n"+
     "Examples:\n"+
     "  java -jar post.jar *.xml\n"+
     "  java -Ddata=args  -jar post.jar '<delete><id>42</id></delete>'\n"+
     "  java -Ddata=stdin -jar post.jar < hd.xml\n"+
     "  java -Dtype=text/csv -jar post.jar *.csv\n"+
     "  java -Dtype=application/json -jar post.jar *.json\n"+
     "  java -Durl=http://localhost:8983/solr/update/extract -Dparams=literal.id=a -Dtype=application/pdf -jar post.jar a.pdf\n"+
     "  java -Dauto -jar post.jar *\n"+
     "  java -Dauto -Drecursive -jar post.jar afolder\n"+
     "  java -Dauto -Dfiletypes=ppt,html -jar post.jar afolder\n"+
     "The options controlled by System Properties include the Solr\n"+
     "URL to POST to, the Content-Type of the data, whether a commit\n"+
     "or optimize should be executed, and whether the response should\n"+
     "be written to STDOUT. If auto=yes the tool will try to set type\n"+
     "and url automatically from file name. When posting rich documents\n"+
     "the file name will be propagated as \"resource.name\" and also used as \"literal.id\".\n" +
     "You may override these or any other request parameter through the -Dparams property.\n"+
     "If you want to do a commit only, use \"-\" as argument.");
  }

  private static boolean isOn(String property) {
    return(TRUE_STRINGS.indexOf(property) >= 0);
  }

  /** Post all filenames provided in args
   * @param args array of file names
   * @param startIndexInArgs offset to start
   * @param out output stream to post data to
   * @param type default content-type to use when posting (may be overridden in auto mode)
   * @return number of files posted
   * */
  int postFiles(String [] args,int startIndexInArgs, OutputStream out, String type) {
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
        if(files.length == 0) {
          warn("No files or directories matching "+srcFile);
          continue;          
        }
        filesPosted += postFiles(parent.listFiles(ff), out, type);
      }
    }
    return filesPosted;
  }
  
  private int postDirectory(File dir, OutputStream out, String type) {
    if(dir.isHidden() && !dir.getName().equals("."))
      return(0);
    info("Indexing directory "+dir.getPath());
    int posted = 0;
    posted += postFiles(dir.listFiles(globFileFilter), out, type);
    if(recursive) {
      for(File d : dir.listFiles()) {
        if(d.isDirectory())
          posted += postDirectory(d, out, type);
      }
    }
    return posted;
  }

  int postFiles(File[] files, OutputStream out, String type) {
    int filesPosted = 0;
    for(File srcFile : files) {
      if(!srcFile.isFile() || srcFile.isHidden())
        continue;
      postFile(srcFile, out, type);
      filesPosted++;
    }
    return filesPosted;
  }

  static void warn(String msg) {
    System.err.println("SimplePostTool: WARNING: " + msg);
  }

  static void info(String msg) {
    System.out.println(msg);
  }

  static void fatal(String msg) {
    System.err.println("SimplePostTool: FATAL: " + msg);
    System.exit(1);
  }

  /**
   * Constructs an instance for posting data to the specified Solr URL 
   * (ie: "http://localhost:8983/solr/update")
   */
  public SimplePostTool(URL solrUrl) {
    this.solrUrl = solrUrl;
  }

  /**
   * Does a simple commit operation 
   */
  public void commit() {
    doGet(appendParam(solrUrl.toString(), "commit=true"));
  }

  /**
   * Does a simple optimize operation 
   */
  public void optimize() {
    doGet(appendParam(solrUrl.toString(), "optimize=true"));
  }

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
   * Opens the file and posts it's contents to the solrUrl,
   * writes to response to output. 
   */
  public void postFile(File file, OutputStream output, String type) {
    InputStream is = null;
    try {
      URL url = solrUrl;
      if(auto) {
        if(type == null) {
          type = guessType(file);
        }
        if(type != null) {
          if(type.equals("text/xml") || type.equals("text/csv") || type.equals("application/json")) {
            // Default handler
          } else {
            // SolrCell
            String urlStr = url.getProtocol() + "://" + url.getAuthority() + url.getPath() + "/extract" + (url.getQuery() != null ? "?"+url.getQuery() : "");
            if(urlStr.indexOf("resource.name")==-1)
              urlStr = appendParam(urlStr, "resource.name=" + URLEncoder.encode(file.getAbsolutePath(), "UTF-8"));
            if(urlStr.indexOf("literal.id")==-1)
              urlStr = appendParam(urlStr, "literal.id=" + URLEncoder.encode(file.getAbsolutePath(), "UTF-8"));
            url = new URL(urlStr);
//            info("Indexing to ExtractingRequestHandler with URL "+url);
          }
        } else {
          warn("Skipping "+file.getName()+". Unsupported file type for auto mode.");
          return;
        }
      } else {
        if(type == null) type = DEFAULT_CONTENT_TYPE;
      }
      info("POSTing file " + file.getName() + (auto?" ("+type+")":""));
      is = new FileInputStream(file);
      postData(is, (int)file.length(), output, type, url);
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

  private String guessType(File file) {
    String name = file.getName();
    String suffix = name.substring(name.lastIndexOf(".")+1);
    return mimeMap.get(suffix.toLowerCase(Locale.ROOT));
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
      HttpURLConnection urlc = (HttpURLConnection) url.openConnection();
      if (HttpURLConnection.HTTP_OK != urlc.getResponseCode()) {
        warn("Solr returned an error #" + urlc.getResponseCode() + 
            " " + urlc.getResponseMessage() + " for url "+url);
      }
    } catch (IOException e) {
      warn("An error occured posting data to "+url+". Please check that Solr is running.");
    }
  }

  public void postData(InputStream data, Integer length, OutputStream output, String type) {
    postData(data, length, output, type, solrUrl);
  }

  /**
   * Reads data from the data stream and posts it to solr,
   * writes to the response to output
   */
  public void postData(InputStream data, Integer length, OutputStream output, String type, URL url) {
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

        if (null != length) urlc.setFixedLengthStreamingMode(length);

      } catch (IOException e) {
        fatal("Connection error (is Solr running at " + solrUrl + " ?): " + e);
      }
      
      OutputStream out = null;
      try {
        out = urlc.getOutputStream();
        pipe(data, out);
      } catch (IOException e) {
        fatal("IOException while posting data: " + e);
      } finally {
        try { if(out!=null) out.close(); } catch (IOException x) { /*NOOP*/ }
      }
      
      InputStream in = null;
      try {
        if (HttpURLConnection.HTTP_OK != urlc.getResponseCode()) {
          warn("Solr returned an error #" + urlc.getResponseCode() + 
                " " + urlc.getResponseMessage());
        }

        in = urlc.getInputStream();
        pipe(in, output);
      } catch (IOException e) {
        warn("IOException while reading response: " + e);
      } finally {
        try { if(in!=null) in.close(); } catch (IOException x) { /*NOOP*/ }
      }
      
    } finally {
      if(urlc!=null) urlc.disconnect();
    }
  }

  public static InputStream stringToStream(String s) {
    InputStream is = null;
    try {
      is = new ByteArrayInputStream(s.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      fatal("Shouldn't happen: UTF-8 not supported?!?!?!");
    }
    return is;
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

  public boolean isAuto() {
    return auto;
  }

  public void setAuto(boolean auto) {
    this.auto = auto;
  }

  public boolean isRecursive() {
    return recursive;
  }

  public void setRecursive(boolean recursive) {
    this.recursive = recursive;
  }

  public String getFileTypes() {
    return fileTypes;
  }

  public void setFileTypes(String fileTypes) {
    this.fileTypes = fileTypes;
    String glob;
    if(fileTypes.equals("*"))
      glob = ".*";
    else
      glob = "^.*\\.(" + fileTypes.replace(",", "|") + ")$";
    this.globFileFilter = new GlobFileFilter(glob, true);
  }

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
    
    public boolean accept(File file)
    {
      return p.matcher(file.getName()).find();
    }
  }
}

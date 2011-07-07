package org.apache.solr.util;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import java.util.HashSet;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

/**
 * A simple utility class for posting raw updates to a Solr server, 
 * has a main method so it can be run on the command line.
 * 
 */
public class SimplePostTool {
  public static final String DEFAULT_POST_URL = "http://localhost:8983/solr/update";
  public static final String VERSION_OF_THIS_TOOL = "1.4";

  private static final String DEFAULT_COMMIT = "yes";
  private static final String DEFAULT_OPTIMIZE = "no";
  private static final String DEFAULT_OUT = "no";

  private static final String DEFAULT_DATA_TYPE = "application/xml";

  private static final String DATA_MODE_FILES = "files";
  private static final String DATA_MODE_ARGS = "args";
  private static final String DATA_MODE_STDIN = "stdin";
  private static final String DEFAULT_DATA_MODE = DATA_MODE_FILES;

  private static final Set<String> DATA_MODES = new HashSet<String>();
  static {
    DATA_MODES.add(DATA_MODE_FILES);
    DATA_MODES.add(DATA_MODE_ARGS);
    DATA_MODES.add(DATA_MODE_STDIN);
  }

  protected URL solrUrl;

  public static void main(String[] args) {
    info("version " + VERSION_OF_THIS_TOOL);

    if (0 < args.length && ("-help".equals(args[0]) || "--help".equals(args[0]) || "-h".equals(args[0]))) {
      System.out.println
        ("This is a simple command line tool for POSTing raw data to a Solr\n"+
         "port.  Data can be read from files specified as commandline args,\n"+
         "as raw commandline arg strings, or via STDIN.\n"+
         "Examples:\n"+
         "  java -jar post.jar *.xml\n"+
         "  java -Ddata=args  -jar post.jar '<delete><id>42</id></delete>'\n"+
         "  java -Ddata=stdin -jar post.jar < hd.xml\n"+
         "  java -Durl=http://localhost:8983/solr/update/csv -Dtype=text/csv -jar post.jar *.csv\n"+
         "  java -Durl=http://localhost:8983/solr/update/json -Dtype=application/json -jar post.jar *.json\n"+
         "  java -Durl=http://localhost:8983/solr/update/extract?literal.id=a -Dtype=application/pdf -jar post.jar a.pdf\n"+
         "Other options controlled by System Properties include the Solr\n"+
         "URL to POST to, the Content-Type of the data, whether a commit\n"+
         "or optimize should be executed, and whether the response should\n"+
         "be written to STDOUT. These are the defaults for all System Properties:\n"+
         "  -Ddata=" + DEFAULT_DATA_MODE + "\n"+
         "  -Dtype=" + DEFAULT_DATA_TYPE + "\n"+
         "  -Durl=" + DEFAULT_POST_URL + "\n"+
         "  -Dcommit=" + DEFAULT_COMMIT + "\n"+
         "  -Doptimize=" + DEFAULT_OPTIMIZE + "\n"+
         "  -Dout=" + DEFAULT_OUT + "\n");
      return;
    }

    OutputStream out = null;

    URL u = null;
    try {
      u = new URL(System.getProperty("url", DEFAULT_POST_URL));
    } catch (MalformedURLException e) {
      fatal("System Property 'url' is not a valid URL: " + u);
    }
    final SimplePostTool t = new SimplePostTool(u);

    final String mode = System.getProperty("data", DEFAULT_DATA_MODE);
    if (! DATA_MODES.contains(mode)) {
      fatal("System Property 'data' is not valid for this tool: " + mode);
    }

    if ("yes".equals(System.getProperty("out", DEFAULT_OUT))) {
      out = System.out;
    }

    try {
      if (DATA_MODE_FILES.equals(mode)) {
        if (0 < args.length) {
          info("POSTing files to " + u + "..");
          t.postFiles(args, 0, out);
        } else {
          info("No files specified. (Use -h for help)");
        }
        
      } else if (DATA_MODE_ARGS.equals(mode)) {
        if (0 < args.length) {
          info("POSTing args to " + u + "..");
          for (String a : args) {
            t.postData(SimplePostTool.stringToStream(a), null, out);
          }
        }
        
      } else if (DATA_MODE_STDIN.equals(mode)) {
        info("POSTing stdin to " + u + "..");
        t.postData(System.in, null, out);
      }
      if ("yes".equals(System.getProperty("commit",DEFAULT_COMMIT))) {
        info("COMMITting Solr index changes..");
        t.commit();
      }
      if ("yes".equals(System.getProperty("optimize",DEFAULT_OPTIMIZE))) {
        info("Performing an OPTIMIZE..");
        t.optimize();
      }
    
    } catch(RuntimeException e) {
      e.printStackTrace();
      fatal("RuntimeException " + e);
    }
  }
 
  /** Post all filenames provided in args, return the number of files posted*/
  int postFiles(String [] args,int startIndexInArgs, OutputStream out) {
    int filesPosted = 0;
    for (int j = startIndexInArgs; j < args.length; j++) {
      File srcFile = new File(args[j]);
      if (srcFile.canRead()) {
        info("POSTing file " + srcFile.getName());
        postFile(srcFile, out);
        filesPosted++;
      } else {
        warn("Cannot read input file: " + srcFile);
      }
    }
    return filesPosted;
  }
  
  static void warn(String msg) {
    System.err.println("SimplePostTool: WARNING: " + msg);
  }

  static void info(String msg) {
    System.out.println("SimplePostTool: " + msg);
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

  private String appendParam(String url, String param) {
    return url + (url.indexOf('?')>0 ? "&" : "?") + param;
  }

  /**
   * Opens the file and posts it's contents to the solrUrl,
   * writes to response to output.
   * @throws UnsupportedEncodingException 
   */
  public void postFile(File file, OutputStream output) {

    InputStream is = null;
    try {
      is = new FileInputStream(file);
      postData(is, (int)file.length(), output);
    } catch (IOException e) {
      fatal("Can't open/read file: " + file);
    } finally {
      try {
        if(is!=null) is.close();
      } catch (IOException e) {
        fatal("IOException while closing file: "+ e);
      }
    }
  }

  /**
   * Performs a simple get on the given URL
   * @param url
   */
  public void doGet(String url) {
    try {
      doGet(new URL(url));
    } catch (MalformedURLException e) {
      fatal("The specified URL "+url+" is not a valid URL. Please check");
    }
  }
  
  /**
   * Performs a simple get on the given URL
   * @param url
   */
  public void doGet(URL url) {
    try {
      HttpURLConnection urlc = (HttpURLConnection) url.openConnection();
      if (HttpURLConnection.HTTP_OK != urlc.getResponseCode()) {
        fatal("Solr returned an error #" + urlc.getResponseCode() + 
            " " + urlc.getResponseMessage());
      }
    } catch (IOException e) {
      fatal("An error occured posting data to "+url+". Please check that Solr is running.");
    }
  }

  /**
   * Reads data from the data stream and posts it to solr,
   * writes to the response to output
   */
  public void postData(InputStream data, Integer length, OutputStream output) {

    final String type = System.getProperty("type", DEFAULT_DATA_TYPE);

    HttpURLConnection urlc = null;
    try {
      try {
        urlc = (HttpURLConnection) solrUrl.openConnection();
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
          fatal("Solr returned an error #" + urlc.getResponseCode() + 
                " " + urlc.getResponseMessage());
        }

        in = urlc.getInputStream();
        pipe(in, output);
      } catch (IOException e) {
        fatal("IOException while reading response: " + e);
      } finally {
        try { if(in!=null) in.close(); } catch (IOException x) { /*NOOP*/ }
      }
      
    } finally {
      if(urlc!=null) urlc.disconnect();
    }
  }

  private static InputStream stringToStream(String s) {
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
   * then everything is read fro msource and thrown away.
   */
  private static void pipe(InputStream source, OutputStream dest) throws IOException {
    byte[] buf = new byte[1024];
    int read = 0;
    while ( (read = source.read(buf) ) >= 0) {
      if (null != dest) dest.write(buf, 0, read);
    }
    if (null != dest) dest.flush();
  }
}

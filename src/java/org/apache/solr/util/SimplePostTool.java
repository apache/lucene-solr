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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
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
  public static final String POST_ENCODING = "UTF-8";
  public static final String VERSION_OF_THIS_TOOL = "1.2";
  private static final String SOLR_OK_RESPONSE_EXCERPT = "<int name=\"status\">0</int>";

  private static final String DEFAULT_COMMIT = "yes";
  
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

  private class PostException extends RuntimeException {
    PostException(String reason,Throwable cause) {
      super(reason + " (POST URL=" + solrUrl + ")",cause);
    }
  }
  
  public static void main(String[] args) {
    info("version " + VERSION_OF_THIS_TOOL);

    if (0 < args.length && "-help".equals(args[0])) {
      System.out.println
        ("This is a simple command line tool for POSTing raw XML to a Solr\n"+
         "port.  XML data can be read from files specified as commandline\n"+
         "args; as raw commandline arg strings; or via STDIN.\n"+
         "Examples:\n"+
         "  java -Ddata=files -jar post.jar *.xml\n"+
         "  java -Ddata=args  -jar post.jar '<delete><id>42</id></delete>'\n"+
         "  java -Ddata=stdin -jar post.jar < hd.xml\n"+
         "Other options controlled by System Properties include the Solr\n"+
         "URL to POST to, and whether a commit should be executed.  These\n"+
         "are the defaults for all System Properties...\n"+
         "  -Ddata=" + DEFAULT_DATA_MODE + "\n"+
         "  -Durl=" + DEFAULT_POST_URL + "\n"+
         "  -Dcommit=" + DEFAULT_COMMIT + "\n");
      return;
    }

    
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

    try {
      if (DATA_MODE_FILES.equals(mode)) {
        if (0 < args.length) {
          info("POSTing files to " + u + "..");
          final int posted = t.postFiles(args,0);
        }
        
      } else if (DATA_MODE_ARGS.equals(mode)) {
        if (0 < args.length) {
          info("POSTing args to " + u + "..");
          for (String a : args) {
            final StringWriter sw = new StringWriter();
            t.postData(new StringReader(a), sw);
            warnIfNotExpectedResponse(sw.toString(),SOLR_OK_RESPONSE_EXCERPT);
          }
        }
        
      } else if (DATA_MODE_STDIN.equals(mode)) {
        info("POSTing stdin to " + u + "..");
        final StringWriter sw = new StringWriter();
        t.postData(new InputStreamReader(System.in,POST_ENCODING), sw);
        warnIfNotExpectedResponse(sw.toString(),SOLR_OK_RESPONSE_EXCERPT);
      }
      if ("yes".equals(System.getProperty("commit",DEFAULT_COMMIT))) {
        info("COMMITting Solr index changes..");
        final StringWriter sw = new StringWriter();
        t.commit(sw);
        warnIfNotExpectedResponse(sw.toString(),SOLR_OK_RESPONSE_EXCERPT);
      }
    
    } catch(IOException ioe) {
      fatal("Unexpected IOException " + ioe);
    }
  }
 
  /** Post all filenames provided in args, return the number of files posted*/
  int postFiles(String [] args,int startIndexInArgs) throws IOException {
    int filesPosted = 0;
    for (int j = startIndexInArgs; j < args.length; j++) {
      File srcFile = new File(args[j]);
      final StringWriter sw = new StringWriter();
      
      if (srcFile.canRead()) {
        info("POSTing file " + srcFile.getName());
        postFile(srcFile, sw);
        filesPosted++;
        warnIfNotExpectedResponse(sw.toString(),SOLR_OK_RESPONSE_EXCERPT);
      } else {
        warn("Cannot read input file: " + srcFile);
      }
    }
    return filesPosted;
  }
  
  /** Check what Solr replied to a POST, and complain if it's not what we expected.
   *  TODO: parse the response and check it XMLwise, here we just check it as an unparsed String  
   */
  static void warnIfNotExpectedResponse(String actual,String expected) {
    if(actual.indexOf(expected) < 0) {
      warn("Unexpected response from Solr: '" + actual + "' does not contain '" + expected + "'");
    }
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
    warn("Make sure your XML documents are encoded in " + POST_ENCODING
        + ", other encodings are not currently supported");
  }

  /**
   * Does a simple commit operation 
   */
  public void commit(Writer output) throws IOException {
    postData(new StringReader("<commit/>"), output);
  }

  /**
   * Opens the file and posts it's contents to the solrUrl,
   * writes to response to output.
   * @throws UnsupportedEncodingException 
   */
  public void postFile(File file, Writer output) 
    throws FileNotFoundException, UnsupportedEncodingException {

    // FIXME; use a real XML parser to read files, so as to support various encodings
    // (and we can only post well-formed XML anyway)
    Reader reader = new InputStreamReader(new FileInputStream(file),POST_ENCODING);
    try {
      postData(reader, output);
    } finally {
      try {
        if(reader!=null) reader.close();
      } catch (IOException e) {
        throw new PostException("IOException while closing file", e);
      }
    }
  }

  /**
   * Reads data from the data reader and posts it to solr,
   * writes to the response to output
   */
  public void postData(Reader data, Writer output) {

    HttpURLConnection urlc = null;
    try {
      urlc = (HttpURLConnection) solrUrl.openConnection();
      try {
        urlc.setRequestMethod("POST");
      } catch (ProtocolException e) {
        throw new PostException("Shouldn't happen: HttpURLConnection doesn't support POST??", e);
      }
      urlc.setDoOutput(true);
      urlc.setDoInput(true);
      urlc.setUseCaches(false);
      urlc.setAllowUserInteraction(false);
      urlc.setRequestProperty("Content-type", "text/xml; charset=" + POST_ENCODING);
      
      OutputStream out = urlc.getOutputStream();
      
      try {
        Writer writer = new OutputStreamWriter(out, POST_ENCODING);
        pipe(data, writer);
        writer.close();
      } catch (IOException e) {
        throw new PostException("IOException while posting data", e);
      } finally {
        if(out!=null) out.close();
      }
      
      InputStream in = urlc.getInputStream();
      try {
        Reader reader = new InputStreamReader(in);
        pipe(reader, output);
        reader.close();
      } catch (IOException e) {
        throw new PostException("IOException while reading response", e);
      } finally {
        if(in!=null) in.close();
      }
      
    } catch (IOException e) {
      fatal("Connection error (is Solr running at " + solrUrl + " ?): " + e);
      
    } finally {
      if(urlc!=null) urlc.disconnect();
    }
  }

  /**
   * Pipes everything from the reader to the writer via a buffer
   */
  private static void pipe(Reader reader, Writer writer) throws IOException {
    char[] buf = new char[1024];
    int read = 0;
    while ( (read = reader.read(buf) ) >= 0) {
      writer.write(buf, 0, read);
    }
    writer.flush();
  }
}

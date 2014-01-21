package org.apache.lucene.server;

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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONStyle;
import net.minidev.json.JSONStyleIdent;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

public abstract class ServerBaseTestCase extends LuceneTestCase {

  private static Thread serverThread;
  static int port;

  /** Current index name; we auto-insert this to outgoing
   *  commands that need it. */

  protected static String curIndexName = "index";
  protected static boolean useDefaultIndex = true;
  
  protected static File STATE_DIR;

  /** Last result from the server; tests can access this to
   *  check results. */
  protected static JSONObject lastResult;
  
  /** We record the last indexGen we saw return from the
   *  server, and then insert that for search command if no
   *  searcher is already specified.  This avoids a common
   *  test bug of forgetting to specify which indexGen to
   *  search. */
  private static long lastIndexGen = -1;

  @BeforeClass
  public static void beforeClassServerBase() throws Exception {
    File dir = _TestUtil.getTempDir("ServerBase");
    STATE_DIR = new File(dir, "state");
  }
  
  @AfterClass
  public static void afterClassServerBase() throws Exception {
    // who sets this? netty? what a piece of crap
    System.clearProperty("sun.nio.ch.bugLevel");
    STATE_DIR = null;
    lastResult = null;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    lastIndexGen = -1;
    if (useDefaultIndex) {
      curIndexName = "index";

      // Some tests bounce the server, so we need to restart
      // the default "index" index for those tests that expect
      // it to be running:
      send("startIndex");
    }
  }

  protected long addDocument(String json) throws Exception {
    JSONObject o = send("addDocument", json);
    return ((Number) o.get("indexGen")).longValue();
  }

  protected static void installPlugin(File sourceFile) throws IOException {
    ZipFile zipFile = new ZipFile(sourceFile);
    
    Enumeration<? extends ZipEntry> entries = zipFile.entries();

    File pluginsDir = new File(STATE_DIR, "plugins");
    if (!pluginsDir.exists()) {
      pluginsDir.mkdirs();
    }

    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      
      InputStream in = zipFile.getInputStream(entry);
      File targetFile = new File(pluginsDir, entry.getName());
      if (entry.isDirectory()) {
        // allow unzipping with directory structure
        targetFile.mkdirs();
      } else {
        if (targetFile.getParentFile()!=null) {
          targetFile.getParentFile().mkdirs();   
        }
        OutputStream out = new BufferedOutputStream(new FileOutputStream(targetFile));
        
        byte[] buffer = new byte[8192];
        int len;
        while((len = in.read(buffer)) >= 0) {
          out.write(buffer, 0, len);
        }
        
        in.close();
        out.close();
      }
    }
    
    zipFile.close();
  }

  protected static void put(JSONObject o, String key, String value) throws ParseException {
    o.put(key, JSONValue.parseWithException(value));
  }
    
  protected static void startServer() throws Exception {
    final CountDownLatch ready = new CountDownLatch(1);
    final Exception[] exc = new Exception[1];
    final AtomicReference<Server> theServer = new AtomicReference<Server>();
    serverThread = new Thread() {
        @Override
        public void run() {
          try {
            Server s = new Server(STATE_DIR);
            theServer.set(s);
            s.run(0, 1, ready);
          } catch (Exception e) {
            exc[0] = e;
            ready.countDown();
          }
        }
      };
    serverThread.start();
    if (!ready.await(2, TimeUnit.SECONDS)) {
      throw new IllegalStateException("server took more than 2 seconds to start");
    }
    if (exc[0] != null) {
      throw exc[0];
    }
    ServerBaseTestCase.port = theServer.get().actualPort;
  }

  protected static void createAndStartIndex() throws Exception {
    _TestUtil.rmDir(new File(curIndexName));
    send("createIndex", "{indexName: " + curIndexName + ", rootDir: " + curIndexName + "}");
    // Wait at most 1 msec for a searcher to reopen; this
    // value is too low for a production site but for
    // testing we want to minimize sleep time:
    send("liveSettings", "{indexName: " + curIndexName + ", minRefreshSec: 0.001}");
    send("startIndex", "{indexName: " + curIndexName + "}");
  }

  protected static void shutdownServer() throws Exception {
    send("shutdown");
    if (serverThread != null) {
      serverThread.join();
      serverThread = null;
    }
    lastIndexGen = -1;
  }

  protected static void deleteAllDocs() throws Exception {
    if (VERBOSE) {
      System.out.println("TEST: deleteAllDocs");
    }
    send("deleteAllDocuments", "{indexName: " + curIndexName + "}");
  }

  protected static void commit() throws Exception {
    send("commit", "{indexName: " + curIndexName + "}");
  }

  /** Send a no-args command, or a command taking just
   *  indexName which is automatically added (e.g., commit,
   *  closeIndex, startIndex). */
  protected static JSONObject send(String command) throws Exception {
    if (command.equals("startIndex")) {
      // We do this so tests that index a doc and then need
      // to search it, don't wait very long for the new
      // searcher:
      send("liveSettings", "{minRefreshSec: 0.001}");
    }
    return _send(command, "{}");
  }

  protected static JSONObject send(String command, String args) throws Exception {
    if (args.equals("{}")) {
      throw new IllegalArgumentException("don't pass empty args");
    }
    JSONObject o;
    try {
      o = (JSONObject) new JSONParser(JSONParser.MODE_PERMISSIVE & ~(JSONParser.ACCEPT_TAILLING_DATA)).parse(args);
    } catch (ParseException pe) {
      // NOTE: don't send pe as the cause; it adds lots of
      // unhelpful noise because the message usually states
      // what's wrong very well:
      throw new IllegalArgumentException("test bug: failed to parse json args \"" + args + "\": " + pe.getMessage());
    }
    return send(command, o);
  }

  private static JSONObject _send(String command, String args) throws Exception {
    JSONObject o;
    try {
      o = (JSONObject) new JSONParser(JSONParser.MODE_PERMISSIVE & ~(JSONParser.ACCEPT_TAILLING_DATA)).parse(args);
    } catch (ParseException pe) {
      // NOTE: don't send pe as the cause; it adds lots of
      // unhelpful noise because the message usually states
      // what's wrong very well:
      throw new IllegalArgumentException("test bug: failed to parse json args \"" + args + "\": " + pe.getMessage());
    }
    return send(command, o);
  }

  private static boolean requiresIndexName(String command) {
    if (command.equals("shutdown")) {
      return false;
    }
    return true;
  }

  protected static JSONObject send(String command, JSONObject args) throws Exception {
    // Auto-insert indexName:
    if (curIndexName != null && requiresIndexName(command) && args.get("indexName") == null) {
      if (VERBOSE) {
        System.out.println("NOTE: ServerBaseTestCase: now add current indexName: " + curIndexName);
      }
      args.put("indexName", curIndexName);
    }

    if (command.equals("search") && args.containsKey("searcher") == false && lastIndexGen != -1) {
      if (VERBOSE) {
        System.out.println("\nNOTE: ServerBaseTestCase: inserting 'searcher: {indexGen: " + lastIndexGen + "}' into search request");
      }
      JSONObject o = new JSONObject();
      o.put("indexGen", lastIndexGen);
      args.put("searcher", o);
    }

    if (VERBOSE) {
      System.out.println("\nNOTE: ServerBaseTestCase: sendRaw command=" + command + " args:\n" + args.toJSONString(new JSONStyleIdent()));
    }

    lastResult = sendRaw(command, args.toJSONString(JSONStyle.NO_COMPRESS));

    if (VERBOSE) {
      System.out.println("NOTE: ServerBaseTestCase: server response:\n" + lastResult.toJSONString(new JSONStyleIdent()));
    }

    if (lastResult.containsKey("indexGen")) {
      lastIndexGen = getLong(lastResult, "indexGen");
      if (VERBOSE) {
        System.out.println("NOTE: ServerBaseTestCase: record lastIndexGen=" + lastIndexGen);
      }
    }

    return lastResult;
  }

  protected static JSONObject sendRaw(String command, String body) throws Exception {
    byte[] bytes = body.getBytes("UTF-8");
    HttpURLConnection c = (HttpURLConnection) new URL("http://localhost:" + port + "/" + command).openConnection();
    c.setUseCaches(false);
    c.setDoOutput(true);
    c.setRequestMethod("POST");
    c.setRequestProperty("Content-Length", ""+bytes.length);
    c.setRequestProperty("Charset", "UTF-8");
    try {
      c.getOutputStream().write(bytes);
    } catch (ConnectException ce) {
      System.out.println("FAILED port=" + port + ":");
      ce.printStackTrace(System.out);
      throw ce;
    }
    // c.connect()
    int code = c.getResponseCode();
    int size = c.getContentLength();
    bytes = new byte[size];
    if (code == 200) {
      InputStream is = c.getInputStream();
      is.read(bytes);
      c.disconnect();
      return (JSONObject) JSONValue.parseStrict(new String(bytes, "UTF-8"));
    } else {
      InputStream is = c.getErrorStream();
      is.read(bytes);
      c.disconnect();
      throw new IOException("Server error:\n" + new String(bytes, "UTF-8"));
    }
  }

  protected static void copyFile(File source, File dest) throws IOException {
    InputStream is = null;
    OutputStream os = null;
    try {
      is = new FileInputStream(source);
      os = new FileOutputStream(dest);
      byte[] buffer = new byte[1024];
      int length;
      while ((length = is.read(buffer)) > 0) {
        os.write(buffer, 0, length);
      }
    } finally {
      is.close();
      os.close();
    }
  }

  protected String prettyPrint(JSONObject o) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readValue(o.toString(), JsonNode.class);
    //ObjectWriter writer = mapper.defaultPrettyPrintingWriter();
    // ***IMPORTANT!!!*** for Jackson 2.x use the line below instead of the one above: 
    ObjectWriter writer = mapper.writer().withDefaultPrettyPrinter();
    return writer.writeValueAsString(rootNode);
  }

  protected static String httpLoad(String path) throws Exception {
    HttpURLConnection c = (HttpURLConnection) new URL("http://localhost:" + port + "/" + path).openConnection();
    c.setUseCaches(false);
    c.setDoOutput(true);
    c.setRequestMethod("GET");
    // c.connect()
    int code = c.getResponseCode();
    int size = c.getContentLength();
    byte[] bytes = new byte[size];
    if (code == 200) {
      InputStream is = c.getInputStream();
      is.read(bytes);
      c.disconnect();
      return new String(bytes, "UTF-8");
    } else {
      InputStream is = c.getErrorStream();
      is.read(bytes);
      c.disconnect();
      throw new IOException("Server error:\n" + new String(bytes, "UTF-8"));
    }
  }

  protected static JSONObject sendChunked(String body, String request) throws Exception {
    HttpURLConnection c = (HttpURLConnection) new URL("http://localhost:" + port + "/" + request).openConnection();
    c.setUseCaches(false);
    c.setDoOutput(true);
    c.setChunkedStreamingMode(256);
    c.setRequestMethod("POST");
    c.setRequestProperty("Charset", "UTF-8");
    byte[] bytes = body.getBytes("UTF-8");
    c.getOutputStream().write(bytes);
    // c.connect()
    int code = c.getResponseCode();
    int size = c.getContentLength();
    if (code == 200) {
      InputStream is = c.getInputStream();
      bytes = new byte[size];
      is.read(bytes);
      c.disconnect();
      return (JSONObject) JSONValue.parseStrict(new String(bytes, "UTF-8"));
    } else {
      InputStream is = c.getErrorStream();
      is.read(bytes);
      c.disconnect();
      throw new IOException("Server error:\n" + new String(bytes, "UTF-8"));
    }
  }

  /** Simple xpath-like utility method to jump down and grab
   *  something out of the JSON response. */
  protected static Object get(Object o, String path) {
    int upto = 0;
    int tokStart = 0;
    boolean inArrayIndex = false;
    while(upto < path.length()) {       
      char ch = path.charAt(upto++);
      if (inArrayIndex) {
        if (ch == ']') {
          int index = Integer.parseInt(path.substring(tokStart, upto-1));
          o = ((JSONArray) o).get(index);
          inArrayIndex = false;
          tokStart = upto;
        }
      } else if (ch == '.' || ch == '[') {
        String name = path.substring(tokStart, upto-1);
        if (name.length() != 0) {
          o = ((JSONObject) o).get(name);
          if (o == null) {
            // Likely a test bug: try to help out:
            throw new IllegalArgumentException("path " + path.substring(0, tokStart-1) + " does not have member ." + name);
          }
        }
        tokStart = upto;
        if (ch == '[') {
          inArrayIndex = true;
        }
      }
    }

    String name = path.substring(tokStart, upto);
    if (name.length() > 0) {
      if (o instanceof JSONArray && name.equals("length")) {
        o = new Integer(((JSONArray) o).size());
      } else {
        o = ((JSONObject) o).get(name);
        if (o == null) {
          // Likely a test bug: try to help out:
          throw new IllegalArgumentException("path " + path.substring(0, tokStart) + " does not have member ." + name);
        }
      }
    }
    return o;
  }

  protected boolean hasParam(Object o, String path) {
    try {
      get(o, path);
      return true;
    } catch (IllegalArgumentException iae) {
      return false;
    }
  }

  protected boolean hasParam(String path) {
    return hasParam(lastResult, path);
  }

  protected static String getString(Object o, String path) {
    return (String) get(o, path);
  }

  protected static String getString(String path) {
    return getString(lastResult, path);
  }

  protected static int getInt(Object o, String path) {
    return ((Number) get(o, path)).intValue();
  }

  protected static int getInt(String path) {
    return getInt(lastResult, path);
  }

  protected static boolean getBoolean(Object o, String path) {
    return ((Boolean) get(o, path)).booleanValue();
  }

  protected static boolean getBoolean(String path) {
    return getBoolean(lastResult, path);
  }

  protected static long getLong(Object o, String path) {
    return ((Number) get(o, path)).longValue();
  }

  protected static long getLong(String path) {
    return getLong(lastResult, path);
  }

  protected static float getFloat(Object o, String path) {
    return ((Number) get(o, path)).floatValue();
  }

  protected static float getFloat(String path) {
    return getFloat(lastResult, path);
  }

  protected static JSONObject getObject(Object o, String path) {
    return (JSONObject) get(o, path);
  }

  protected static JSONObject getObject(String path) {
    return getObject(lastResult, path);
  }

  protected static JSONArray getArray(Object o, String path) {
    return (JSONArray) get(o, path);
  }

  protected static JSONArray getArray(String path) {
    return getArray(lastResult, path);
  }

  protected static JSONArray getArray(JSONArray o, int index) {
    return (JSONArray) o.get(index);
  }

  /** Renders one hilited field (multiple passages) value
   * with <b>...</b> tags, and ... separating the passages. */ 
  protected String renderHighlight(JSONArray hit) {
    StringBuilder sb = new StringBuilder();
    for(Object o : hit) {
      if (sb.length() != 0) {
        sb.append("...");
      }
      sb.append(renderSingleHighlight(getArray(o, "parts")));
    }

    return sb.toString();
  }

  /** Renders a single passage with <b>...</b> tags. */
  protected String renderSingleHighlight(JSONArray passage) {
    StringBuilder sb = new StringBuilder();
    for(Object o2 : passage) {
      if (o2 instanceof String) {
        sb.append((String) o2);
      } else {
        JSONObject obj = (JSONObject) o2;
        sb.append("<b>");
        sb.append(obj.get("text"));
        sb.append("</b>");
      }
    }

    return sb.toString();
  }

  /** Sends the command + args, expecting a failure such
   *  that all fragments occur in the failure message
   *  string.  Use this to verify a failure case is hitting
   *  the right error messages back to the user. */
  protected void assertFailsWith(String command, String args, String... fragments) throws Exception {
    try {
      send(command, args);
      fail("did not hit expected exception");
    } catch (IOException ioe) {
      for(String fragment : fragments) {
        if (ioe.getMessage().contains(fragment) == false) {
          fail("expected: " + fragment + "\nactual: \"" + ioe.getMessage());
        }
      }
    }
  }
}


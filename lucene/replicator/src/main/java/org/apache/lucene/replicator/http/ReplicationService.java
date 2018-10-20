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
package org.apache.lucene.replicator.http;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpStatus;
import org.apache.lucene.replicator.Replicator;
import org.apache.lucene.replicator.SessionToken;

/**
 * A server-side service for handling replication requests. The service assumes
 * requests are sent in the format
 * <code>/&lt;context&gt;/&lt;shard&gt;/&lt;action&gt;</code> where
 * <ul>
 * <li>{@code context} is the servlet context, e.g. {@link #REPLICATION_CONTEXT}
 * <li>{@code shard} is the ID of the shard, e.g. "s1"
 * <li>{@code action} is one of {@link ReplicationAction} values
 * </ul>
 * For example, to check whether there are revision updates for shard "s1" you
 * should send the request: <code>http://host:port/replicate/s1/update</code>.
 * <p>
 * This service is written like a servlet, and
 * {@link #perform(HttpServletRequest, HttpServletResponse)} takes servlet
 * request and response accordingly, so it is quite easy to embed in your
 * application's servlet.
 * 
 * @lucene.experimental
 */
public class ReplicationService {
  
  /** Actions supported by the {@link ReplicationService}. */
  public enum ReplicationAction {
    OBTAIN, RELEASE, UPDATE
  }
  
  /** The context path for the servlet. */
  public static final String REPLICATION_CONTEXT = "/replicate";
  
  /** Request parameter name for providing the revision version. */
  public final static String REPLICATE_VERSION_PARAM = "version";
  
  /** Request parameter name for providing a session ID. */
  public final static String REPLICATE_SESSION_ID_PARAM = "sessionid";
  
  /** Request parameter name for providing the file's source. */
  public final static String REPLICATE_SOURCE_PARAM = "source";
  
  /** Request parameter name for providing the file's name. */
  public final static String REPLICATE_FILENAME_PARAM = "filename";
  
  private static final int SHARD_IDX = 0, ACTION_IDX = 1;
  
  private final Map<String,Replicator> replicators;
  
  public ReplicationService(Map<String,Replicator> replicators) {
    super();
    this.replicators = replicators;
  }
  
  /**
   * Returns the path elements that were given in the servlet request, excluding
   * the servlet's action context.
   */
  private String[] getPathElements(HttpServletRequest req) {
    String path = req.getServletPath();
    String pathInfo = req.getPathInfo();
    if (pathInfo != null) {
      path += pathInfo;
    }
    int actionLen = REPLICATION_CONTEXT.length();
    int startIdx = actionLen;
    if (path.length() > actionLen && path.charAt(actionLen) == '/') {
      ++startIdx;
    }
    
    // split the string on '/' and remove any empty elements. This is better
    // than using String.split() since the latter may return empty elements in
    // the array
    StringTokenizer stok = new StringTokenizer(path.substring(startIdx), "/");
    ArrayList<String> elements = new ArrayList<>();
    while (stok.hasMoreTokens()) {
      elements.add(stok.nextToken());
    }
    return elements.toArray(new String[0]);
  }
  
  private static String extractRequestParam(HttpServletRequest req, String paramName) throws ServletException {
    String param = req.getParameter(paramName);
    if (param == null) {
      throw new ServletException("Missing mandatory parameter: " + paramName);
    }
    return param;
  }
  
  private static void copy(InputStream in, OutputStream out) throws IOException {
    byte[] buf = new byte[16384];
    int numRead;
    while ((numRead = in.read(buf)) != -1) {
      out.write(buf, 0, numRead);
    }
  }
  
  /** Executes the replication task. */
  public void perform(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String[] pathElements = getPathElements(req);
    
    if (pathElements.length != 2) {
      throw new ServletException("invalid path, must contain shard ID and action, e.g. */s1/update");
    }
    
    final ReplicationAction action;
    try {
      action = ReplicationAction.valueOf(pathElements[ACTION_IDX].toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new ServletException("Unsupported action provided: " + pathElements[ACTION_IDX]);
    }
    
    final Replicator replicator = replicators.get(pathElements[SHARD_IDX]);
    if (replicator == null) {
      throw new ServletException("unrecognized shard ID " + pathElements[SHARD_IDX]);
    }
    
    // SOLR-8933 Don't close this stream.
    ServletOutputStream resOut = resp.getOutputStream();
    try {
      switch (action) {
        case OBTAIN:
          final String sessionID = extractRequestParam(req, REPLICATE_SESSION_ID_PARAM);
          final String fileName = extractRequestParam(req, REPLICATE_FILENAME_PARAM);
          final String source = extractRequestParam(req, REPLICATE_SOURCE_PARAM);
          InputStream in = replicator.obtainFile(sessionID, source, fileName);
          try {
            copy(in, resOut);
          } finally {
            in.close();
          }
          break;
        case RELEASE:
          replicator.release(extractRequestParam(req, REPLICATE_SESSION_ID_PARAM));
          break;
        case UPDATE:
          String currVersion = req.getParameter(REPLICATE_VERSION_PARAM);
          SessionToken token = replicator.checkForUpdate(currVersion);
          if (token == null) {
            resOut.write(0); // marker for null token
          } else {
            resOut.write(1);
            token.serialize(new DataOutputStream(resOut));
          }
          break;
      }
    } catch (Exception e) {
      resp.setStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR); // propagate the failure
      try {
        /*
         * Note: it is assumed that "identified exceptions" are thrown before
         * anything was written to the stream.
         */
        ObjectOutputStream oos = new ObjectOutputStream(resOut);
        oos.writeObject(e);
        oos.flush();
      } catch (Exception e2) {
        throw new IOException("Could not serialize", e2);
      }
    } finally {
      resp.flushBuffer();
    }
  }
  
}

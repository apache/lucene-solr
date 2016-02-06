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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.Callable;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;

/**
 * Base class for Http clients.
 * 
 * @lucene.experimental
 * */
public abstract class HttpClientBase implements Closeable {
  
  /** Default connection timeout for this client, in milliseconds. */
  public static final int DEFAULT_CONNECTION_TIMEOUT = 1000;
  
  /** Default socket timeout for this client, in milliseconds. */
  public static final int DEFAULT_SO_TIMEOUT = 60000;
  
  // TODO compression?
  
  /** The URL stting to execute requests against. */
  protected final String url;
  
  private volatile boolean closed = false;
  
  private final CloseableHttpClient httpc;
  private final RequestConfig defaultConfig;
  
  /**
   * @param conMgr
   *          connection manager to use for this http client. <b>NOTE:</b>The
   *          provided {@link HttpClientConnectionManager} will not be
   *          {@link HttpClientConnectionManager#shutdown()} by this class.
   * @param defaultConfig
   *          the default {@link RequestConfig} to set on the client. If
   *          {@code null} a default config is created w/ the default connection
   *          and socket timeouts.
   */
  protected HttpClientBase(String host, int port, String path, HttpClientConnectionManager conMgr, RequestConfig defaultConfig) {
    url = normalizedURL(host, port, path);
    if (defaultConfig == null) {
      this.defaultConfig = RequestConfig.custom()
          .setConnectionRequestTimeout(DEFAULT_CONNECTION_TIMEOUT)
          .setSocketTimeout(DEFAULT_SO_TIMEOUT).build();
    } else {
      this.defaultConfig = defaultConfig;
    }
    httpc = HttpClientBuilder.create().setConnectionManager(conMgr).setDefaultRequestConfig(this.defaultConfig).build();
  }
  
  /** Throws {@link AlreadyClosedException} if this client is already closed. */
  protected final void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("HttpClient already closed");
    }
  }
  
  /**
   * Create a URL out of the given parameters, translate an empty/null path to '/'
   */
  private static String normalizedURL(String host, int port, String path) {
    if (path == null || path.length() == 0) {
      path = "/";
    }
    return "http://" + host + ":" + port + path;
  }
  
  /**
   * <b>Internal:</b> response status after invocation, and in case or error attempt to read the 
   * exception sent by the server. 
   */
  protected void verifyStatus(HttpResponse response) throws IOException {
    StatusLine statusLine = response.getStatusLine();
    if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
      try {
        throwKnownError(response, statusLine); 
      } finally {
        EntityUtils.consumeQuietly(response.getEntity());
      }
    }
  }
  
  protected void throwKnownError(HttpResponse response, StatusLine statusLine) throws IOException {
    ObjectInputStream in = null;
    try {
      in = new ObjectInputStream(response.getEntity().getContent());
    } catch (Throwable t) {
      // the response stream is not an exception - could be an error in servlet.init().
      throw new RuntimeException("Unknown error: " + statusLine, t);
    }
    
    Throwable t;
    try {
      t = (Throwable) in.readObject();
    } catch (Throwable th) { 
      throw new RuntimeException("Failed to read exception object: " + statusLine, th);
    } finally {
      in.close();
    }
    IOUtils.reThrow(t);
  }
  
  /**
   * <b>internal:</b> execute a request and return its result
   * The <code>params</code> argument is treated as: name1,value1,name2,value2,...
   */
  protected HttpResponse executePOST(String request, HttpEntity entity, String... params) throws IOException {
    ensureOpen();
    HttpPost m = new HttpPost(queryString(request, params));
    m.setEntity(entity);
    HttpResponse response = httpc.execute(m);
    verifyStatus(response);
    return response;
  }
  
  /**
   * <b>internal:</b> execute a request and return its result
   * The <code>params</code> argument is treated as: name1,value1,name2,value2,...
   */
  protected HttpResponse executeGET(String request, String... params) throws IOException {
    ensureOpen();
    HttpGet m = new HttpGet(queryString(request, params));
    HttpResponse response = httpc.execute(m);
    verifyStatus(response);
    return response;
  }
  
  private String queryString(String request, String... params) throws UnsupportedEncodingException {
    StringBuilder query = new StringBuilder(url).append('/').append(request).append('?');
    if (params != null) {
      for (int i = 0; i < params.length; i += 2) {
        query.append(params[i]).append('=').append(URLEncoder.encode(params[i+1], "UTF8")).append('&');
      }
    }
    return query.substring(0, query.length() - 1);
  }
  
  /** Internal utility: input stream of the provided response */
  public InputStream responseInputStream(HttpResponse response) throws IOException {
    return responseInputStream(response, false);
  }
  
  // TODO: can we simplify this Consuming !?!?!?
  /**
   * Internal utility: input stream of the provided response, which optionally 
   * consumes the response's resources when the input stream is exhausted.
   */
  public InputStream responseInputStream(HttpResponse response, boolean consume) throws IOException {
    final HttpEntity entity = response.getEntity();
    final InputStream in = entity.getContent();
    if (!consume) {
      return in;
    }
    return new InputStream() {
      private boolean consumed = false;
      @Override
      public int read() throws IOException {
        final int res = in.read();
        consume(res);
        return res;
      }
      @Override
      public void close() throws IOException {
        in.close();
        consume(-1);
      }
      @Override
      public int read(byte[] b) throws IOException {
        final int res = in.read(b);
        consume(res);
        return res;
      }
      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        final int res = in.read(b, off, len);
        consume(res);
        return res;
      }
      private void consume(int minusOne) {
        if (!consumed && minusOne == -1) {
          try {
            EntityUtils.consume(entity);
          } catch (Exception e) {
            // ignored on purpose
          }
          consumed = true;
        }
      }
    };
  }
  
  /**
   * Returns true iff this instance was {@link #close() closed}, otherwise
   * returns false. Note that if you override {@link #close()}, you must call
   * {@code super.close()}, in order for this instance to be properly closed.
   */
  protected final boolean isClosed() {
    return closed;
  }
  
  /**
   * Same as {@link #doAction(HttpResponse, boolean, Callable)} but always do consume at the end.
   */
  protected <T> T doAction(HttpResponse response, Callable<T> call) throws IOException {
    return doAction(response, true, call);
  }
  
  /**
   * Do a specific action and validate after the action that the status is still OK, 
   * and if not, attempt to extract the actual server side exception. Optionally
   * release the response at exit, depending on <code>consume</code> parameter.
   */
  protected <T> T doAction(HttpResponse response, boolean consume, Callable<T> call) throws IOException {
    Throwable th = null;
    try {
      return call.call();
    } catch (Throwable t) {
      th = t;
    } finally {
      try {
        verifyStatus(response);
      } finally {
        if (consume) {
          EntityUtils.consumeQuietly(response.getEntity());
        }
      }
    }
    assert th != null; // extra safety - if we get here, it means the callable failed
    IOUtils.reThrow(th);
    return null; // silly, if we're here, IOUtils.reThrow always throws an exception 
  }
  
  @Override
  public void close() throws IOException {
    httpc.close();
    closed = true;
  }
  
}

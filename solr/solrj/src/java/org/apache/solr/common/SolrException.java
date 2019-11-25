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
package org.apache.solr.common;

import java.io.CharArrayWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.MDC;

/**
 *
 */
@SuppressWarnings("unchecked")
public class SolrException extends RuntimeException {

  public static final String ROOT_ERROR_CLASS = "root-error-class";
  public static final String ERROR_CLASS = "error-class";
  final private Map mdcContext;

  /**
   * This list of valid HTTP Status error codes that Solr may return in 
   * the case of a "Server Side" error.
   *
   * @since solr 1.2
   */
  public enum ErrorCode {
    BAD_REQUEST( 400 ),
    UNAUTHORIZED( 401 ),
    FORBIDDEN( 403 ),
    NOT_FOUND( 404 ),
    CONFLICT( 409 ),
    UNSUPPORTED_MEDIA_TYPE( 415 ),
    SERVER_ERROR( 500 ),
    SERVICE_UNAVAILABLE( 503 ),
    INVALID_STATE( 510 ),
    UNKNOWN(0);
    public final int code;
    
    private ErrorCode( int c )
    {
      code = c;
    }
    public static ErrorCode getErrorCode(int c){
      for (ErrorCode err : values()) {
        if(err.code == c) return err;
      }
      return UNKNOWN;
    }
  };

  public SolrException(ErrorCode code, String msg) {
    super(msg);
    this.code = code.code;
    this.mdcContext = MDC.getCopyOfContextMap();
  }
  public SolrException(ErrorCode code, String msg, Throwable th) {
    super(msg, th);
    this.code = code.code;
    this.mdcContext = MDC.getCopyOfContextMap();
  }

  public SolrException(ErrorCode code, Throwable th) {
    super(th);
    this.code = code.code;
    this.mdcContext = MDC.getCopyOfContextMap();
  }

  /**
   * Constructor that can set arbitrary http status code. Not for 
   * use in Solr, but may be used by clients in subclasses to capture 
   * errors returned by the servlet container or other HTTP proxies.
   */
  protected SolrException(int code, String msg, Throwable th) {
    super(msg, th);
    this.code = code;
    this.mdcContext = MDC.getCopyOfContextMap();
  }
  
  int code=0;
  protected NamedList<String> metadata;

  /**
   * The HTTP Status code associated with this Exception.  For SolrExceptions 
   * thrown by Solr "Server Side", this should valid {@link ErrorCode}, 
   * however client side exceptions may contain an arbitrary error code based 
   * on the behavior of the Servlet Container hosting Solr, or any HTTP 
   * Proxies that may exist between the client and the server.
   *
   * @return The HTTP Status code associated with this Exception
   */
  public int code() { return code; }

  public void setMetadata(NamedList<String> metadata) {
    this.metadata = metadata;
  }

  public NamedList<String> getMetadata() {
    return metadata;
  }

  public String getMetadata(String key) {
    return (metadata != null && key != null) ? metadata.get(key) : null;
  }

  public void setMetadata(String key, String value) {
    if (key == null || value == null)
      throw new IllegalArgumentException("Exception metadata cannot be null!");

    if (metadata == null)
      metadata = new NamedList<String>();
    metadata.add(key, value);
  }
  
  public String getThrowable() {
    return getMetadata(ERROR_CLASS);
  }

  public String getRootThrowable() {
    return getMetadata(ROOT_ERROR_CLASS);
  }

  public void log(Logger log) { log(log,this); }
  public static void log(Logger log, Throwable e) {
    String stackTrace = toStr(e);
    String ignore = doIgnore(e, stackTrace);
    if (ignore != null) {
      log.info(ignore);
      return;
    }
    log.error(stackTrace);

  }

  public static void log(Logger log, String msg, Throwable e) {
    String stackTrace = msg + ':' + toStr(e);
    String ignore = doIgnore(e, stackTrace);
    if (ignore != null) {
      log.info(ignore);
      return;
    }
    log.error(stackTrace);
  }
  
  public static void log(Logger log, String msg) {
    String stackTrace = msg;
    String ignore = doIgnore(null, stackTrace);
    if (ignore != null) {
      log.info(ignore);
      return;
    }
    log.error(stackTrace);
  }

  // public String toString() { return toStr(this); }  // oops, inf loop
  @Override
  public String toString() { return super.toString(); }

  public static String toStr(Throwable e) {   
    CharArrayWriter cw = new CharArrayWriter();
    PrintWriter pw = new PrintWriter(cw);
    e.printStackTrace(pw);
    pw.flush();
    return cw.toString();

/** This doesn't work for some reason!!!!!
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    pw.flush();
    System.out.println("The STRING:" + sw.toString());
    return sw.toString();
**/
  }


  /**
   * For test code - do not log exceptions that match any of these regular expressions.
   * A {@link java.util.concurrent.CopyOnWriteArraySet is recommended}.
   */
  public static Set<String> ignorePatterns;

  /** Returns null if this exception does not match any ignore patterns, or a message string to use if it does. */
  public static String doIgnore(Throwable t, String m) {
    Set<String> ignorePatterns = SolrException.ignorePatterns; // guard against races, albeit unlikely
    if (ignorePatterns == null || m == null) return null;
    if (t != null && t instanceof AssertionError) return null;

    for (String regex : ignorePatterns) {
      Pattern pattern = Pattern.compile(regex); // TODO why do we compile late; why not up-front?
      Matcher matcher = pattern.matcher(m);
      
      if (matcher.find()) return "Ignoring exception matching " + regex;
    }

    return null;
  }
  
    public static Throwable getRootCause(Throwable t) {
    while (true) {
      Throwable cause = t.getCause();
      if (cause!=null) {
        t = cause;
      } else {
        break;
      }
    }
    return t;
  }

  public void logInfoWithMdc(Logger logger, String msg) {
    Map previousMdcContext = MDC.getCopyOfContextMap();
    MDC.setContextMap(mdcContext);
    try {
      logger.info(msg);
    } finally{
      MDC.setContextMap(previousMdcContext);
    }
  }

  public void logDebugWithMdc(Logger logger, String msg) {
    Map previousMdcContext = MDC.getCopyOfContextMap();
    MDC.setContextMap(mdcContext);
    try {
      logger.debug(msg);
    } finally{
      MDC.setContextMap(previousMdcContext);
    }
  }

  public void logWarnWithMdc(Logger logger, String msg) {
    Map previousMdcContext = MDC.getCopyOfContextMap();
    MDC.setContextMap(mdcContext);
    try {
      logger.warn(msg);
    } finally{
      MDC.setContextMap(previousMdcContext);
    }
  }

}

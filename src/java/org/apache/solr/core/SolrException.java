/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.core;

import java.util.logging.Logger;
import java.io.CharArrayWriter;
import java.io.PrintWriter;

/**
 * @author yonik
 * @version $Id: SolrException.java,v 1.6 2005/06/14 20:42:26 yonik Exp $
 */


public class SolrException extends RuntimeException {
  public boolean logged=false;

  public SolrException(int code, String msg) {
    super(msg);
    this.code=code;
  }
  public SolrException(int code, String msg, boolean alreadyLogged) {
    super(msg);
    this.code=code;
    this.logged=alreadyLogged;
  }

  public SolrException(int code, String msg, Throwable th, boolean alreadyLogged) {
    super(msg,th);
    this.code=code;
    logged=alreadyLogged;
  }

  public SolrException(int code, String msg, Throwable th) {
    this(code,msg,th,true);
  }

  public SolrException(int code, Throwable th) {
    super(th);
    this.code=code;
    logged=true;
  }

  int code=0;
  public int code() { return code; }




  public void log(Logger log) { log(log,this); }
  public static void log(Logger log, Throwable e) {
    log.severe(toStr(e));
    if (e instanceof SolrException) {
      ((SolrException)e).logged = true;
    }
  }

  public static void log(Logger log, String msg, Throwable e) {
    log.severe(msg + ':' + toStr(e));
    if (e instanceof SolrException) {
      ((SolrException)e).logged = true;
    }
  }

  public static void logOnce(Logger log, String msg, Throwable e) {
    if (e instanceof SolrException) {
      if(((SolrException)e).logged) return;
    }
    if (msg!=null) log(log,msg,e);
    else log(log,e);
  }


  // public String toString() { return toStr(this); }  // oops, inf loop
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

}

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

package org.apache.solr.handler.admin;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * Copied from "admin/threaddump.jsp"
 * 
 * NOTE: the response format is still likely to change.  It should be designed so
 * that it works nicely with an XSLT transformation.  Until we have a nice
 * XSLT front end for /admin, the format is still open to change.
 * 
 *
 * @since solr 1.2
 */
public class ThreadDumpHandler extends RequestHandlerBase
{
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException 
  {    
    SimpleOrderedMap<Object> system = new SimpleOrderedMap<Object>();
    rsp.add( "system", system );

    ThreadMXBean tmbean = ManagementFactory.getThreadMXBean();
    
    // Thread Count
    SimpleOrderedMap<Object> nl = new SimpleOrderedMap<Object>();
    nl.add( "current",tmbean.getThreadCount() );
    nl.add( "peak", tmbean.getPeakThreadCount() );
    nl.add( "daemon", tmbean.getDaemonThreadCount() );
    system.add( "threadCount", nl );
    
    // Deadlocks
    ThreadInfo[] tinfos;
    long[] tids = tmbean.findMonitorDeadlockedThreads();
    if (tids != null) {
      tinfos = tmbean.getThreadInfo(tids, Integer.MAX_VALUE);
      NamedList<SimpleOrderedMap<Object>> lst = new NamedList<SimpleOrderedMap<Object>>();
      for (ThreadInfo ti : tinfos) {
        lst.add( "thread", getThreadInfo( ti, tmbean ) );
      }
      system.add( "deadlocks", lst );
    }
    
    // Now show all the threads....
    tids = tmbean.getAllThreadIds();
    tinfos = tmbean.getThreadInfo(tids, Integer.MAX_VALUE);
    NamedList<SimpleOrderedMap<Object>> lst = new NamedList<SimpleOrderedMap<Object>>();
    for (ThreadInfo ti : tinfos) {
      lst.add( "thread", getThreadInfo( ti, tmbean ) );
    }
    system.add( "threadDump", lst );
    rsp.setHttpCaching(false);
  }

  //--------------------------------------------------------------------------------
  //--------------------------------------------------------------------------------
  
  private static SimpleOrderedMap<Object> getThreadInfo( ThreadInfo ti, ThreadMXBean tmbean ) throws IOException 
  {
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<Object>();
    long tid = ti.getThreadId();

    info.add( "id", tid );
    info.add( "name", ti.getThreadName() );
    info.add( "state", ti.getThreadState().toString() );
    
    if (ti.getLockName() != null) {
      info.add( "lock", ti.getLockName() );
    }
    if (ti.isSuspended()) {
      info.add( "suspended", true );
    }
    if (ti.isInNative()) {
      info.add( "native", true );
    }
    
    if (tmbean.isThreadCpuTimeSupported()) {
      info.add( "cpuTime", formatNanos(tmbean.getThreadCpuTime(tid)) );
      info.add( "userTime", formatNanos(tmbean.getThreadUserTime(tid)) );
    }

    if (ti.getLockOwnerName() != null) {
      SimpleOrderedMap<Object> owner = new SimpleOrderedMap<Object>();
      owner.add( "name", ti.getLockOwnerName() );
      owner.add( "id", ti.getLockOwnerId() );
    }
    
    // Add the stack trace
    int i=0;
    String[] trace = new String[ti.getStackTrace().length];
    for( StackTraceElement ste : ti.getStackTrace()) {
      trace[i++] = ste.toString();
    }
    info.add( "stackTrace", trace );
    return info;
  }
  
  private static String formatNanos(long ns) {
    return String.format("%.4fms", ns / (double) 1000000);
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Thread Dump";
  }

  @Override
  public String getVersion() {
      return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}

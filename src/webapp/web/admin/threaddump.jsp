<%@ page contentType="text/xml; charset=utf-8" pageEncoding="UTF-8" language="java" %>
<%--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
--%>
<%@ page import="org.apache.solr.core.SolrCore,
                 java.lang.management.ManagementFactory,
                 java.lang.management.ThreadMXBean,
                 java.lang.management.ThreadInfo,
                 java.io.IOException,
                 org.apache.solr.common.util.XML"%>
<%@include file="_info.jsp" %>


<?xml-stylesheet type="text/xsl" href="threaddump.xsl"?>
<%!
  static ThreadMXBean tmbean = ManagementFactory.getThreadMXBean();
%>
<solr>
  <core><%= collectionName %></core>
  <system>
  <jvm>
    <version><%=System.getProperty("java.vm.version")%></version>
    <name><%=System.getProperty("java.vm.name")%></name>
  </jvm>
  <threadCount>
    <current><%=tmbean.getThreadCount()%></current>
    <peak><%=tmbean.getPeakThreadCount()%></peak>
    <daemon><%=tmbean.getDaemonThreadCount()%></daemon>
  </threadCount>
<%
  long[] tids;
  ThreadInfo[] tinfos;
  tids = tmbean.findMonitorDeadlockedThreads();
  if (tids != null) {
      out.println("  <deadlocks>");
      tinfos = tmbean.getThreadInfo(tids, Integer.MAX_VALUE);
      for (ThreadInfo ti : tinfos) {
          printThreadInfo(ti, out);
      }
      out.println("  </deadlocks>");
  }
%>
<%
  tids = tmbean.getAllThreadIds();
  tinfos = tmbean.getThreadInfo(tids, Integer.MAX_VALUE);
  out.println("  <threadDump>");
  for (ThreadInfo ti : tinfos) {
     printThreadInfo(ti, out);
  }
  out.println("  </threadDump>");
%>
  </system>
</solr>

<%!
  static void printThreadInfo(ThreadInfo ti, JspWriter out) throws IOException {
      long tid = ti.getThreadId();
      out.println("    <thread>");
      out.println("      <id>" + tid + "</id>");
      out.print("      <name>");
      XML.escapeCharData(ti.getThreadName(), out);
      out.println("</name>");
      out.println("      <state>" + ti.getThreadState() + "</state>");
      if (ti.getLockName() != null) {
          out.println("      <lock>" + ti.getLockName() + "</lock>");
      }
      if (ti.isSuspended()) {
          out.println("      <suspended/>");
      }
      if (ti.isInNative()) {
          out.println("      <inNative/>");
      }
      if (tmbean.isThreadCpuTimeSupported()) {
          out.println("      <cpuTime>" + formatNanos(tmbean.getThreadCpuTime(tid)) + "</cpuTime>");
          out.println("      <userTime>" + formatNanos(tmbean.getThreadUserTime(tid)) + "</userTime>");
      }

      if (ti.getLockOwnerName() != null) {
          out.println("      <owner>");
          out.println("        <name>" + ti.getLockOwnerName() + "</name>");
          out.println("        <id>" + ti.getLockOwnerId() + "</id>");
          out.println("      </owner>");
      }
      out.println("      <stackTrace>");
      for (StackTraceElement ste : ti.getStackTrace()) {
          out.print("        <line>");
          XML.escapeCharData("at " + ste.toString(), out);
          out.println("        </line>");
      }
      out.println("      </stackTrace>");
      out.println("    </thread>");
  }

  static String formatNanos(long ns) {
      return String.format("%.4fms", ns / (double) 1000000);
  }
%>

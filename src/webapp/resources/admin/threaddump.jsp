<%@ page contentType="text/html; charset=utf-8" pageEncoding="UTF-8"%>
<%@ page import="java.lang.management.ManagementFactory,
                 java.lang.management.ThreadMXBean,
                 java.lang.management.ThreadInfo,
                 java.io.IOException"%>
<%@include file="header.jsp" %>
<%!
  static ThreadMXBean tmbean = ManagementFactory.getThreadMXBean();
%>
<br clear="all">
<h2>Thread Dump</h2>
<table>
<tr>
<td>
<%
  out.print(System.getProperty("java.vm.name") +
            " " + System.getProperty("java.vm.version") + "<br>");
%>
</td>
</tr>
<tr>
<td>
<%
  long[] tids;
  ThreadInfo[] tinfos;

  out.print("Thread Count: current=" + tmbean.getThreadCount() +
            " deamon=" + tmbean.getDaemonThreadCount() +
            " peak=" + tmbean.getPeakThreadCount());
%>
</td>
</tr>
<tr>
<td>
<%
  tids = tmbean.findMonitorDeadlockedThreads();
  if (tids == null) {
      out.print("No deadlock found.");
  }
  else {
      out.print("Deadlock found :-");
      tinfos = tmbean.getThreadInfo(tids, Integer.MAX_VALUE);
      for (ThreadInfo ti : tinfos) {
          printThreadInfo(ti, out);
      }
  }
%>
</td>
</tr>
<tr>
<td>
<%
  out.print("Full Thread Dump:<br>");
  tids = tmbean.getAllThreadIds();
  tinfos = tmbean.getThreadInfo(tids, Integer.MAX_VALUE);
  for (ThreadInfo ti : tinfos) {
     printThreadInfo(ti, out);
  }
%>
</td>
</tr>
</table>
<br><br>
    <a href=".">Return to Admin Page</a>
</body>
</html>

<%!
  static String INDENT = "&nbsp&nbsp&nbsp&nbsp ";

  static void printThreadInfo(ThreadInfo ti, JspWriter out) throws IOException {
      long tid = ti.getThreadId();
      StringBuilder sb = new StringBuilder("\"" + ti.getThreadName() + "\"" +
                                           " Id=" + tid +
                                           " in " + ti.getThreadState());
      if (ti.getLockName() != null) {
          sb.append(" on lock=" + ti.getLockName());
      }
      if (ti.isSuspended()) {
          sb.append(" (suspended)");
      }
      if (ti.isInNative()) {
          sb.append(" (running in native)");
      }
      if (tmbean.isThreadCpuTimeSupported()) {
          sb.append(" total cpu time="
                    +formatNanos(tmbean.getThreadCpuTime(tid)));
          sb.append(" user time="
                    +formatNanos(tmbean.getThreadUserTime(tid)));
      }
      out.print(sb.toString()+"<br>");
      if (ti.getLockOwnerName() != null) {
          out.print(INDENT + " owned by " + ti.getLockOwnerName() +
                    " Id=" + ti.getLockOwnerId()+"<br>");
      }
      for (StackTraceElement ste : ti.getStackTrace()) {
          out.print(INDENT + "at " + ste.toString()+"<br>");
      }
      out.print("<br>");
  }

  static String formatNanos(long ns) {
      return String.format("%.4fms", ns / (double) 1000000);
  }
%>

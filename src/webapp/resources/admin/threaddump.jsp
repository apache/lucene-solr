<%@ page import="java.io.BufferedReader,
                 java.io.FileReader"%>
<%@include file="header.jsp" %>
<%
  File getinfo = new File("logs/jvm.log");
%>
<br clear="all">
<%
  Runtime rt = Runtime.getRuntime();
  Process p = rt.exec("./getinfo");
  p.waitFor();
%>
<table>
  <tr>
    <td>
      <H3>Exit Value:</H3>
    </td>
    <td>
      <%= p.exitValue() %>
    </td>
  </tr>
</table>
<br>
<table>
  <tr>
    <td>
    </td>
    <td>
      Current Time: <%= new Date().toString() %>
    </td>
  </tr>
  <tr>
    <td>
    </td>
    <td>
      Server Start At: <%= startTime %>
    </td>
  </tr>
</table>
<%
  BufferedReader in = new BufferedReader(new FileReader(getinfo));
  StringBuffer buf = new StringBuffer();
  String line;

  while((line = in.readLine()) != null) {
    if (line.startsWith("taking thread dump")) {
      buf = new StringBuffer();
    }
    buf.append(line).append("<br>");
  }
%>
<br>
Thread Dumps
<table>
  <tr>
    <td>
    </td>
    <td>
      [<a href=get-file.jsp?file=logs/jvm.log>All Entries</a>]
    </td>
  </tr>
  <tr>
    <td>
      Last Entry
    </td>
    <td>
      <%= buf.toString() %>
    </td>
  </tr>
</table>
<br><br>
    <a href=".">Return to Admin Page</a>
</body>
</html>

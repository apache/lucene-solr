<%@ page contentType="text/html; charset=utf-8" pageEncoding="UTF-8"%>
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
<%@ page import="javax.servlet.jsp.JspWriter,java.io.IOException,org.apache.zookeeper.*,org.apache.zookeeper.data.Stat,org.apache.solr.core.*,org.apache.solr.cloud.*,org.apache.solr.common.cloud.*,org.apache.solr.common.util.*,java.util.concurrent.TimeoutException"%>
<%@ page import="java.io.*"%>
<%@ page import="java.util.*"%>
<%@ page import="java.net.URLEncoder"%>

<%@include file="header.jsp" %>

<br clear="all">
<h2>Zookeeper Browser</h2>

<%
  String path = request.getParameter("path");
  String addr = request.getParameter("addr");
  if (addr != null && addr.length() == 0)
    addr = null;
  String detailS = request.getParameter("detail");
  boolean detail = detailS != null && detailS.equals("true");

  ZKPrinter printer = new ZKPrinter(out, core, addr);
  printer.detail = detail;
  String tryAddr = printer.keeperAddr != null ? printer.keeperAddr
      : "localhost:2181";
%>

<form method="GET" action="zookeeper.jsp" accept-charset="UTF-8">
<table>
<tr>
  <td>
     <strong>   <%
     XML.escapeCharData(printer.zkClient == null ? "Disconnected"
         : ("Connected to zookeeper " + printer.keeperAddr), out);
   %>  </strong>
  </td>
  <td>
        Connect to different zookeeper:
	<input class="std" name="addr" type="text" value="<%XML.escapeCharData(tryAddr, out);%>">
  </td>
    <td>
	<input class="stdbutton" type="submit" value="CONNECT">
  </td>
</tr>
<tr>
</table>
</form>


<%
  try {
    printer.print(path);
  } finally {
    printer.close();
  }
%>

</body>
</html>

<%!static class ZKPrinter {
    static boolean FULLPATH_DEFAULT = false;

    boolean indent = true;
    boolean fullpath = FULLPATH_DEFAULT;

    boolean detail = false;

    String addr; // the address passed to us

    String keeperAddr; // the address we're connected to

    SolrZkClient zkClient;
    boolean doClose;  // close the client after done if we opened it

    JspWriter out;

    int level;

    int maxData = 100;

    private boolean levelchange;

    public ZKPrinter(JspWriter out, SolrCore core, String addr)
        throws IOException {
      this.out = out;
      this.addr = addr;

      if (addr == null) {
        ZkController controller = core.getCoreDescriptor().getCoreContainer().getZkController();
        if (controller != null) {
          // this core is zk enabled
          keeperAddr = controller.getZkServerAddress();
          zkClient = controller.getZkClient();
          if (zkClient != null && zkClient.isConnected()) {
            return;
          } else {
            // try a different client with this address
            addr = keeperAddr;
          }
        }
      }

      keeperAddr = addr;
      if (addr == null) {
        out.println("Zookeeper is not configured for this Solr Core.  Please try connecting to an alternate zookeeper address.");
        return;
      }

      try {
        zkClient = new SolrZkClient(addr, 10000);
        doClose = true;
      } catch (TimeoutException e) {
       out.println("Could not connect to zookeeper at " + addr);
       zkClient = null;
       return;
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        out.println("Could not connect to zookeeper at " + addr);
        zkClient = null;
        return;
      }


    }

    public void close() {
        try {
          if (doClose) zkClient.close();
        } catch (InterruptedException e) {
            // ignore exception on close
        }
    }

    // main entry point
    void print(String path) throws IOException {
      if (zkClient == null)
        return;

      out.print("<table>");
      out.print("<tr><td>");
      out.print("[");
      url("ROOT", "/", false);
      out.print("]");

      // normalize path
      if (path == null)
        path = "/";
      else {
        path.trim();
        if (path.length() == 0)
          path = "/";
      }
      if (path.endsWith("/") && path.length() > 1) {
        path = path.substring(0, path.length() - 1);
      }

      int idx = path.lastIndexOf('/');
      String parent = idx >= 0 ? path.substring(0, idx) : path;
      if (parent.length() == 0)
        parent = "/";

      out.print(" [");
      url("PARENT", parent, detail);
      out.print("]");
      out.print("</td></tr>");

      if (detail) {
        out.print("<tr><td>");
        printZnode(path);
        out.print("</td></tr>");
      }

      out.print("<tr><td>");
      printTree(path);
      out.print("</td></tr>");

      out.print("</table>");
    }

    void exception(Exception e) {
      try {
        out.println(e.toString());
      } catch (IOException e1) {
        // nothing we can do
      }
    }

    void xmlescape(String s) {
      try {
        XML.escapeCharData(s, out);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    void up() throws IOException {
      level++;
      levelchange = true;
    }

    void down() throws IOException {
      level--;
      levelchange = true;
    }

    void indent() throws IOException {
      // if we are using blockquote and just changed indent levels, don't output a break
      // if (fullpath || !levelchange)
      out.println("<br>");
      levelchange = false;

      for (int i=0; i<level; i++)
        out.println("&nbsp;&nbsp;&nbsp;&nbsp;");

      // if fullpath, no indent is needed
      // if not, we are currently using blockquote which the browser
      // will take care of indenting.
    }

    // collapse all whitespace to a single space or escaped newline
    String compress(String str) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < str.length(); i++) {
        char ch = str.charAt(i);
        boolean whitespace = false;
        boolean newline = false;
        while (Character.isWhitespace(ch)) {
          whitespace = true;
          if (ch == '\n')
            newline = true;
          if (++i >= str.length())
            return sb.toString();
          ch = str.charAt(i);
        }

        if (newline) {
          // sb.append("\\n");
          sb.append("  ");  // collapse newline to two spaces
        } else if (whitespace) {
          sb.append(' ');
        }

        // TODO: handle non-printable chars
        sb.append(ch);

        if (sb.length() >= maxData)
          return sb.toString() + "...";
      }
      return sb.toString();
    }

    void url(String label, String path, boolean detail) throws IOException {
      try {
        out.print("<a href=\"zookeeper.jsp?");
        if (path != null) {
          out.print("path=");
          out.print(URLEncoder.encode(path, "UTF-8"));
        }
        if (detail) {
          out.print("&detail=" + detail);
        }
        if (fullpath != FULLPATH_DEFAULT) {
          out.print("&fullpath=" + fullpath);
        }
        if (addr != null) {
          out.print("&addr=");
          out.print(URLEncoder.encode(addr, "UTF-8"));
        }

        out.print("\">");
        xmlescape(label);
        out.print("</a>");

      } catch (UnsupportedEncodingException e) {
        exception(e);
      }
    }

    void printTree(String path) throws IOException {

      indent();

      // TODO: make a link from the path

      String label = path;
      if (!fullpath) {
        int idx = path.lastIndexOf('/');
        label = idx > 0 ? path.substring(idx + 1) : path;
      }

      url(label, path, true);

      out.print(" (");

      Stat stat = new Stat();
      try {
        byte[] data = zkClient.getData(path, null, stat);

        if (stat.getEphemeralOwner() != 0)
          out.print("ephemeral ");
        out.print("v=" + stat.getVersion());
        if (stat.getNumChildren() != 0) {
          out.print(" children=" + stat.getNumChildren());
        }
        out.print(")");

        if (data != null) {

          String str;
          try {
            str = new String(data, "UTF-8");
            out.print(" \"");
            xmlescape(compress(str));
            out.print("\"");
          } catch (UnsupportedEncodingException e) {
            // not UTF8
            StringBuilder sb = new StringBuilder("BIN(");
            sb.append("len=" + data.length);
            sb.append("hex=");
            int limit = Math.min(data.length, maxData / 2);
            for (int i = 0; i < limit; i++) {
              byte b = data[i];
              sb.append(StrUtils.HEX_DIGITS[(b >> 4) & 0xf]);
              sb.append(StrUtils.HEX_DIGITS[b & 0xf]);
            }
            if (limit != data.length)
              sb.append("...");
            sb.append(")");
            str = sb.toString();
            out.print(str);
          }

        }

      } catch (IllegalArgumentException e) {
        // path doesn't exist (must have been removed)
        out.println("(path gone)");
      } catch (KeeperException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      if (stat.getNumChildren() <= 0)
        return;

      List<String> children = null;
      try {
        children = zkClient.getChildren(path, null);
      } catch (KeeperException e) {
        exception(e);
        return;
      } catch (InterruptedException e) {
        exception(e);
      } catch (IllegalArgumentException e) {
        // path doesn't exist (must have been removed)
        out.println("(children gone)");
      }

      up();
      for (String child : children) {
        String childPath = path + (path.endsWith("/") ? "" : "/") + child;
        printTree(childPath);
      }
      down();
    }

    String time(long ms) {
      return (new Date(ms)).toString() + " (" + ms + ")";
    }

    void printZnode(String path) throws IOException {
      try {

        Stat stat = new Stat();
        byte[] data = zkClient.getData(path, null, stat);

        out.print("<h2>");
        xmlescape(path);
        out.print("</h2>");

        up();
        indent();
        out.print("version = " + stat.getVersion());
        indent();
        out.print("aversion = " + stat.getAversion());
        indent();
        out.print("cversion = " + stat.getCversion());
        indent();
        out.print("ctime = " + time(stat.getCtime()));
        indent();
        out.print("mtime = " + time(stat.getMtime()));
        indent();
        out.print("czxid = " + stat.getCzxid());
        indent();
        out.print("mzxid = " + stat.getMzxid());
        indent();
        out.print("pzxid = " + stat.getPzxid());
        indent();
        out.print("numChildren = " + stat.getNumChildren());
        indent();
        out.print("ephemeralOwner = " + stat.getEphemeralOwner());
        indent();
        out.print("dataLength = " + stat.getDataLength());

        if (data != null) {
          boolean isBinary = false;
          String str;
          try {
            str = new String(data, "UTF-8");
          } catch (UnsupportedEncodingException e) {
            // The results are unspecified
            // when the bytes are not properly encoded.

            // not UTF8
            StringBuilder sb = new StringBuilder(data.length * 2);
            for (int i = 0; i < data.length; i++) {
              byte b = data[i];
              sb.append(StrUtils.HEX_DIGITS[(b >> 4) & 0xf]);
              sb.append(StrUtils.HEX_DIGITS[b & 0xf]);
              if ((i & 0x3f) == 0x3f)
                sb.append("\n");
            }
            str = sb.toString();
          }

          int nLines = 1;
          int lineLen = 0;
          int maxLineLen = 10; // the minimum
          for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == '\n') {
              nLines++;
              maxLineLen = Math.max(maxLineLen, lineLen);
              lineLen = 0;
            } else {
              lineLen++;
            }
          }

          indent();
          out.println("<form method='post' action=''>");
          out.println("<textarea class='big' wrap='off' readonly rows='"
              + Math.min(20, nLines)
              //                  + "' cols='" + Math.min(80, maxLineLen+1)
              //                  + "' cols='" + (maxLineLen+1)
              + "' name='data'>");

          xmlescape(str);

          out.println("</textarea></form>");
        }

        down();

      } catch (KeeperException e) {
        exception(e);
        return;
      } catch (InterruptedException e) {
        exception(e);
      }
    }
  }%>
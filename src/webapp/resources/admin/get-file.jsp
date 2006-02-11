<%@ page import="org.apache.solr.core.Config,
                 org.apache.solr.core.SolrConfig,
                 java.io.FileInputStream,
                 java.io.InputStream,
                 java.io.InputStreamReader,
                 java.io.Reader,
                 java.util.StringTokenizer"%>
<%@ page contentType="text/plain;charset=UTF-8" language="java" %>
<%
  String fname = request.getParameter("file");
  String gettableFiles = SolrConfig.config.get("admin/gettableFiles","");
  StringTokenizer st = new StringTokenizer(gettableFiles);
  InputStream is;
  boolean isValid = false;
  if (fname != null) {
    // Validate fname
    while(st.hasMoreTokens()) {
      if (st.nextToken().compareTo(fname) == 0) isValid = true;
    }
  }
  if (isValid) {
    is= Config.openResource(fname);
    Reader input = new InputStreamReader(is);
    char[] buf = new char[4096];
    while (true) {
      int len = input.read(buf);
      if (len<=0) break;
      out.write(buf,0,len);
    }
  } else {
    out.println("<ERROR>");
    out.println("Permission denied for file "+ fname);
    out.println("</ERROR>");
  }
%>

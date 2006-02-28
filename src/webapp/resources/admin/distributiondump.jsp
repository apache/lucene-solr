<%@ page import="org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.IndexSchema,
                 java.io.BufferedReader,
                 java.io.File,
                 java.io.FileReader,
                 java.net.InetAddress,
                 java.net.UnknownHostException,
                 java.util.Date"%>

<%@include file="header.jsp" %>

<%
  File slaveinfo = new File(cwd + "/logs/snappuller.status");

  StringBuffer buffer = new StringBuffer();
  String mode = "";

  if (slaveinfo.canRead()) {
    // Slave instance
    mode = "Slave";
    File slavevers = new File(cwd + "/logs/snapshot.current");
    BufferedReader inforeader = new BufferedReader(new FileReader(slaveinfo));
    BufferedReader versreader = new BufferedReader(new FileReader(slavevers));
    buffer.append("<tr>\n" +
                    "<td>\n" +
                      "Version:" +
                    "</td>\n" +
                    "<td>\n")
          .append(    versreader.readLine())
          .append(  "<td>\n" +
                    "</td>\n" +
                  "</tr>\n" +
                  "<tr>\n" +
                    "<td>\n" +
                      "Status:" +
                    "</td>\n" +
                    "<td>\n")
          .append(    inforeader.readLine())
          .append(  "</td>\n" +
                  "</tr>\n");
  } else {
    // Master instance
    mode = "Master";
    File masterdir = new File(cwd + "/logs/clients");
    File[] clients = masterdir.listFiles();
    if (clients == null) {
      buffer.append("<tr>\n" +
                      "<td>\n" +
                      "</td>\n" +
                      "<td>\n" +
                        "No distribution info present" +
                      "</td>\n" +
                    "</tr>\n");
    } else {
      int i = 0;
      while (i < clients.length) {
        BufferedReader reader = new BufferedReader(new FileReader(clients[i]));
        buffer.append("<tr>\n" +
                        "<td>\n" +
                        "Client:" +
                        "</td>\n" +
                        "<td>\n")
              .append(    clients[i].toString())
              .append(  "</td>\n" +
                      "</tr>\n" +
                      "<tr>\n" +
                        "<td>\n" +
                        "</td>\n" +
                        "<td>\n")
              .append(    reader.readLine())
              .append(  "</td>\n" +
                      "</tr>\n" +
                      "<tr>\n" +
                      "</tr>\n");
        i++;
      }
    }
  }
%>


</body>
</html>

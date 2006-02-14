<%@ page import="org.apache.lucene.analysis.Analyzer,
                 org.apache.lucene.analysis.Token,
                 org.apache.lucene.analysis.TokenStream,
                 org.apache.solr.analysis.TokenFilterFactory,
                 org.apache.solr.analysis.TokenizerChain,
                 org.apache.solr.analysis.TokenizerFactory,
                 org.apache.solr.core.SolrConfig,
                 org.apache.solr.core.SolrCore,
                 org.apache.solr.schema.FieldType,
                 org.apache.solr.schema.IndexSchema,org.apache.solr.schema.SchemaField
                "%>
<%@ page import="org.apache.solr.util.XML"%>
<%@ page import="javax.servlet.jsp.JspWriter"%>
<%@ page import="java.io.File"%>
<%@ page import="java.io.IOException"%>
<%@ page import="java.io.Reader"%>
<%@ page import="java.io.StringReader"%>
<%@ page import="java.net.InetAddress"%>
<%@ page import="java.net.UnknownHostException"%>
<%@ page import="java.util.*"%>
<!-- $Id: analysis.jsp,v 1.2 2005/09/20 18:23:30 yonik Exp $ -->
<!-- $Source: /cvs/main/searching/org.apache.solrolarServer/resources/admin/analysis.jsp,v $ -->
<!-- $Name:  $ -->

<%
  SolrCore core = SolrCore.getSolrCore();
  IndexSchema schema = core.getSchema();

  String rootdir = "/var/opt/resin3/"+request.getServerPort();
  File pidFile = new File(rootdir + "/logs/resin.pid");
  File enableFile = new File(rootdir + "/logs/server-enabled");
  boolean isEnabled = false;
  String enabledStatus = "";
  String enableActionStatus = "";
  String makeEnabled = "";
  String action = request.getParameter("action");
  String startTime = "";

  try {
    startTime = (pidFile.lastModified() > 0)
      ? new Date(pidFile.lastModified()).toString()
      : "No Resin Pid found (logs/resin.pid)";
  } catch (Exception e) {
    out.println("<ERROR>");
    out.println("Couldn't open Solr pid file:" + e.toString());
    out.println("</ERROR>");
  }


  try {
    isEnabled = (enableFile.lastModified() > 0);
    enabledStatus = (isEnabled)
      ? "Enabled"
      : "Disabled";
    makeEnabled = (isEnabled)
      ? "Disable"
      : "Enable";
  } catch (Exception e) {
    out.println("<ERROR>");
    out.println("Couldn't check server-enabled file:" + e.toString());
    out.println("</ERROR>");
  }

  String collectionName = schema!=null ? schema.getName():"unknown";
  String hostname="localhost";
  String defaultSearch= SolrConfig.config.get("admin/defaultQuery","");
  try {
    InetAddress addr = InetAddress.getLocalHost();
    // Get IP Address
    byte[] ipAddr = addr.getAddress();
    // Get hostname
    // hostname = addr.getHostName();
    hostname = addr.getCanonicalHostName();
  } catch (UnknownHostException e) {}
%>

<%
  String name = request.getParameter("name");
  if (name==null || name.length()==0) name="";
  String val = request.getParameter("val");
  if (val==null || val.length()==0) val="";
  String qval = request.getParameter("qval");
  if (qval==null || qval.length()==0) qval="";
  String verboseS = request.getParameter("verbose");
  boolean verbose = verboseS!=null && verboseS.equalsIgnoreCase("on");
  String qverboseS = request.getParameter("qverbose");
  boolean qverbose = qverboseS!=null && qverboseS.equalsIgnoreCase("on");
  String highlightS = request.getParameter("highlight");
  boolean highlight = highlightS!=null && highlightS.equalsIgnoreCase("on");
%>


<html>
<head>
<link rel="stylesheet" type="text/css" href="solr-admin.css">
<link rel="icon" href="favicon.ico" type="image/ico">
<link rel="shortcut icon" href="favicon.ico" type="image/ico">
<title>SOLR Interface</title>
</head>

<body>
<a href=""><img border="0" align="right" height="88" width="215" src="solr-head.gif" alt="SOLR"></a>
<h1>SOLR Interface (<%= collectionName %>) - <%= enabledStatus %></h1>
<%= hostname %> : <%= request.getServerPort() %>
<br clear="all">


<h2>Field Analysis</h2>

<form method="GET" action="analysis.jsp">
<table>
<tr>
  <td>
	<strong>Field name</strong>
  </td>
  <td>
	<input name="name" type="text" value="<%= name %>">
  </td>
</tr>
<tr>
  <td>
	<strong>Field value (Index)</strong>
  <br/>
  verbose output
  <input name="verbose" type="checkbox"
     <%= verbose ? "checked=\"true\"" : "" %> >
    <br/>
  highlight matches
  <input name="highlight" type="checkbox"
     <%= highlight ? "checked=\"true\"" : "" %> >
  </td>
  <td>
	<textarea rows="3" cols="70" name="val"><%= val %></textarea>
  </td>
</tr>
<tr>
  <td>
	<strong>Field value (Query)</strong>
  <br/>
  verbose output
  <input name="qverbose" type="checkbox"
     <%= qverbose ? "checked=\"true\"" : "" %> >
  </td>
  <td>
	<textarea rows="1" cols="70" name="qval"><%= qval %></textarea>
  </td>
</tr>
<tr>

  <td>
  </td>

  <td>
	<input type="submit" value="analyze">
  </td>

</tr>
</table>
</form>


<%
  SchemaField field=null;

  if (name!="") {
    try {
      field = schema.getField(name);
    } catch (Exception e) {
      out.println("<strong>Unknown Field " + name + "</strong>");
    }
  }

  if (field!=null) {
    HashSet<Tok> matches = null;
    if (qval!="" && highlight) {
      Reader reader = new StringReader(qval);
      Analyzer analyzer =  field.getType().getQueryAnalyzer();
      TokenStream tstream = analyzer.tokenStream(field.getName(),reader);
      List<Token> tokens = getTokens(tstream);
      matches = new HashSet<Tok>();
      for (Token t : tokens) { matches.add( new Tok(t,0)); }
    }

    if (val!="") {
      out.println("<h3>Index Analyzer</h3>");
      doAnalyzer(out, field, val, false, verbose,matches);
    }
    if (qval!="") {
      out.println("<h3>Query Analyzer</h3>");
      doAnalyzer(out, field, qval, true, qverbose,null);
    }
  }

%>


</body>
</html>


<%!
  private static void doAnalyzer(JspWriter out, SchemaField field, String val, boolean queryAnalyser, boolean verbose, Set<Tok> match) throws Exception {
    Reader reader = new StringReader(val);

    FieldType ft = field.getType();
     Analyzer analyzer = queryAnalyser ?
             ft.getQueryAnalyzer() : ft.getAnalyzer();
     if (analyzer instanceof TokenizerChain) {
       TokenizerChain tchain = (TokenizerChain)analyzer;
       TokenizerFactory tfac = tchain.getTokenizerFactory();
       TokenFilterFactory[] filtfacs = tchain.getTokenFilterFactories();

       TokenStream tstream = tfac.create(reader);
       List<Token> tokens = getTokens(tstream);
       tstream = tfac.create(reader);
       if (verbose) {
         writeHeader(out, tfac.getClass(), tfac.getArgs());
       }

       writeTokens(out, tokens, ft, verbose, match);

       for (TokenFilterFactory filtfac : filtfacs) {
         if (verbose) {
           writeHeader(out, filtfac.getClass(), filtfac.getArgs());
         }

         final Iterator<Token> iter = tokens.iterator();
         tstream = filtfac.create( new TokenStream() {
           public Token next() throws IOException {
             return iter.hasNext() ? iter.next() : null;
           }
          }
         );
         tokens = getTokens(tstream);

         writeTokens(out, tokens, ft, verbose, match);
       }

     } else {
       TokenStream tstream = analyzer.tokenStream(field.getName(),reader);
       List<Token> tokens = getTokens(tstream);
       if (verbose) {
         writeHeader(out, analyzer.getClass(), new HashMap<String,String>());
       }
       writeTokens(out, tokens, ft, verbose, match);
     }
  }


  static List<Token> getTokens(TokenStream tstream) throws IOException {
    List<Token> tokens = new ArrayList<Token>();
    while (true) {
      Token t = tstream.next();
      if (t==null) break;
      tokens.add(t);
    }
    return tokens;
  }


  private static class Tok {
    Token token;
    int pos;
    Tok(Token token, int pos) {
      this.token=token;
      this.pos=pos;
    }

    public boolean equals(Object o) {
      return ((Tok)o).token.termText().equals(token.termText());
    }
    public int hashCode() {
      return token.termText().hashCode();
    }
    public String toString() {
      return token.termText();
    }
  }

  private static interface ToStr {
    public String toStr(Object o);
  }

  private static void printRow(JspWriter out, String header, List[] arrLst, ToStr converter, boolean multival, boolean verbose, Set<Tok> match) throws IOException {
    // find the maximum number of terms for any position
    int maxSz=1;
    if (multival) {
      for (List lst : arrLst) {
        maxSz = Math.max(lst.size(), maxSz);
      }
    }


    for (int idx=0; idx<maxSz; idx++) {
      out.println("<tr>");
      if (idx==0 && verbose) {
        if (header != null) {
          out.print("<th NOWRAP rowspan=\""+maxSz+"\">");
          XML.escapeCharData(header,out);
          out.println("</th>");
        }
      }

      for (List<Tok> lst : arrLst) {
        if (lst.size() <= idx) continue;
        if (match!=null && match.contains(lst.get(idx))) {
          out.print("<td name=\"highlight\"");
        } else {
          out.print("<td name=\"debugdata\"");
        }

        if (idx==0 && lst.size()==1 && maxSz > 1) {
          out.print("rowspan=\""+maxSz+'"');
        }

        out.print('>');

        XML.escapeCharData(converter.toStr(lst.get(idx)), out);
        out.print("</td>");
      }

      out.println("</tr>");
    }

  }



  static void writeHeader(JspWriter out, Class clazz, Map<String,String> args) throws IOException {
    out.print("<h4>");
    out.print(clazz.getName());
    XML.escapeCharData("   "+args,out);
    out.println("</h4>");
  }



  // readable, raw, pos, type, start/end
  static void writeTokens(JspWriter out, List<Token> tokens, final FieldType ft, boolean verbose, Set<Tok> match) throws IOException {

    // Use a map to tell what tokens are in what positions
    // because some tokenizers/filters may do funky stuff with
    // very large increments, or negative increments.
    HashMap<Integer,List<Tok>> map = new HashMap<Integer,List<Tok>>();
    boolean needRaw=false;
    int pos=0;
    for (Token t : tokens) {
      if (!t.termText().equals(ft.indexedToReadable(t.termText()))) {
        needRaw=true;
      }

      pos += t.getPositionIncrement();
      List lst = map.get(pos);
      if (lst==null) {
        lst = new ArrayList(1);
        map.put(pos,lst);
      }
      Tok tok = new Tok(t,pos);
      lst.add(tok);
    }

    List<Tok>[] arr = (List<Tok>[])map.values().toArray(new ArrayList[map.size()]);

    /***
    // This generics version works fine with Resin, but fails with Tomcat 5.5
    // with java.lang.AbstractMethodError
    //    at java.util.Arrays.mergeSort(Arrays.java:1284)
    //    at java.util.Arrays.sort(Arrays.java:1223) 
    Arrays.sort(arr, new Comparator<List<Tok>>() {
      public int compare(List<Tok> toks, List<Tok> toks1) {
        return toks.get(0).pos - toks1.get(0).pos;
      }
    }
    ***/
    Arrays.sort(arr, new Comparator() {
      public int compare(Object a, Object b) {
        List<Tok> toks = (List<Tok>)a;
        List<Tok> toks1 = (List<Tok>)b;
        return toks.get(0).pos - toks1.get(0).pos;
      }
    }

    );

    out.println("<table width=\"auto\" name=\"table\" border=\"1\">");

    if (verbose) {
      printRow(out,"term position", arr, new ToStr() {
        public String toStr(Object o) {
          return Integer.toString(((Tok)o).pos);
        }
      }
              ,false
              ,verbose
              ,null);
    }


    printRow(out,"term text", arr, new ToStr() {
      public String toStr(Object o) {
        return ft.indexedToReadable( ((Tok)o).token.termText() );
      }
    }
            ,true
            ,verbose
            ,match
   );

    if (needRaw) {
      printRow(out,"raw text", arr, new ToStr() {
        public String toStr(Object o) {
          // todo: output in hex or something?
          // check if it's all ascii or not?
          return ((Tok)o).token.termText();
        }
      }
              ,true
              ,verbose
              ,match
      );
    }

    if (verbose) {
      printRow(out,"term type", arr, new ToStr() {
        public String toStr(Object o) {
          return  ((Tok)o).token.type();
        }
      }
              ,true
              ,verbose,
              null
      );
    }

    if (verbose) {
      printRow(out,"source start,end", arr, new ToStr() {
        public String toStr(Object o) {
          Token t = ((Tok)o).token;
          return Integer.toString(t.startOffset()) + ',' + t.endOffset() ;
        }
      }
              ,true
              ,verbose
              ,null
      );
    }

    out.println("</table>");
  }

%>

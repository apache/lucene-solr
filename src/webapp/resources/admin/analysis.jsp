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
<%@ page import="org.apache.lucene.analysis.Analyzer,
                 org.apache.lucene.analysis.Token,
                 org.apache.lucene.analysis.TokenStream,
                 org.apache.solr.analysis.TokenFilterFactory,
                 org.apache.solr.analysis.TokenizerChain,
                 org.apache.solr.analysis.TokenizerFactory,
                 org.apache.solr.schema.FieldType,
                 org.apache.solr.schema.SchemaField,
                 org.apache.solr.util.XML,
                 javax.servlet.jsp.JspWriter,java.io.IOException
                "%>
<%@ page import="java.io.Reader"%>
<%@ page import="java.io.StringReader"%>
<%@ page import="java.util.*"%>

<?xml-stylesheet type="text/xsl" href="analysis.xsl"?>

<%@include file="_info.jsp" %>

<%
  String name = request.getParameter("name");
  if (name == null || name.length() == 0) name = "";
  String val = request.getParameter("val");
  if (val == null || val.length() == 0) val = "";
  String qval = request.getParameter("qval");
  if (qval == null || qval.length() == 0) qval = "";
  String verboseS = request.getParameter("verbose");
  boolean verbose = verboseS != null && verboseS.equalsIgnoreCase("on");
  String qverboseS = request.getParameter("qverbose");
  boolean qverbose = qverboseS != null && qverboseS.equalsIgnoreCase("on");
  String highlightS = request.getParameter("highlight");
  boolean highlight = highlightS != null && highlightS.equalsIgnoreCase("on");
%>

<solr>
<%@include file="heading.jsp" %>
  <analysis>

<%
  SchemaField field = null;

  if (name != "") {
    try {
      field = schema.getField(name);
    } catch (Exception e) {
      out.println("<error>Unknown Field " + name + "</error>");
    }
  }

  if (field != null) {
    out.println("    <form>");
    out.println("      <field>");
      XML.escapeCharData(name, out);
    out.println("</field>");
    out.print("      <fieldIndexValue>");
      XML.escapeCharData(val, out);
    out.println("      </fieldIndexValue>");
    out.print("      <fieldQueryValue>");
      XML.escapeCharData(qval, out);
    out.println("      </fieldQueryValue>");
    out.println("    </form>");

    HashSet<Tok> matches = null;
    if (qval != "" && highlight) {
      Reader reader = new StringReader(qval);
      Analyzer analyzer =  field.getType().getQueryAnalyzer();
      TokenStream tstream = analyzer.tokenStream(field.getName(), reader);
      List<Token> tokens = getTokens(tstream);
      matches = new HashSet<Tok>();
      for (Token t : tokens) { matches.add( new Tok(t, 0)); }
    }

    out.println("    <results>");
    if (val != "") {
      out.println("<indexAnalyzer>");
      doAnalyzer(out, field, val, false, verbose, matches);
      out.println("</indexAnalyzer>");
    }
    if (qval != "") {
      out.println("<queryAnalyzer>");
      doAnalyzer(out, field, qval, true, qverbose, null);
      out.println("</queryAnalyzer>");
    }
    out.println("    </results>");
  }
%>
  </analysis>
</solr>

<%!
  private static void doAnalyzer(JspWriter out, SchemaField field, String val, boolean queryAnalyser, boolean verbose, Set<Tok> match) throws Exception {
    Reader reader = new StringReader(val);

    FieldType ft = field.getType();
    Analyzer analyzer = queryAnalyser ? ft.getQueryAnalyzer() : ft.getAnalyzer();
     if (analyzer instanceof TokenizerChain) {
       TokenizerChain tchain = (TokenizerChain)analyzer;
       TokenizerFactory tfac = tchain.getTokenizerFactory();
       TokenFilterFactory[] filtfacs = tchain.getTokenFilterFactories();

       TokenStream tstream = tfac.create(reader);
       List<Token> tokens = getTokens(tstream);
       tstream = tfac.create(reader);
       // write tokenizer factories
       if (verbose) {
         writeHeader(out, tfac.getClass(), tfac.getArgs());
       }

       writeTokens(out, tokens, ft, verbose, match);

       // write filter factories
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
      if (t == null) break;
      tokens.add(t);
    }
    return tokens;
  }


  private static class Tok {
    Token token;
    int pos;
    Tok(Token token, int pos) {
      this.token = token;
      this.pos = pos;
    }

    public boolean equals(Object o) {
      return ((Tok)o).token.termText().equals(token.termText());
    }
    public int hashCode() {
      return token.termText().hashCode();
    }
    public String toString() {
      return token.termText() + " at position " + pos;
    }
  }

  private static interface ToStr {
    public String toStr(Object o);
  }

  static void writeHeader(JspWriter out, Class clazz, Map<String,String> args) throws IOException {
    out.println("  <factory class=\"" + clazz.getName() + "\">");
    out.println("    <args>");
    for (Iterator<String> iter = args.keySet().iterator(); iter.hasNext(); ) {
      String key = iter.next();
      String value = args.get(key);
      out.println("      <arg name=\"" + key + "\">" + value + "</arg>");
    }
    out.println("    </args>");
  }

  // readable, raw, pos, type, start/end
  static void writeTokens(JspWriter out, List<Token> tokens, final FieldType ft, boolean verbose, Set<Tok> match) throws IOException {

    // Use a map to tell what tokens are in what positions
    // because some tokenizers/filters may do funky stuff with
    // very large increments, or negative increments.
    HashMap<Integer, List<Tok>> map = new HashMap<Integer, List<Tok>>();
    boolean needRaw = false;
    int pos = 0;
    for (Token t : tokens) {
      if (!t.termText().equals(ft.indexedToReadable(t.termText()))) {
        needRaw = true;
      }

      pos += t.getPositionIncrement();
      List lst = map.get(pos);
      if (lst == null) {
        lst = new ArrayList(1);
        map.put(pos, lst);
      }
      Tok tok = new Tok(t, pos);
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

   out.println("    <tokens>");
   for (int i = 0; i < arr.length; i++) {
     for (Tok tok : arr[i]) {
       out.print("      <token");
       out.print(" type=\"" + tok.token.type() + "\"");
       out.print(" pos=\"" + tok.pos + "\"");
       out.print(" start=\"" + tok.token.startOffset() + "\"");
       out.print(" end=\"" + tok.token.endOffset() + "\"");
       out.print(">");
       out.print(tok.token.termText());
       out.println("      </token>");
     }
   }
   out.println("    </tokens>");
   out.println("  </factory>");
  }

%>

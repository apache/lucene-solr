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

import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.request.*;


import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathConstants;
import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.Handler;
import java.util.logging.ConsoleHandler;

import org.w3c.dom.Document;


/**
 * User: Yonik Seeley
 * Date: Aug 16, 2004
 */
public class SolrTest extends Thread {
  static SolrCore core;

  static String[] requestDict;
  static String[] updateDict;
  static String[] testDict;
  static List<Integer> testDictLineno;

  static List<Integer> lineno;
  public static String[] readDict(String filename) throws IOException {
      BufferedReader br = new BufferedReader(new FileReader(filename));
      ArrayList lst = new ArrayList(1024);
    lineno = new ArrayList<Integer>(1024);
    String line;
    int lineNum=0;
    while ((line = br.readLine())!=null) {
      lineNum++;
      if (line.length() <= 1) continue;
      lst.add(line);
      lineno.add(lineNum);
    }
    br.close();
    return (String[]) lst.toArray(new String[lst.size()]);
  }


  public static boolean verbose=false;
  static boolean doValidate=true;

  static int countdown;
  static synchronized boolean runAgain() {
    return countdown-- > 0;
  }


  // statistics per client
  int numReq=0;
  int numErr=0;
  int numBodyChars=0;

  boolean isWriter=false;
  boolean sequenceTest=false;

  public void run() {

    if (sequenceTest) {
      try {
      for (int i=0; i<testDict.length; i++) {
        String s = testDict[i];
        int lineno = testDictLineno.get(i);
        String req;
        String test=null;
        String params=null;
        char[] resp;
        if (s.length()<2 || s.startsWith("#")) continue;  // comment
        System.out.println("LINE=" + lineno + " EXECUTING " + s);

        int endQuery = s.length();
        int startParams = s.indexOf("%%");
        int endParams = s.length();
        int endTests = s.length();
        if (startParams > 0) {
          endQuery = startParams;
          endParams = s.length();
        }
        int startTests = s.indexOf('%', startParams+2);
        if (startTests > 0) {
          if (endQuery == s.length()) endQuery = startTests;
          endParams = startTests;
        }

        req = s.substring(0,endQuery).trim();
        if (startParams > 0) params = s.substring(startParams+2,endParams).trim();
        if (startTests > 0) test = s.substring(startTests+1,endTests).trim();

        // System.out.println("###req=" + req);
        // System.out.println("###params=" + params);
        // System.out.println("###tests=" + test);

        if (req.startsWith("<")) {
          resp = doUpdate(req);
        } else {
          resp = doReq(req,params);
        }
        if (doValidate) {
          validate(req,test,resp);
        } else {
          System.out.println("#### no validation performed");
        }
      }
      } catch (RuntimeException e) {
        numErr++;
        throw(e);
      }

      System.out.println(">>>>>>>>>>>>>>>>>>>>>>>> SUCCESS <<<<<<<<<<<<<<<<<<<<<<<<<<");
    }

    else {
      while(runAgain()) {
        if (isWriter) doUpdate(updateDict[(int)(Math.random()*updateDict.length)]);
        else doReq(requestDict[(int)(Math.random()*requestDict.length)], null);
      }
    }
  }

  private DocumentBuilder builder;
  private XPath xpath = XPathFactory.newInstance().newXPath();
  {
    try {
      builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      e.printStackTrace();
    }
  }

  private void validate(String req, String test, char[] resp) {
    if (test==null || test.length()==0) return;
    Document document=null;
    try {
      // the resp[] contains a declaration that it is UTF-8, so we
      // need to change it to that for the XML parser.

      document = builder.parse(new ByteArrayInputStream(new String(resp).getBytes("UTF-8")));
      // document = builder.parse(new String(resp));
    } catch (Exception e) {
      System.out.println("ERROR parsing '" + new String(resp) + "'");
      throw new RuntimeException(e);
    }

      String[] tests = test.split("%");
      for (String xp : tests) {
        Boolean bool=false;
        xp=xp.trim();
        try {
           bool = (Boolean) xpath.evaluate(xp, document, XPathConstants.BOOLEAN);
        } catch (Exception e) {
          System.out.println("##################ERROR EVALUATING XPATH '" + xp + "'");
          throw new RuntimeException(e);
        }
        if (!bool) {
          System.out.println("##################ERROR");
          System.out.println("req="+req);
          System.out.println("xp="+xp);
          throw new RuntimeException("test failed.");
        }
      }

  }


  public char[] doUpdate(String req) {
    try {
      // String lucene=updateDict[(int)(Math.random()*updateDict.length)];
      String lucene=req;
      StringReader ureq = new StringReader(lucene);
      CharArrayWriter writer = new CharArrayWriter(32000);
      core.update(ureq, writer);
      if (verbose) System.out.println("UPDATE RESPONSE:'" + writer + "'");
      // if (verbose) System.out.println("BODY chars read:" + writer.size());
      this.numBodyChars+=writer.size();
      this.numReq++;
      return writer.toCharArray();
    } catch (Exception e) {
      this.numErr++;
      e.printStackTrace();
    }
    return null;
  }


  static XMLResponseWriter xmlwriter = new XMLResponseWriter();
  static SolrRequestHandler handler =
           // new OldRequestHandler();
              new StandardRequestHandler();
  static String qargs = null; // default query arguments

  public char[] doReq(String req, String params)  {
    int start=0;
    int limit=10;
    String handler="standard";
    //handler="test";


    Map args = new HashMap();
    args.put("indent", "on");
    args.put("debugQuery", "on");
    args.put("version", "2.0");


    if (qargs != null) {
      if (params==null) params=qargs;
      else params = qargs + '&' + params;
    }

    if (params != null) {
      String[] plist = params.split("&");
      for (String decl : plist) {
        String[] nv = decl.split("=");
        if (nv.length==1) {
          nv = new String[] { nv[0], "" };
        }
        if (nv[0].equals("start")) {
          start=Integer.parseInt(nv[1]);
        }
        else if (nv[0].equals("limit")) {
          limit=Integer.parseInt(nv[1]);
        }
        else if (nv[0].equals("qt")) {
          handler = nv[1];
        } else {
          args.put(nv[0], nv[1]);
        }
      }
    }

    try {
      // String lucene=requestDict[(int)(Math.random()*requestDict.length)];
      String lucene=req;
      CharArrayWriter writer = new CharArrayWriter(32000);

      System.out.println("start="+start+" limit="+limit+" handler="+handler);
      LocalSolrQueryRequest qreq = new LocalSolrQueryRequest(core,lucene,handler,start,limit,args);
      SolrQueryResponse qrsp = new SolrQueryResponse();
      try {
        core.execute(qreq,qrsp);
        if (qrsp.getException() != null) throw qrsp.getException();
        // handler.handleRequest(qreq,qrsp);
        xmlwriter.write(writer,qreq,qrsp);
      } finally {
        qreq.close();
      }
      if (verbose) System.out.println("GOT:'" + writer + "'");
      if (verbose) System.out.println("BODY chars read:" + writer.size());
      this.numBodyChars+=writer.size();
      this.numReq++;
      return writer.toCharArray();
    } catch (Exception e) {
      this.numErr++;
      e.printStackTrace();
    }
    return null;
  }



  public static void main(String[] args) throws Exception {
    int readers=1;
    int requests=1;
    int writers=0;

    Logger log = Logger.getLogger("org.apache.solr");
    log.setUseParentHandlers(false);
    log.setLevel(Level.FINEST);
    Handler handler = new ConsoleHandler();
    handler.setLevel(Level.FINEST);
    log.addHandler(handler);

    String filename="dict.txt";
    String updateFilename="update_dict.txt";
    String dataDir =null;
    String schemaFile=null;
    String testFile=null;

    boolean b_numUpdates=false; boolean b_writers=false;

    int i=0; String arg;
    while (i < args.length && args[i].startsWith("-")) {
      arg = args[i++];
      if (arg.equals("-verbose")) {
        verbose=true;
      } else if (arg.equals("-dict")) {
        filename=args[i++];
      } else if (arg.equals("-data")) {
        dataDir =args[i++];
      } else if (arg.equals("-readers")) {
        readers=Integer.parseInt(args[i++]);
      } else if (arg.equals("-numRequests")) {
        requests=Integer.parseInt(args[i++]);
      } else if (arg.equals("-writers")) {
        writers=Integer.parseInt(args[i++]);
        b_writers=true;
      } else if (arg.equals("-schema")) {
        schemaFile=args[i++];
      } else if (arg.equals("-test")) {
        testFile=args[i++];
      } else if (arg.equals("-noValidate")) {
        doValidate=false;
      } else if (arg.equals("-qargs")) {
        qargs=args[i++];
      } else {
        System.out.println("Unknown option: " + arg);
        return;
      }
    }

    try {

      IndexSchema schema = schemaFile==null ? null : new IndexSchema(schemaFile);
      countdown = requests;
      core=new SolrCore(dataDir,schema);

      try {
        if (testFile != null) {
          testDict = readDict(testFile);
          testDictLineno = lineno;
        }  else {
          if (readers > 0) requestDict = readDict(filename);
          if (writers > 0) updateDict = readDict(updateFilename);
        }
      } catch (IOException e) {
        e.printStackTrace();
        System.out.println("Can't read "+filename);
        return;
      }

      SolrTest[] clients = new SolrTest[readers+writers];
      for (i=0; i<readers; i++) {
        clients[i] = new SolrTest();
        if (testFile != null) clients[i].sequenceTest=true;
        clients[i].start();
      }
      for (i=readers; i<readers+writers; i++) {
        clients[i] = new SolrTest();
        clients[i].isWriter = true;
        clients[i].start();
      }

      for (i=0; i<readers; i++) {
        clients[i].join();
      }
      for (i=readers; i<readers+writers; i++) {
        clients[i].join();
      }

      core.close();
      core=null;

      if (testFile!=null) {
        if (clients[0].numErr == 0) {
          System.out.println(">>>>>>>>>>>>>>>>>>>>>>>> SUCCESS <<<<<<<<<<<<<<<<<<<<<<<<<<");
        } else {
          System.exit(1);
        }
      }

    } catch (Throwable e) {
      if (core!=null) {try{core.close();} catch (Throwable th){}}
      e.printStackTrace();
      System.exit(1);
    }


  }

}

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

package org.apache.solr.util;

import org.apache.solr.schema.IndexSchema;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.*;

import org.xml.sax.SAXException;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathConstants;
import java.io.*;
import java.util.*;


/**
 * This class provides a simple harness that may be usefull when
 * writing testcasses.
 *
 * <p>
 * This class lives in the main source tree (and not in the test source
 * tree) so that it will be included with even the most minimal solr
 * distribution -- to encourage plugin writers to creat unit tests for their
 * plugins.
 *
 * @author hossman
 * @version $Id:$
 */
public class TestHarness {

  private SolrCore core;
  private XMLResponseWriter xmlwriter = new XMLResponseWriter();
  private XPath xpath = XPathFactory.newInstance().newXPath();
  private DocumentBuilder builder;
        
  /**
   * Assumes "solrconfig.xml" is the config file to use, and
   * "schema.xml" is the schema path to use.
   *
   * @param dataDirectory path for index data, will not be cleaned up
   */
  public TestHarness(String dataDirectory) {
    this(dataDirectory, "schema.xml");
  }
  /**
   * Assumes "solrconfig.xml" is the config file to use.
   *
   * @param dataDirectory path for index data, will not be cleaned up
   * @param schemaFile path of schema file
   */
  public TestHarness(String dataDirectory, String schemaFile) {
    this(dataDirectory, "solrconfig.xml", schemaFile);
  }
  /**
   * @param dataDirectory path for index data, will not be cleaned up
   * @param confFile solrconfig filename
   * @param schemaFile schema filename
   */
  public TestHarness(String dataDirectory,
                     String confFile,
                     String schemaFile) {
    try {
      SolrConfig.initConfig(confFile);
      core = new SolrCore(dataDirectory, new IndexSchema(schemaFile));
      builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

        
  /**
   * Processes an "update" (add, commit or optimize) and
   * returns the response as a String.
   *
   * @param xml The XML of the update
   * @return The XML response to the update
   */
  public String update(String xml) {
                
    StringReader req = new StringReader(xml);
    StringWriter writer = new StringWriter(32000);
    core.update(req, writer);
    return writer.toString();
                
  }

        
  /**
   * Validates that an "update" (add, commit or optimize) results in success.
   *
   * :TODO: currently only deals with one add/doc at a time, this will need changed if/when SOLR-2 is resolved
   * 
   * @param xml The XML of the update
   * @return null if succesful, otherwise the XML response to the update
   */
  public String validateUpdate(String xml) throws SAXException {
    try {
      String res = update(xml);
      String valid = validateXPath(res, "//result[@status=0]" );
      return (null == valid) ? null : res;
    } catch (XPathExpressionException e) {
      throw new RuntimeException
        ("?!? static xpath has bug?", e);
    }
  }

        
  /**
   * Validates that an add of a single document results in success.
   *
   * @param fieldsAndValues Odds are field names, Evens are values
   * @return null if succesful, otherwise the XML response to the update
   * @see appendSimpleDoc
   */
  public String validateAddDoc(String... fieldsAndValues)
    throws XPathExpressionException, SAXException, IOException {

    StringBuffer buf = new StringBuffer();
    buf.append("<add>");
    appendSimpleDoc(buf, fieldsAndValues);
    buf.append("</add>");
        
    String res = update(buf.toString());
    String valid = validateXPath(res, "//result[@status=0]" );
    return (null == valid) ? null : res;
  }


    
  /**
   * Validates a "query" response against an array of XPath test strings
   *
   * @param req the Query to process
   * @return null if all good, otherwise the first test that fails.
   * @exception Exception any exception in the response.
   * @exception IOException if there is a problem writing the XML
   * @see LocalSolrQueryRequest
   */
  public String validateQuery(SolrQueryRequest req, String... tests)
    throws IOException, Exception {
                
    String res = query(req);
    return validateXPath(res, tests);
  }
            
  /**
   * Processes a "query" using a user constructed SolrQueryRequest
   *
   * @param req the Query to process, will be closed.
   * @return The XML response to the query
   * @exception Exception any exception in the response.
   * @exception IOException if there is a problem writing the XML
   * @see LocalSolrQueryRequest
   */
  public String query(SolrQueryRequest req) throws IOException, Exception {

    SolrQueryResponse rsp = new SolrQueryResponse();
    core.execute(req,rsp);
    if (rsp.getException() != null) {
      throw rsp.getException();
    }
                
    StringWriter writer = new StringWriter(32000);
    xmlwriter.write(writer,req,rsp);

    req.close();
    
    return writer.toString();
  }


  /**
   * A helper method which valides a String against an array of XPath test
   * strings.
   *
   * @param xml The xml String to validate
   * @param tests Array of XPath strings to test (in boolean mode) on the xml
   * @return null if all good, otherwise the first test that fails.
   */
  public String validateXPath(String xml, String... tests)
    throws XPathExpressionException, SAXException {
        
    if (tests==null || tests.length == 0) return null;
                
    Document document=null;
    try {
      document = builder.parse(new ByteArrayInputStream
                               (xml.getBytes("UTF-8")));
    } catch (UnsupportedEncodingException e1) {
      throw new RuntimeException("Totally weird UTF-8 exception", e1);
    } catch (IOException e2) {
      throw new RuntimeException("Totally weird io exception", e2);
    }
                
    for (String xp : tests) {
      xp=xp.trim();
      Boolean bool = (Boolean) xpath.evaluate(xp, document,
                                              XPathConstants.BOOLEAN);

      if (!bool) {
        return xp;
      }
    }
    return null;
                
  }

  public SolrCore getCore() {
    return core;
  }
        
  /**
   * Shuts down and frees any resources
   */
  public void close() {
    core.close();
  }

  /**
   * A helper that adds an xml &lt;doc&gt; containing all of the
   * fields and values specified (odds are fields, evens are values)
   * to a StringBuffer.
   */
  public void appendSimpleDoc(StringBuffer buf, String... fieldsAndValues)
    throws IOException {

    buf.append(makeSimpleDoc(fieldsAndValues));
  }
  /**
   * A helper that creates an xml &lt;doc&gt; containing all of the
   * fields and values specified
   *
   * @param fieldsAndValues 0 and Even numbered args are fields names odds are field values.
   */
  public static StringBuffer makeSimpleDoc(String... fieldsAndValues) {

    try {
      StringWriter w = new StringWriter();
      w.append("<doc>");
      for (int i = 0; i < fieldsAndValues.length; i+=2) {
        XML.writeXML(w, "field", fieldsAndValues[i+1], "name",
                     fieldsAndValues[i]);
      }
      w.append("</doc>");
      return w.getBuffer();
    } catch (IOException e) {
      throw new RuntimeException
        ("this should never happen with a StringWriter", e);
    }
  }

  /**
   * Generates a delete by query xml string
   * @param q Query that has not already been xml escaped
   */
  public static String deleteByQuery(String q) {
    return delete("query", q);
  }
  /**
   * Generates a delete by id xml string
   * @param id ID that has not already been xml escaped
   */
  public static String deleteById(String id) {
    return delete("id", id);
  }
        
  /**
   * Generates a delete xml string
   * @param val text that has not already been xml escaped
   */
  private static String delete(String deltype, String val) {
    try {
      StringWriter r = new StringWriter();
            
      r.write("<delete>");
      XML.writeXML(r, deltype, val);
      r.write("</delete>");
            
      return r.getBuffer().toString();
    } catch (IOException e) {
      throw new RuntimeException
        ("this should never happen with a StringWriter", e);
    }
  }
    
  /**
   * Helper that returns an &lt;optimize&gt; String with
   * optional key/val pairs.
   *
   * @param args 0 and Even numbered args are params, Odd numbered args are values.
   */
  public static String optimize(String... args) {
    return simpleTag("optimize", args);
  }

  private static String simpleTag(String tag, String... args) {
    try {
      StringWriter r = new StringWriter();

      // this is anoying
      if (null == args || 0 == args.length) {
        XML.writeXML(r, tag, null);
      } else {
        XML.writeXML(r, tag, null, (Object)args);
      }
      return r.getBuffer().toString();
    } catch (IOException e) {
      throw new RuntimeException
        ("this should never happen with a StringWriter", e);
    }
  }
    
  /**
   * Helper that returns an &lt;commit&gt; String with
   * optional key/val pairs.
   *
   * @param args 0 and Even numbered args are params, Odd numbered args are values.
   */
  public static String commit(String... args) {
    return simpleTag("commit", args);
  }
    
  public LocalRequestFactory getRequestFactory(String qtype,
                                               int start,
                                               int limit) {
    LocalRequestFactory f = new LocalRequestFactory();
    f.qtype = qtype;
    f.start = start;
    f.limit = limit;
    return f;
  }
    
  /**
   * 0 and Even numbered args are keys, Odd numbered args are values.
   */
  public LocalRequestFactory getRequestFactory(String qtype,
                                               int start, int limit,
                                               String... args) {
    LocalRequestFactory f = getRequestFactory(qtype, start, limit);
    for (int i = 0; i < args.length; i+=2) {
      f.args.put(args[i], args[i+1]);
    }
    return f;
        
  }
    
  public LocalRequestFactory getRequestFactory(String qtype,
                                               int start, int limit,
                                               Map<String,String> args) {

    LocalRequestFactory f = getRequestFactory(qtype, start, limit);
    f.args.putAll(args);
    return f;
  }
    
  /**
   * A Factory that generates LocalSolrQueryRequest objects using a
   * specified set of default options.
   */
  public class LocalRequestFactory {
    public String qtype = "standard";
    public int start = 0;
    public int limit = 1000;
    public Map<String,String> args = new HashMap<String,String>();
    public LocalRequestFactory() {
    }
    public LocalSolrQueryRequest makeRequest(String q) {
      return new LocalSolrQueryRequest(TestHarness.this.getCore(),
                                       q, qtype, start, limit, args);
    }
  }
}

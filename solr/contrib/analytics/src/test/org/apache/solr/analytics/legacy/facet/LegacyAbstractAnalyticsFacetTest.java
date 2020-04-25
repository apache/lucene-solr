/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.analytics.legacy.facet;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.util.AnalyticsResponseHeadings;
import org.apache.solr.analytics.util.MedianCalculator;
import org.apache.solr.analytics.util.OrdinalCalculator;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.common.collect.ObjectArrays;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

public class LegacyAbstractAnalyticsFacetTest extends SolrTestCaseJ4 {
  protected static final HashMap<String,Object> defaults = new HashMap<>();

  protected String latestType = "";

  private static Document doc;
  private static XPathFactory xPathFact;
  private static String rawResponse;

  @BeforeClass
  public static void beforeClassAbstractAnalysis() {
    xPathFact = XPathFactory.newInstance();
  }

  @AfterClass
  public static void afterClassAbstractAnalysis() {
    xPathFact = null;
    doc = null;
    rawResponse = null;
    defaults.clear();
  }

  protected static void setResponse(String response) throws ParserConfigurationException, IOException, SAXException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true); // never forget this!
    DocumentBuilder builder = factory.newDocumentBuilder();
    doc = builder.parse(new InputSource(new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8))));
    rawResponse = response;
  }

  protected String getRawResponse() {
    return rawResponse;
  }

  protected Node getNode(String xPath) throws XPathExpressionException {
    return (Node)xPathFact.newXPath().compile(xPath).evaluate(doc, XPathConstants.NODE);
  }
  private NodeList getNodes(String n1, String n2, String n3, String element, String n4) throws XPathExpressionException {
    // Construct the XPath expression. The form better not change or all these will fail.
    StringBuilder sb = new StringBuilder("/response/lst[@name='"+AnalyticsResponseHeadings.COMPLETED_OLD_HEADER+"']/lst[@name='").append(n1).append("']");
    sb.append("/lst[@name='").append(n2).append("']");
    sb.append("/lst[@name='").append(n3).append("']");
    sb.append("/lst[@name!='(MISSING)']");
    sb.append("//").append(element).append("[@name='").append(n4).append("']");
    return (NodeList)xPathFact.newXPath().compile(sb.toString()).evaluate(doc, XPathConstants.NODESET);

  }
  protected ArrayList<String> getStringList(String n1, String n2, String n3, String element, String n4)
      throws XPathExpressionException {
    ArrayList<String> ret = new ArrayList<>();
    NodeList nodes = getNodes(n1, n2, n3, element, n4);
    for (int idx = 0; idx < nodes.getLength(); ++idx) {
      ret.add(nodes.item(idx).getTextContent());
    }
    return ret;
  }

  protected ArrayList<Integer> getIntegerList(String n1, String n2, String n3, String element, String n4)
      throws XPathExpressionException {
    ArrayList<Integer> ret = new ArrayList<>();
    NodeList nodes = getNodes(n1, n2, n3, element, n4);
    for (int idx = 0; idx < nodes.getLength(); ++idx) {
      ret.add(Integer.parseInt(nodes.item(idx).getTextContent()));
    }
    return ret;
  }
  protected ArrayList<Long> getLongList(String n1, String n2, String n3, String element, String n4)
      throws XPathExpressionException {
    ArrayList<Long> ret = new ArrayList<>();
    NodeList nodes = getNodes(n1, n2, n3, element, n4);
    for (int idx = 0; idx < nodes.getLength(); ++idx) {
      ret.add(Long.parseLong(nodes.item(idx).getTextContent()));
    }
    return ret;
  }
  protected ArrayList<Float> getFloatList(String n1, String n2, String n3, String element, String n4)
      throws XPathExpressionException {
    ArrayList<Float> ret = new ArrayList<>();
    NodeList nodes = getNodes(n1, n2, n3, element, n4);
    for (int idx = 0; idx < nodes.getLength(); ++idx) {
      ret.add(Float.parseFloat(nodes.item(idx).getTextContent()));
    }
    return ret;
  }

  protected ArrayList<Double> getDoubleList(String n1, String n2, String n3, String element, String n4)
      throws XPathExpressionException {
    ArrayList<Double> ret = new ArrayList<>();
    NodeList nodes = getNodes(n1, n2, n3, element, n4);
    for (int idx = 0; idx < nodes.getLength(); ++idx) {
      ret.add(Double.parseDouble(nodes.item(idx).getTextContent()));
    }
    return ret;
  }


  public static void increment(List<Long> list, int idx){
    Long i = list.remove(idx);
    list.add(idx, i+1);
  }

  public static String[] filter(String...args){
    List<String> l = new ArrayList<>();
    for( int i=0; i <args.length; i+=2){
      if( args[i+1].equals("0") || args[i+1].equals("0.0") ||
          args[i+1].equals("1800-12-31T23:59:59Z") || args[i+1].equals("str0") ||
          args[i+1].equals("this is the firststr0") ||
          args[i+1].equals("this is the secondstr0") ){
        continue;
      }
      l.add(args[i]);
      l.add(args[i+1]);
    }
    return l.toArray(new String[0]);
  }

  protected void setLatestType(String latestType) {
    this.latestType = latestType;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public <T extends Number & Comparable<T>> ArrayList calculateNumberStat(ArrayList<ArrayList<T>> lists, String stat) {
    ArrayList result;
    if (stat.equals("median")) {
      result = new ArrayList<Double>();
      for (List<T> list : lists) {
        result.add(MedianCalculator.getMedian(list));
      }
    } else if (stat.equals("mean")) {
      result = new ArrayList<Double>();
      for (List<T> list : lists) {
        double d = 0;
        for (T element : list) {
          d += element.doubleValue();
        }
        result.add(d/list.size());
      }
    } else if (stat.equals("sum")) {
      result = new ArrayList<Double>();
      for (Collection<T> list : lists) {
        double d = 0;
        for (T element : list) {
          d += element.doubleValue();
        }
        result.add(d);
      }
    } else if (stat.equals("sumOfSquares")) {
      result = new ArrayList<Double>();
      for (List<T> list : lists) {
        double d = 0;
        for (T element : list) {
          d += element.doubleValue()*element.doubleValue();
        }
        result.add(d);
      }
    } else if (stat.equals("stddev")) {
      result = new ArrayList<Double>();
      for (List<T> list : lists) {
        double sum = 0;
        double sumSquares = 0;
        for (T element : list) {
          sum += element.doubleValue();
          sumSquares += element.doubleValue()*element.doubleValue();
        }
        String res = Double.toString(Math.sqrt(sumSquares/list.size()-sum*sum/(list.size()*list.size())));
        result.add(Double.parseDouble(res));
      }
    } else {
      throw new IllegalArgumentException();
    }
    return result;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public <T extends Comparable<T>> ArrayList calculateStat(ArrayList<ArrayList<T>> lists, String stat) {
    ArrayList result;
    if (stat.contains("perc_")) {
      result = new ArrayList<T>();
      for (List<T> list : lists) {
        if( list.size() == 0) continue;
        int ord = (int) Math.ceil(Double.parseDouble(stat.substring(5))/100 * list.size()) - 1;
        ArrayList<Integer> percs = new ArrayList<>(1);
        percs.add(ord);
        OrdinalCalculator.putOrdinalsInPosition(list, percs);
        result.add(list.get(ord));
      }
    } else if (stat.equals("count")) {
      result = new ArrayList<Long>();
      for (List<T> list : lists) {
        //if( list.size() == 0) continue;
        result.add((long)list.size());
      }
    } else if (stat.equals("missing")) {
      result = new ArrayList<Long>();
      for (ArrayList<T> list : lists) {
        if( list.size() == 0) continue;
        result.add(calculateMissing(list,latestType));
      }
    } else if (stat.equals("unique")) {
      result = new ArrayList<Long>();
      for (List<T> list : lists) {
        HashSet<T> set = new HashSet<>();
        set.addAll(list);
        result.add((long)set.size());
      }
    } else if (stat.equals("max")) {
      result = new ArrayList<T>();
      for (List<T> list : lists) {
        if( list.size() == 0) continue;
        Collections.sort(list);
        result.add(list.get(list.size()-1));
      }
    } else if (stat.equals("min")) {
      result = new ArrayList<T>();
      for (List<T> list : lists) {
        if( list.size() == 0) continue;
        Collections.sort((List<T>)list);
        result.add(list.get(0));
      }
    } else {
      result = null;
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Long calculateMissing(ArrayList<T> list, String type) {
    T def = (T)defaults.get(type);
    long miss = 0;
    for (T element : list) {
      if (element.compareTo(def)==0) {
        miss++;
      }
    }
    return Long.valueOf(miss);
  }

  public static SolrQueryRequest request(String...args){
    return SolrTestCaseJ4.req( ObjectArrays.concat(BASEPARMS, args,String.class) );
  }

  public static final String[] BASEPARMS = new String[]{ "q", "*:*", "indent", "true", "olap", "true", "rows", "0" };


  public static String[] fileToStringArr(Class<?> clazz, String fileName) throws FileNotFoundException {
    InputStream in = clazz.getResourceAsStream("/solr/analytics/legacy/" + fileName);
    if (in == null) throw new FileNotFoundException("Resource not found: " + fileName);
    Scanner file = new Scanner(in, "UTF-8");
    try {
      ArrayList<String> strList = new ArrayList<>();
      while (file.hasNextLine()) {
        String line = file.nextLine();
        if (line.length()<2) {
          continue;
        }
        int commentStart = line.indexOf("//");
        if (commentStart >= 0) {
          line = line.substring(0,commentStart);
        }
        String[] param = line.split("=");
        if (param.length != 2) {
          continue;
        }
        strList.add(param[0]);
        strList.add(param[1]);
      }
      return strList.toArray(new String[0]);
    } finally {
      IOUtils.closeWhileHandlingException(file, in);
    }
  }

  protected void removeNodes(String xPath, List<Double> string) throws XPathExpressionException {
    NodeList missingNodes = getNodes(xPath);
    List<Double> result = new ArrayList<Double>();
    for (int idx = 0; idx < missingNodes.getLength(); ++idx) {
      result.add(Double.parseDouble(missingNodes.item(idx).getTextContent()));
    }
    string.removeAll(result);
  }

  protected NodeList getNodes(String xPath) throws XPathExpressionException {
    StringBuilder sb = new StringBuilder(xPath);
    return (NodeList) xPathFact.newXPath().compile(sb.toString()).evaluate(doc, XPathConstants.NODESET);
  }

}

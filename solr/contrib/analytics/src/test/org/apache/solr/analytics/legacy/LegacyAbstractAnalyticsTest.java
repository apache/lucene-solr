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
package org.apache.solr.analytics.legacy;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.util.AnalyticsResponseHeadings;
import org.apache.solr.analytics.util.MedianCalculator;
import org.apache.solr.analytics.util.OrdinalCalculator;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.common.collect.ObjectArrays;

public class LegacyAbstractAnalyticsTest extends SolrTestCaseJ4 {

  protected static final String[] BASEPARMS = new String[]{ "q", "*:*", "indent", "true", "olap", "true", "rows", "0" };
  protected static final HashMap<String,Object> defaults = new HashMap<>();

  public static enum VAL_TYPE {
    INTEGER("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    STRING("str"),
    DATE("date");

    private VAL_TYPE (final String text) {
      this.text = text;
    }

    private final String text;

    @Override
    public String toString() {
      return text;
    }
  }

  static private Document doc;
  static private XPathFactory xPathFact;

  static private String rawResponse;

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

  public static void setResponse(String response) throws ParserConfigurationException, IOException, SAXException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true); // never forget this!
    DocumentBuilder builder = factory.newDocumentBuilder();
    doc = builder.parse(new InputSource(new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8))));
    rawResponse = response;
  }

  protected String getRawResponse() {
    return rawResponse;
  }

  public Object getStatResult(String section, String name, VAL_TYPE type) throws XPathExpressionException {

    // Construct the XPath expression. The form better not change or all these will fail.
    StringBuilder sb = new StringBuilder("/response/lst[@name='"+AnalyticsResponseHeadings.COMPLETED_OLD_HEADER+"']/lst[@name='").append(section).append("']");

    // This is a little fragile in that it demands the elements have the same name as type, i.e. when looking for a
    // VAL_TYPE.DOUBLE, the element in question is <double name="blah">47.0</double>.
    sb.append("/").append(type.toString()).append("[@name='").append(name).append("']");
    String val = xPathFact.newXPath().compile(sb.toString()).evaluate(doc, XPathConstants.STRING).toString();
    try {
      switch (type) {
        case INTEGER: return Integer.parseInt(val);
        case DOUBLE:  return Double.parseDouble(val);
        case FLOAT:   return Float.parseFloat(val);
        case LONG:    return Long.parseLong(val);
        case STRING:  assertTrue(rawResponse, val != null && val.length() > 0 ); return val;
        case DATE:    assertTrue(rawResponse, val != null && val.length() > 0 ); return val;
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Caught exception in getStatResult, xPath = " + sb.toString() + " \nraw data: " + rawResponse);
    }
    fail("Unknown type used in getStatResult");
    return null; // Really can't get here, but the compiler thinks we can!
  }


  public <T extends Number & Comparable<T>> Double calculateNumberStat(ArrayList<T> list, String stat) {
    Double result;
    if (stat.equals("median")) {
      result = MedianCalculator.getMedian(list);
    } else if (stat.equals("mean")) {
      double d = 0;
      for (T element : list) {
        d += element.doubleValue();
      }
      result = Double.valueOf(d/list.size());
    } else if (stat.equals("sum")) {
      double d = 0;
      for (T element : list) {
        d += element.doubleValue();
      }
      result = Double.valueOf(d);
    } else if (stat.equals("sumOfSquares")) {
      double d = 0;
      for (T element : list) {
        d += element.doubleValue()*element.doubleValue();
      }
      result = Double.valueOf(d);
    } else if (stat.equals("stddev")) {
      double sum = 0;
      double sumSquares = 0;
      for (T element : list) {
        sum += element.doubleValue();
        sumSquares += element.doubleValue()*element.doubleValue();
      }
      result = Math.sqrt(sumSquares/list.size()-sum*sum/(list.size()*list.size()));
    } else {
      throw new IllegalArgumentException();
    }
    return result;
  }

  public <T extends Comparable<T>> Object calculateStat(ArrayList<T> list, String stat) {
    Object result;
    if (stat.contains("perc_")) {
      ArrayList<Integer> percs = new ArrayList<>(1);
      int ord = (int) Math.ceil(Double.parseDouble(stat.substring(5))/100 * list.size()) - 1;
      percs.add(ord);
      OrdinalCalculator.putOrdinalsInPosition(list, percs);
      result = list.get(percs.get(0));
    } else if (stat.equals("count")) {
      result = Long.valueOf(list.size());
    } else if (stat.equals("unique")) {
      HashSet<T> set = new HashSet<>();
      set.addAll(list);
      result = Long.valueOf((long)set.size());
    } else if (stat.equals("max")) {
      Collections.sort(list);
      result = list.get(list.size()-1);
    } else if (stat.equals("min")) {
      Collections.sort(list);
      result = list.get(0);
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

  public static SolrQueryRequest request(String[] args, String... additional){
    return SolrTestCaseJ4.req( ObjectArrays.concat(BASEPARMS, args,String.class), additional );
  }
  
  public static String[] fileToStringArr(Class<?> clazz, String fileName) throws FileNotFoundException {
    InputStream in = clazz.getResourceAsStream("/solr/analytics/legacy/" + fileName);
    if (in == null) throw new FileNotFoundException("Resource not found: " + fileName);
    Scanner file = new Scanner(in, "UTF-8");
    try {
      ArrayList<String> strList = new ArrayList<>();
      while (file.hasNextLine()) {
        String line = file.nextLine();
        line = line.trim();
        if( StringUtils.isBlank(line) || line.startsWith("#")){
          continue;
        }
        String[] param = line.split("=");
        strList.add(param[0]);
        strList.add(param[1]);
      }
      return strList.toArray(new String[0]);
    } finally {
      IOUtils.closeWhileHandlingException(file, in);
    }
  }

}

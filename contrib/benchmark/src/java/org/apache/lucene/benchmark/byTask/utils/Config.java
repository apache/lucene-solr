package org.apache.lucene.benchmark.byTask.utils;

/**
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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * Perf run configuration properties.
 * <p>
 * Numeric peroperty containing ":", e.g. "10:100:5" is interpreted 
 * as array of numeric values. It is extracted once, on first use, and 
 * maintain a round number to return the appropriate value.
 * <p>
 * The config property "work.dir" tells where is the root of 
 * docs data dirs and indexes dirs. It is set to either of: <ul>
 * <li>value supplied for it in the alg file;</li>
 * <li>otherwise, value of System property "benchmark.work.dir";</li>
 * <li>otherwise, "work".</li>
 * </ul>
 */
public class Config {

  private static final String NEW_LINE = System.getProperty("line.separator");

  private int roundNumber = 0;
  private Properties props;
  private HashMap valByRound = new HashMap();
  private HashMap colForValByRound = new HashMap();
  private String algorithmText;

  /**
   * Read both algorithm and config properties.
   * @param algReader from where to read algorithm and config properties.
   * @throws IOException
   */
  public Config (Reader algReader) throws IOException {
    // read alg file to array of lines
    ArrayList lines = new ArrayList();
    BufferedReader r = new BufferedReader(algReader);
    int lastConfigLine=0;
    for (String line = r.readLine(); line!=null; line=r.readLine()) {
      lines.add(line);
      if (line.indexOf('=')>0) {
        lastConfigLine = lines.size();
      }
    }
    r.close();
    // copy props lines to string
    StringBuffer sb = new StringBuffer();
    for (int i=0; i<lastConfigLine; i++) {
      sb.append(lines.get(i));
      sb.append(NEW_LINE);
    }
    // read props from string
    this.props = new Properties();
    props.load(new ByteArrayInputStream(sb.toString().getBytes()));

    // make sure work dir is set properly 
    if (props.get("work.dir")==null) {
      props.setProperty("work.dir",System.getProperty("benchmark.work.dir","work"));
    }
    
    if (Boolean.valueOf(props.getProperty("print.props","true")).booleanValue()) {
      printProps();
    }
    
    // copy algorithm lines
    sb = new StringBuffer();
    for (int i=lastConfigLine; i<lines.size(); i++) {
      sb.append(lines.get(i));
      sb.append(NEW_LINE);
    }
    algorithmText = sb.toString();
  }

  /**
   * Create config without algorithm - usefull for a programmatic perf test.
   * @param props - configuration properties.
   * @throws IOException
   */
  public Config (Properties props) {
    this.props = props;
    if (Boolean.valueOf(props.getProperty("print.props","true")).booleanValue()) {
      printProps();
    }
  }

  private void printProps() {
    System.out.println("------------> config properties:");
    List propKeys = new ArrayList(props.keySet());
    Collections.sort(propKeys);
    for (Iterator it = propKeys.iterator(); it.hasNext();) {
      String propName = (String) it.next();
      System.out.println(propName + " = " + props.getProperty(propName));
    }
    System.out.println("-------------------------------");
  }

  /**
   * Return a string property.
   * @param name name of property.
   * @param dflt default value.
   * @return a string property.
   */
  public String get (String name, String dflt) {
    return props.getProperty(name,dflt);
  }

  /**
   * Set a property.
   * Note: once a multiple values property is set, it can no longer be modified.
   * @param name name of property.
   * @param value either single or multiple propery value (multple values are separated by ":")
   * @throws Exception 
   */
  public void set (String name, String value) throws Exception {
    if (valByRound.get(name) != null) {
      throw new Exception("Cannot modify a multi value property!");
    }
    props.setProperty(name,value);
  }

  /**
   * Return an int property.
   * If the property contain ":", e.g. "10:100:5", it is interpreted 
   * as array of ints. It is extracted once, on first call
   * to get() it, and a by-round-value is returned. 
   * @param name name of property
   * @param dflt default value
   * @return a int property.
   */
  public int get (String name, int dflt) {
    // use value by round if already parsed
    int vals[] = (int[]) valByRound.get(name);
    if (vals != null) {
      return vals[roundNumber % vals.length];
    }
    // done if not by round 
    String sval = props.getProperty(name,""+dflt);
    if (sval.indexOf(":")<0) {
      return Integer.parseInt(sval);
    }
    // first time this prop is extracted by round
    int k = sval.indexOf(":");
    String colName = sval.substring(0,k);
    sval = sval.substring(k+1);
    colForValByRound.put(name,colName);
    vals = propToIntArray(sval);
    valByRound.put(name,vals);
    return vals[roundNumber % vals.length];
  }
  
  /**
   * Return a double property.
   * If the property contain ":", e.g. "10:100:5", it is interpreted 
   * as array of doubles. It is extracted once, on first call
   * to get() it, and a by-round-value is returned. 
   * @param name name of property
   * @param dflt default value
   * @return a double property.
   */
  public double get (String name, double dflt) {
    // use value by round if already parsed
    double vals[] = (double[]) valByRound.get(name);
    if (vals != null) {
      return vals[roundNumber % vals.length];
    }
    // done if not by round 
    String sval = props.getProperty(name,""+dflt);
    if (sval.indexOf(":")<0) {
      return Double.parseDouble(sval);
    }
    // first time this prop is extracted by round
    int k = sval.indexOf(":");
    String colName = sval.substring(0,k);
    sval = sval.substring(k+1);
    colForValByRound.put(name,colName);
    vals = propToDoubleArray(sval);
    valByRound.put(name,vals);
    return vals[roundNumber % vals.length];
  }
  
  /**
   * Return a boolean property.
   * If the property contain ":", e.g. "true.true.false", it is interpreted 
   * as array of boleans. It is extracted once, on first call
   * to get() it, and a by-round-value is returned. 
   * @param name name of property
   * @param dflt default value
   * @return a int property.
   */
  public boolean get (String name, boolean dflt) {
    // use value by round if already parsed
    boolean vals[] = (boolean[]) valByRound.get(name);
    if (vals != null) {
      return vals[roundNumber % vals.length];
    }
    // done if not by round 
    String sval = props.getProperty(name,""+dflt);
    if (sval.indexOf(":")<0) {
      return Boolean.valueOf(sval).booleanValue();
    }
    // first time this prop is extracted by round 
    int k = sval.indexOf(":");
    String colName = sval.substring(0,k);
    sval = sval.substring(k+1);
    colForValByRound.put(name,colName);
    vals = propToBooleanArray(sval);
    valByRound.put(name,vals);
    return vals[roundNumber % vals.length];
  }
  
  /**
   * Increment the round number, for config values that are extracted by round number. 
   * @return the new round number.
   */
  public int newRound () {
    roundNumber++;
    
    StringBuffer sb = new StringBuffer("--> Round ").append(roundNumber-1).append("-->").append(roundNumber);

    // log changes in values
    if (valByRound.size()>0) {
      sb.append(": ");
      for (Iterator iter = valByRound.keySet().iterator(); iter.hasNext();) {
        String name = (String) iter.next();
        Object a = valByRound.get(name);
        if (a instanceof int[]) {
          int ai[] = (int[]) a;
          int n1 = (roundNumber-1)%ai.length;
          int n2 = roundNumber%ai.length;
          sb.append("  ").append(name).append(":").append(ai[n1]).append("-->").append(ai[n2]);
        } else if (a instanceof double[]){
          double ad[] = (double[]) a;
          int n1 = (roundNumber-1)%ad.length;
          int n2 = roundNumber%ad.length;
          sb.append("  ").append(name).append(":").append(ad[n1]).append("-->").append(ad[n2]);
        }
        else {
          boolean ab[] = (boolean[]) a;
          int n1 = (roundNumber-1)%ab.length;
          int n2 = roundNumber%ab.length;
          sb.append("  ").append(name).append(":").append(ab[n1]).append("-->").append(ab[n2]);
        }
      }
    }

    System.out.println();
    System.out.println(sb.toString());
    System.out.println();
    
    return roundNumber;
  }
  
  // extract properties to array, e.g. for "10:100:5" return int[]{10,100,5}. 
  private int[] propToIntArray (String s) {
    if (s.indexOf(":")<0) {
      return new int [] { Integer.parseInt(s) };
    }
    
    ArrayList a = new ArrayList();
    StringTokenizer st = new StringTokenizer(s,":");
    while (st.hasMoreTokens()) {
      String t = st.nextToken();
      a.add(new Integer(t));
    }
    int res[] = new int[a.size()]; 
    for (int i=0; i<a.size(); i++) {
      res[i] = ((Integer) a.get(i)).intValue();
    }
    return res;
  }
    
  // extract properties to array, e.g. for "10.7:100.4:-2.3" return int[]{10.7,100.4,-2.3}. 
  private double[] propToDoubleArray (String s) {
    if (s.indexOf(":")<0) {
      return new double [] { Double.parseDouble(s) };
    }
    
    ArrayList a = new ArrayList();
    StringTokenizer st = new StringTokenizer(s,":");
    while (st.hasMoreTokens()) {
      String t = st.nextToken();
      a.add(new Double(t));
    }
    double res[] = new double[a.size()]; 
    for (int i=0; i<a.size(); i++) {
      res[i] = ((Double) a.get(i)).doubleValue();
    }
    return res;
  }
    
  // extract properties to array, e.g. for "true:true:false" return boolean[]{true,false,false}. 
  private boolean[] propToBooleanArray (String s) {
    if (s.indexOf(":")<0) {
      return new boolean [] { Boolean.valueOf(s).booleanValue() };
    }
    
    ArrayList a = new ArrayList();
    StringTokenizer st = new StringTokenizer(s,":");
    while (st.hasMoreTokens()) {
      String t = st.nextToken();
      a.add(new Boolean(t));
    }
    boolean res[] = new boolean[a.size()]; 
    for (int i=0; i<a.size(); i++) {
      res[i] = ((Boolean) a.get(i)).booleanValue();
    }
    return res;
  }

  /**
   * @return names of params set by round, for reports title
   */
  public String getColsNamesForValsByRound() {
    if (colForValByRound.size()==0) {
      return "";
    }
    StringBuffer sb = new StringBuffer(); 
    for (Iterator it = colForValByRound.keySet().iterator(); it.hasNext();) {
      String name = (String) it.next();
      String colName = (String) colForValByRound.get(name);
      sb.append(" ").append(colName);
    }
    return sb.toString();
  }

  /**
   * @return values of params set by round, for reports lines.
   */
  public String getColsValuesForValsByRound(int roundNum) {
    if (colForValByRound.size()==0) {
      return "";
    }
    StringBuffer sb = new StringBuffer(); 
    for (Iterator it = colForValByRound.keySet().iterator(); it.hasNext();) {
      String name = (String) it.next();
      String colName = (String) colForValByRound.get(name);
      String template = " "+colName;
      if (roundNum<0) {
        // just append blanks
        sb.append(Format.formatPaddLeft("-",template));
      } else {
        // append actual values, for that round
        Object a = valByRound.get(name);
        if (a instanceof int[]) {
          int ai[] = (int[]) a;
          int n = roundNum % ai.length;
          sb.append(Format.format(ai[n],template));
        }
        else if (a instanceof double[]) {
          double ad[] = (double[]) a;
          int n = roundNum % ad.length;
          sb.append(Format.format(2, ad[n],template));
        }
        else {
          boolean ab[] = (boolean[]) a;
          int n = roundNum % ab.length;
          sb.append(Format.formatPaddLeft(""+ab[n],template));
        }
      }
    }
    return sb.toString();
  }

  /**
   * @return the round number.
   */
  public int getRoundNumber() {
    return roundNumber;
  }

  /**
   * @return Returns the algorithmText.
   */
  public String getAlgorithmText() {
    return algorithmText;
  }

}

package org.apache.lucene.benchmark.byTask.tasks;

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

import java.lang.reflect.Constructor;
import java.util.StringTokenizer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.util.Version;

/**
 * Task to support benchmarking ShingleFilter / ShingleAnalyzerWrapper
 * <p>
 * <ul>
 *  <li> <code>NewShingleAnalyzer</code> (constructs with all defaults)
 *  <li> <code>NewShingleAnalyzer(analyzer:o.a.l.analysis.StandardAnalyzer,maxShingleSize:2,outputUnigrams:true)</code>
 * </ul>
 * </p>
 */
public class NewShingleAnalyzerTask extends PerfTask {

  private String analyzerClassName = "standard.StandardAnalyzer";
  private int maxShingleSize = 2;
  private boolean outputUnigrams = true;
  
  public NewShingleAnalyzerTask(PerfRunData runData) {
    super(runData);
  }

  private void setAnalyzer() throws Exception {
    Class<? extends Analyzer> clazz = null;
    Analyzer wrappedAnalyzer;
    try {
      if (analyzerClassName == null || analyzerClassName.equals("")) {
        analyzerClassName 
          = "org.apache.lucene.analysis.standard.StandardAnalyzer"; 
      }
      if (analyzerClassName.indexOf(".") == -1 
          || analyzerClassName.startsWith("standard.")) {
        //there is no package name, assume o.a.l.analysis
        analyzerClassName = "org.apache.lucene.analysis." + analyzerClassName;
      }
      clazz = Class.forName(analyzerClassName).asSubclass(Analyzer.class);
      // first try to use a ctor with version parameter (needed for many new 
      // Analyzers that have no default one anymore)
      Constructor<? extends Analyzer> ctor = clazz.getConstructor(Version.class);
      wrappedAnalyzer = ctor.newInstance(Version.LUCENE_CURRENT);
    } catch (NoSuchMethodException e) {
      // otherwise use default ctor
      wrappedAnalyzer = clazz.newInstance();
    }
    
    ShingleAnalyzerWrapper analyzer = new ShingleAnalyzerWrapper(
        wrappedAnalyzer,
        ShingleFilter.DEFAULT_MIN_SHINGLE_SIZE,
        maxShingleSize,
        ShingleFilter.TOKEN_SEPARATOR,
        outputUnigrams,
        false);
    getRunData().setAnalyzer(analyzer);
  }
  
  @Override
  public int doLogic() throws Exception {
    try {
      setAnalyzer();
      System.out.println
        ("Changed Analyzer to: ShingleAnalyzerWrapper, wrapping ShingleFilter over" 
         + analyzerClassName);
    } catch (Exception e) {
      throw new RuntimeException("Error creating Analyzer", e);
    }
    return 1;
  }
  
  @Override
  public void setParams(String params) {
    super.setParams(params);
    StringTokenizer st = new StringTokenizer(params, ",");
    while (st.hasMoreTokens()) {
      String param = st.nextToken();
      StringTokenizer expr = new StringTokenizer(param, ":");
      String key = expr.nextToken();
      String value = expr.nextToken();
      if (key.equalsIgnoreCase("analyzer")) {
        analyzerClassName = value;
      } else if (key.equalsIgnoreCase("outputUnigrams")) {
        outputUnigrams = Boolean.parseBoolean(value);
      } else if (key.equalsIgnoreCase("maxShingleSize")) {
        maxShingleSize = (int)Double.parseDouble(value);
      } else {
        throw new RuntimeException("Unknown parameter " + param);
      }
    }
  }

  @Override
  public boolean supportsParams() {
    return true;
  }
}

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
package org.apache.lucene.benchmark.byTask.tasks;


import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.byTask.PerfRunData;

/**
 * Task to support benchmarking collation.
 * <br>
 * <ul>
 *  <li> <code>NewCollationAnalyzer</code> with the default jdk impl
 *  <li> <code>NewCollationAnalyzer(impl:icu)</code> specify an impl (jdk,icu)
 * </ul>
 */
public class NewCollationAnalyzerTask extends PerfTask {
  /**
   * Different Collation implementations: currently 
   * limited to what is provided in the JDK and ICU.
   * 
   * @see <a href="http://site.icu-project.org/charts/collation-icu4j-sun">
   *      Comparison of implementations</a>
   */
  public enum Implementation { 
    JDK("org.apache.lucene.collation.CollationKeyAnalyzer", 
        "java.text.Collator"),
    ICU("org.apache.lucene.collation.ICUCollationKeyAnalyzer", 
        "com.ibm.icu.text.Collator");
    
    String className;
    String collatorClassName;
    
    Implementation(String className, String collatorClassName) {
      this.className = className;
      this.collatorClassName = collatorClassName;
    }
  }
  
  private Implementation impl = Implementation.JDK;

  public NewCollationAnalyzerTask(PerfRunData runData) {
    super(runData);
  }

  static Analyzer createAnalyzer(Locale locale, Implementation impl)
      throws Exception {
    final Class<?> collatorClazz = Class.forName(impl.collatorClassName);
    Method collatorMethod = collatorClazz.getMethod("getInstance", Locale.class);
    Object collator = collatorMethod.invoke(null, locale);
    
    final Class<? extends Analyzer> clazz = Class.forName(impl.className)
        .asSubclass(Analyzer.class);
    Constructor<? extends Analyzer> ctor = clazz.getConstructor(collatorClazz);
    return ctor.newInstance(collator);
  }
  
  @Override
  public int doLogic() throws Exception {
    try {
      Locale locale = getRunData().getLocale();
      if (locale == null) throw new RuntimeException(
          "Locale must be set with the NewLocale task!");
      Analyzer analyzer = createAnalyzer(locale, impl);
      getRunData().setAnalyzer(analyzer);
      System.out.println("Changed Analyzer to: "
          + analyzer.getClass().getName() + "(" + locale + ")");
    } catch (Exception e) {
      throw new RuntimeException("Error creating Analyzer: impl=" + impl, e);
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
      // for now we only support the "impl" parameter.
      // TODO: add strength, decomposition, etc
      if (key.equals("impl")) {
        if (value.equalsIgnoreCase("icu"))
          impl = Implementation.ICU;
        else if (value.equalsIgnoreCase("jdk"))
          impl = Implementation.JDK;
        else
          throw new RuntimeException("Unknown parameter " + param);
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

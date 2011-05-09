package org.apache.lucene.benchmark.byTask.tasks;
/**
 * Copyright 2005 The Apache Software Foundation
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.*;
import java.lang.reflect.Constructor;

/**
 * Create a new {@link org.apache.lucene.analysis.Analyzer} and set it it in the getRunData() for use by all future tasks.
 *
 */
public class NewAnalyzerTask extends PerfTask {
  private List<String> analyzerClassNames;
  private int current;

  public NewAnalyzerTask(PerfRunData runData) {
    super(runData);
    analyzerClassNames = new ArrayList<String>();
  }
  
  public static final Analyzer createAnalyzer(String className) throws Exception{
    final Class<? extends Analyzer> clazz = Class.forName(className).asSubclass(Analyzer.class);
    try {
      // first try to use a ctor with version parameter (needed for many new Analyzers that have no default one anymore
      Constructor<? extends Analyzer> cnstr = clazz.getConstructor(Version.class);
      return cnstr.newInstance(Version.LUCENE_CURRENT);
    } catch (NoSuchMethodException nsme) {
      // otherwise use default ctor
      return clazz.newInstance();
    }
  }

  @Override
  public int doLogic() throws IOException {
    String className = null;
    try {
      if (current >= analyzerClassNames.size()) {
        current = 0;
      }
      className = analyzerClassNames.get(current++);
      Analyzer analyzer = null;
      if (null == className || 0 == className.length()) {
        className = "org.apache.lucene.analysis.standard.StandardAnalyzer";
      }
      if (-1 == className.indexOf(".")) {
        try {
          // If no package, first attempt to instantiate a core analyzer
          String coreClassName = "org.apache.lucene.analysis.core." + className;
          analyzer = createAnalyzer(coreClassName);
          className = coreClassName;
        } catch (ClassNotFoundException e) {
          // If not a core analyzer, try the base analysis package 
          className = "org.apache.lucene.analysis." + className;
          analyzer = createAnalyzer(className);
        }
      } else {
        if (className.startsWith("standard.")) {
          className = "org.apache.lucene.analysis." + className;
        }
        analyzer = createAnalyzer(className);
      }
      getRunData().setAnalyzer(analyzer);
      System.out.println("Changed Analyzer to: " + className);
    } catch (Exception e) {
      throw new RuntimeException("Error creating Analyzer: " + className, e);
    }
    return 1;
  }

  /**
   * Set the params (analyzerClassName only),  Comma-separate list of Analyzer class names.  If the Analyzer lives in
   * org.apache.lucene.analysis, the name can be shortened by dropping the o.a.l.a part of the Fully Qualified Class Name.
   * <p/>
   * Example Declaration: {"NewAnalyzer" NewAnalyzer(WhitespaceAnalyzer, SimpleAnalyzer, StopAnalyzer, standard.StandardAnalyzer) >
   * @param params analyzerClassName, or empty for the StandardAnalyzer
   */
  @Override
  public void setParams(String params) {
    super.setParams(params);
    for (StringTokenizer tokenizer = new StringTokenizer(params, ","); tokenizer.hasMoreTokens();) {
      String s = tokenizer.nextToken();
      analyzerClassNames.add(s.trim());
    }
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  @Override
  public boolean supportsParams() {
    return true;
  }
}

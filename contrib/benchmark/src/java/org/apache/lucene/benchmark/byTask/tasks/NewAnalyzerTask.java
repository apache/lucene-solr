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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Create a new {@link org.apache.lucene.analysis.Analyzer} and set it it in the getRunData() for use by all future tasks.
 *
 */
public class NewAnalyzerTask extends PerfTask {
  private List/*<String>*/ analyzerClassNames;
  private int current;

  public NewAnalyzerTask(PerfRunData runData) {
    super(runData);
    analyzerClassNames = new ArrayList();
  }

  public int doLogic() throws IOException {
    String className = null;
    try {
      if (current >= analyzerClassNames.size())
      {
        current = 0;
      }
      className = (String) analyzerClassNames.get(current++);
      if (className == null || className.equals(""))
      {
        className = "org.apache.lucene.analysis.standard.StandardAnalyzer"; 
      }
      if (className.indexOf(".") == -1  || className.startsWith("standard."))//there is no package name, assume o.a.l.analysis
      {
        className = "org.apache.lucene.analysis." + className;
      }
      getRunData().setAnalyzer((Analyzer) Class.forName(className).newInstance());
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
  public boolean supportsParams() {
    return true;
  }
}

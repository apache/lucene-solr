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
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.utils.AnalyzerFactory;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.*;
import java.lang.reflect.Constructor;

/**
 * Create a new {@link org.apache.lucene.analysis.Analyzer} and set it it in the getRunData() for use by all future tasks.
 *
 */
public class NewAnalyzerTask extends PerfTask {
  private List<String> analyzerNames;
  private int current;

  public NewAnalyzerTask(PerfRunData runData) {
    super(runData);
    analyzerNames = new ArrayList<String>();
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
    String analyzerName = null;
    try {
      if (current >= analyzerNames.size()) {
        current = 0;
      }
      analyzerName = analyzerNames.get(current++);
      Analyzer analyzer = null;
      if (null == analyzerName || 0 == analyzerName.length()) {
        analyzerName = "org.apache.lucene.analysis.standard.StandardAnalyzer";
      }
      // First, lookup analyzerName as a named analyzer factory
      AnalyzerFactory factory = getRunData().getAnalyzerFactories().get(analyzerName);
      if (null != factory) {
        analyzer = factory.create();
      } else {
        if (analyzerName.contains(".")) {
          if (analyzerName.startsWith("standard.")) {
            analyzerName = "org.apache.lucene.analysis." + analyzerName;
          }
          analyzer = createAnalyzer(analyzerName);
        } else { // No package
          try {
            // Attempt to instantiate a core analyzer
            String coreClassName = "org.apache.lucene.analysis.core." + analyzerName;
            analyzer = createAnalyzer(coreClassName);
            analyzerName = coreClassName;
          } catch (ClassNotFoundException e) {
            // If not a core analyzer, try the base analysis package
            analyzerName = "org.apache.lucene.analysis." + analyzerName;
            analyzer = createAnalyzer(analyzerName);
          }
        }
      }
      getRunData().setAnalyzer(analyzer);
    } catch (Exception e) {
      throw new RuntimeException("Error creating Analyzer: " + analyzerName, e);
    }
    return 1;
  }

  /**
   * Set the params (analyzerName only),  Comma-separate list of Analyzer class names.  If the Analyzer lives in
   * org.apache.lucene.analysis, the name can be shortened by dropping the o.a.l.a part of the Fully Qualified Class Name.
   * <p/>
   * Analyzer names may also refer to previously defined AnalyzerFactory's.
   * <p/>
   * Example Declaration: {"NewAnalyzer" NewAnalyzer(WhitespaceAnalyzer, SimpleAnalyzer, StopAnalyzer, standard.StandardAnalyzer) >
   * <p/>
   * Example AnalyzerFactory usage:
   * <pre>
   * -AnalyzerFactory(name:'whitespace tokenized',WhitespaceTokenizer)
   * -NewAnalyzer('whitespace tokenized')
   * </pre>
   * @param params analyzerClassName, or empty for the StandardAnalyzer
   */
  @Override
  public void setParams(String params) {
    super.setParams(params);
    final StreamTokenizer stok = new StreamTokenizer(new StringReader(params));
    stok.quoteChar('"');
    stok.quoteChar('\'');
    stok.eolIsSignificant(false);
    stok.ordinaryChar(',');
    try {
      while (stok.nextToken() != StreamTokenizer.TT_EOF) {
        switch (stok.ttype) {
          case ',': {
            // Do nothing
            break;
          }
          case '\'':
          case '\"':
          case StreamTokenizer.TT_WORD: {
            analyzerNames.add(stok.sval);
            break;
          }
          default: {
            throw new RuntimeException("Unexpected token: " + stok.toString());
          }
        }
      }
    } catch (RuntimeException e) {
      if (e.getMessage().startsWith("Line #")) {
        throw e;
      } else {
        throw new RuntimeException("Line #" + (stok.lineno() + getAlgLineNum()) + ": ", e);
      }
    } catch (Throwable t) {
      throw new RuntimeException("Line #" + (stok.lineno() + getAlgLineNum()) + ": ", t);
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

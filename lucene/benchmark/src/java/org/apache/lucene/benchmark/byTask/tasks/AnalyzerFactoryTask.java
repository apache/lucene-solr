package org.apache.lucene.benchmark.byTask.tasks;

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

import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.FilesystemResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.utils.AnalyzerFactory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Analyzer factory construction task.  The name given to the constructed factory may
 * be given to NewAnalyzerTask, which will call AnalyzerFactory.create().
 *
 * Params are in the form argname:argvalue or argname:"argvalue" or argname:'argvalue';
 * use backslashes to escape '"' or "'" inside a quoted value when it's used as the enclosing
 * quotation mark,
 *
 * Specify params in a comma separated list of the following, in order:
 * <ol>
 *   <li>Analyzer args:
 *     <ul>
 *       <li><b>Required</b>: <code>name:<i>analyzer-factory-name</i></code></li>
 *       <li>Optional: <tt>positionIncrementGap:<i>int value</i></tt> (default: 0)</li>
 *       <li>Optional: <tt>offsetGap:<i>int value</i></tt> (default: 1)</li>
 *     </ul>
 *   </li>
 *   <li>zero or more CharFilterFactory's, followed by</li>
 *   <li>exactly one TokenizerFactory, followed by</li>
 *   <li>zero or more TokenFilterFactory's</li>
 * </ol>
 *
 * Each component analysis factory map specify <tt>luceneMatchVersion</tt> (defaults to
 * {@link Version#LUCENE_CURRENT}) and any of the args understood by the specified
 * *Factory class, in the above-describe param format.
 * <p/>
 * Example:
 * <pre>
 *     -AnalyzerFactory(name:'strip html, fold to ascii, whitespace tokenize, max 10k tokens',
 *                      positionIncrementGap:100,
 *                      HTMLStripCharFilter,
 *                      MappingCharFilter(mapping:'mapping-FoldToASCII.txt'),
 *                      WhitespaceTokenizer(luceneMatchVersion:LUCENE_43),
 *                      TokenLimitFilter(maxTokenCount:10000, consumeAllTokens:false))
 *     [...]
 *     -NewAnalyzer('strip html, fold to ascii, whitespace tokenize, max 10k tokens')
 * </pre>
 * <p/>
 * AnalyzerFactory will direct analysis component factories to look for resources
 * under the directory specified in the "work.dir" property.
 */
public class AnalyzerFactoryTask extends PerfTask {
  private static final String LUCENE_ANALYSIS_PACKAGE_PREFIX = "org.apache.lucene.analysis.";
  private static final Pattern ANALYSIS_COMPONENT_SUFFIX_PATTERN
      = Pattern.compile("(?s:(?:(?:Token|Char)?Filter|Tokenizer)(?:Factory)?)$");
  private static final Pattern TRAILING_DOT_ZERO_PATTERN = Pattern.compile("\\.0$");

  private enum ArgType {ANALYZER_ARG, ANALYZER_ARG_OR_CHARFILTER_OR_TOKENIZER, TOKENFILTER }

  String factoryName = null;
  Integer positionIncrementGap = null;
  Integer offsetGap = null;
  private List<CharFilterFactory> charFilterFactories = new ArrayList<CharFilterFactory>();
  private TokenizerFactory tokenizerFactory = null;
  private List<TokenFilterFactory> tokenFilterFactories = new ArrayList<TokenFilterFactory>();

  public AnalyzerFactoryTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public int doLogic() {
    return 1;
  }

  /**
   * Sets the params.
   * Analysis component factory names may optionally include the "Factory" suffix.
   *
   * @param params analysis pipeline specification: name, (optional) positionIncrementGap,
   *               (optional) offsetGap, 0+ CharFilterFactory's, 1 TokenizerFactory,
   *               and 0+ TokenFilterFactory's
   */
  @Override
  public void setParams(String params) {
    super.setParams(params);
    ArgType expectedArgType = ArgType.ANALYZER_ARG;

    final StreamTokenizer stok = new StreamTokenizer(new StringReader(params));
    stok.commentChar('#');
    stok.quoteChar('"');
    stok.quoteChar('\'');
    stok.eolIsSignificant(false);
    stok.ordinaryChar('(');
    stok.ordinaryChar(')');
    stok.ordinaryChar(':');
    stok.ordinaryChar(',');
    try {
      while (stok.nextToken() != StreamTokenizer.TT_EOF) {
        switch (stok.ttype) {
          case ',': {
            // Do nothing
            break;
          }
          case StreamTokenizer.TT_WORD: {
            if (expectedArgType.equals(ArgType.ANALYZER_ARG)) {
              final String argName = stok.sval;
              if ( ! argName.equalsIgnoreCase("name")
                  && ! argName.equalsIgnoreCase("positionIncrementGap")
                  && ! argName.equalsIgnoreCase("offsetGap")) {
                throw new RuntimeException
                    ("Line #" + lineno(stok) + ": Missing 'name' param to AnalyzerFactory: '" + params + "'");
              }
              stok.nextToken();
              if (stok.ttype != ':') {
                throw new RuntimeException
                    ("Line #" + lineno(stok) + ": Missing ':' after '" + argName + "' param to AnalyzerFactory");
              }

              stok.nextToken();
              String argValue = stok.sval;
              switch (stok.ttype) {
                case StreamTokenizer.TT_NUMBER: {
                  argValue = Double.toString(stok.nval);
                  // Drop the ".0" from numbers, for integer arguments
                  argValue = TRAILING_DOT_ZERO_PATTERN.matcher(argValue).replaceFirst("");
                  // Intentional fallthrough
                }
                case '"':
                case '\'':
                case StreamTokenizer.TT_WORD: {
                  if (argName.equalsIgnoreCase("name")) {
                    factoryName = argValue;
                    expectedArgType = ArgType.ANALYZER_ARG_OR_CHARFILTER_OR_TOKENIZER;
                  } else {
                    int intArgValue = 0;
                    try {
                      intArgValue = Integer.parseInt(argValue);
                    } catch (NumberFormatException e) {
                      throw new RuntimeException
                          ("Line #" + lineno(stok) + ": Exception parsing " + argName + " value '" + argValue + "'", e);
                    }
                    if (argName.equalsIgnoreCase("positionIncrementGap")) {
                      positionIncrementGap = intArgValue;
                    } else if (argName.equalsIgnoreCase("offsetGap")) {
                      offsetGap = intArgValue;
                    }
                  }
                  break;
                }
                case StreamTokenizer.TT_EOF: {
                  throw new RuntimeException("Unexpected EOF: " + stok.toString());
                }
                default: {
                  throw new RuntimeException
                      ("Line #" + lineno(stok) + ": Unexpected token: " + stok.toString());
                }
              }
            } else if (expectedArgType.equals(ArgType.ANALYZER_ARG_OR_CHARFILTER_OR_TOKENIZER)) {
              final String argName = stok.sval;

              if (argName.equalsIgnoreCase("positionIncrementGap")
                  || argName.equalsIgnoreCase("offsetGap")) {
                stok.nextToken();
                if (stok.ttype != ':') {
                  throw new RuntimeException
                      ("Line #" + lineno(stok) + ": Missing ':' after '" + argName + "' param to AnalyzerFactory");
                }
                stok.nextToken();
                int intArgValue = (int)stok.nval;
                switch (stok.ttype) {
                  case '"':
                  case '\'':
                  case StreamTokenizer.TT_WORD: {
                    intArgValue = 0;
                    try {
                      intArgValue = Integer.parseInt(stok.sval.trim());
                    } catch (NumberFormatException e) {
                      throw new RuntimeException
                          ("Line #" + lineno(stok) + ": Exception parsing " + argName + " value '" + stok.sval + "'", e);
                    }
                    // Intentional fall-through
                  }
                  case StreamTokenizer.TT_NUMBER: {
                    if (argName.equalsIgnoreCase("positionIncrementGap")) {
                      positionIncrementGap = intArgValue;
                    } else if (argName.equalsIgnoreCase("offsetGap")) {
                      offsetGap = intArgValue;
                    }
                    break;
                  }
                  case StreamTokenizer.TT_EOF: {
                    throw new RuntimeException("Unexpected EOF: " + stok.toString());
                  }
                  default: {
                    throw new RuntimeException
                        ("Line #" + lineno(stok) + ": Unexpected token: " + stok.toString());
                  }
                }
                break;
              }
              try {
                final Class<? extends CharFilterFactory> clazz;
                clazz = lookupAnalysisClass(argName, CharFilterFactory.class);
                createAnalysisPipelineComponent(stok, clazz);
              } catch (IllegalArgumentException e) {
                try {
                  final Class<? extends TokenizerFactory> clazz;
                  clazz = lookupAnalysisClass(argName, TokenizerFactory.class);
                  createAnalysisPipelineComponent(stok, clazz);
                  expectedArgType = ArgType.TOKENFILTER;
                } catch (IllegalArgumentException e2) {
                  throw new RuntimeException("Line #" + lineno(stok) + ": Can't find class '"
                                             + argName + "' as CharFilterFactory or TokenizerFactory");
                }
              }
            } else { // expectedArgType = ArgType.TOKENFILTER
              final String className = stok.sval;
              final Class<? extends TokenFilterFactory> clazz;
              try {
                clazz = lookupAnalysisClass(className, TokenFilterFactory.class);
              } catch (IllegalArgumentException e) {
                  throw new RuntimeException
                      ("Line #" + lineno(stok) + ": Can't find class '" + className + "' as TokenFilterFactory");
              }
              createAnalysisPipelineComponent(stok, clazz);
            }
            break;
          }
          default: {
            throw new RuntimeException("Line #" + lineno(stok) + ": Unexpected token: " + stok.toString());
          }
        }
      }
    } catch (RuntimeException e) {
      if (e.getMessage().startsWith("Line #")) {
        throw e;
      } else {
        throw new RuntimeException("Line #" + lineno(stok) + ": ", e);
      }
    } catch (Throwable t) {
      throw new RuntimeException("Line #" + lineno(stok) + ": ", t);
    }

    final AnalyzerFactory analyzerFactory = new AnalyzerFactory
        (charFilterFactories, tokenizerFactory, tokenFilterFactories);
    analyzerFactory.setPositionIncrementGap(positionIncrementGap);
    analyzerFactory.setOffsetGap(offsetGap);
    getRunData().getAnalyzerFactories().put(factoryName, analyzerFactory);
  }

  /**
   * Instantiates the given analysis factory class after pulling params from
   * the given stream tokenizer, then stores the result in the appropriate
   * pipeline component list.
   *
   * @param stok stream tokenizer from which to draw analysis factory params
   * @param clazz analysis factory class to instantiate
   */
  private void createAnalysisPipelineComponent
      (StreamTokenizer stok, Class<? extends AbstractAnalysisFactory> clazz) {
    Map<String,String> argMap = new HashMap<String,String>();
    boolean parenthetical = false;
    try {
      WHILE_LOOP: while (stok.nextToken() != StreamTokenizer.TT_EOF) {
        switch (stok.ttype) {
          case ',': {
            if (parenthetical) {
              // Do nothing
              break;
            } else {
              // Finished reading this analysis factory configuration
              break WHILE_LOOP;
            }
          }
          case '(': {
            if (parenthetical) {
              throw new RuntimeException
                  ("Line #" + lineno(stok) + ": Unexpected opening parenthesis.");
            }
            parenthetical = true;
            break;
          }
          case ')': {
            if (parenthetical) {
              parenthetical = false;
            } else {
              throw new RuntimeException
                  ("Line #" + lineno(stok) + ": Unexpected closing parenthesis.");
            }
            break;
          }
          case StreamTokenizer.TT_WORD: {
            if ( ! parenthetical) {
              throw new RuntimeException("Line #" + lineno(stok) + ": Unexpected token '" + stok.sval + "'");
            }
            String argName = stok.sval;
            stok.nextToken();
            if (stok.ttype != ':') {
              throw new RuntimeException
                  ("Line #" + lineno(stok) + ": Missing ':' after '" + argName + "' param to " + clazz.getSimpleName());
            }
            stok.nextToken();
            String argValue = stok.sval;
            switch (stok.ttype) {
              case StreamTokenizer.TT_NUMBER: {
                  argValue = Double.toString(stok.nval);
                  // Drop the ".0" from numbers, for integer arguments
                  argValue = TRAILING_DOT_ZERO_PATTERN.matcher(argValue).replaceFirst("");
                  // Intentional fall-through
              }
              case '"':
              case '\'':
              case StreamTokenizer.TT_WORD: {
                argMap.put(argName, argValue);
                break;
              }
              case StreamTokenizer.TT_EOF: {
                throw new RuntimeException("Unexpected EOF: " + stok.toString());
              }
              default: {
                throw new RuntimeException
                    ("Line #" + lineno(stok) + ": Unexpected token: " + stok.toString());
              }
            }
          }
        }
      }
      if (!argMap.containsKey("luceneMatchVersion")) {
        argMap.put("luceneMatchVersion", Version.LUCENE_CURRENT.toString());
      }
      final AbstractAnalysisFactory instance;
      try {
        instance = clazz.getConstructor(Map.class).newInstance(argMap);
      } catch (Exception e) {
        throw new RuntimeException("Line #" + lineno(stok) + ": ", e);
      }
      if (instance instanceof ResourceLoaderAware) {
        File baseDir = new File(getRunData().getConfig().get("work.dir", "work")).getAbsoluteFile();
        if ( ! baseDir.isDirectory()) {
          baseDir = new File(".").getAbsoluteFile();
        }
        ((ResourceLoaderAware)instance).inform(new FilesystemResourceLoader(baseDir));
      }
      if (CharFilterFactory.class.isAssignableFrom(clazz)) {
        charFilterFactories.add((CharFilterFactory)instance);
      } else if (TokenizerFactory.class.isAssignableFrom(clazz)) {
        tokenizerFactory = (TokenizerFactory)instance;
      } else if (TokenFilterFactory.class.isAssignableFrom(clazz)) {
        tokenFilterFactories.add((TokenFilterFactory)instance);
      }
    } catch (RuntimeException e) {
      if (e.getMessage().startsWith("Line #")) {
        throw (e);
      } else {
        throw new RuntimeException("Line #" + lineno(stok) + ": ", e);
      }
    } catch (Throwable t) {
      throw new RuntimeException("Line #" + lineno(stok) + ": ", t);
    }
  }

  /**
   * This method looks up a class with its fully qualified name (FQN), or a short-name
   * class-simplename, or with a package suffix, assuming "org.apache.lucene.analysis."
   * as the package prefix (e.g. "standard.ClassicTokenizerFactory" ->
   * "org.apache.lucene.analysis.standard.ClassicTokenizerFactory").
   *
   * If className contains a period, the class is first looked up as-is, assuming that it
   * is an FQN.  If this fails, lookup is retried after prepending the Lucene analysis
   * package prefix to the class name.
   *
   * If className does not contain a period, the analysis SPI *Factory.lookupClass()
   * methods are used to find the class.
   *
   * @param className The name or the short name of the class.
   * @param expectedType The superclass className is expected to extend
   * @return the loaded class.
   * @throws ClassNotFoundException if lookup fails
   */
  public <T> Class<? extends T> lookupAnalysisClass(String className, Class<T> expectedType)
      throws ClassNotFoundException {
    if (className.contains(".")) {
      try {
        // First, try className == FQN
        return Class.forName(className).asSubclass(expectedType);
      } catch (ClassNotFoundException e) {
        try {
          // Second, retry lookup after prepending the Lucene analysis package prefix
          return Class.forName(LUCENE_ANALYSIS_PACKAGE_PREFIX + className).asSubclass(expectedType);
        } catch (ClassNotFoundException e1) {
          throw new ClassNotFoundException("Can't find class '" + className
                                           + "' or '" + LUCENE_ANALYSIS_PACKAGE_PREFIX + className + "'");
        }
      }
    }
    // No dot - use analysis SPI lookup
    final String analysisComponentName = ANALYSIS_COMPONENT_SUFFIX_PATTERN.matcher(className).replaceFirst("");
    if (CharFilterFactory.class.isAssignableFrom(expectedType)) {
      return CharFilterFactory.lookupClass(analysisComponentName).asSubclass(expectedType);
    } else if (TokenizerFactory.class.isAssignableFrom(expectedType)) {
      return TokenizerFactory.lookupClass(analysisComponentName).asSubclass(expectedType);
    } else if (TokenFilterFactory.class.isAssignableFrom(expectedType)) {
      return TokenFilterFactory.lookupClass(analysisComponentName).asSubclass(expectedType);
    }

    throw new ClassNotFoundException("Can't find class '" + className + "'");
  }


  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  @Override
  public boolean supportsParams() {
    return true;
  }

  /** Returns the current line in the algorithm file */
  public int lineno(StreamTokenizer stok) {
    return getAlgLineNum() + stok.lineno();
  }
}

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
package org.apache.lucene.analysis.util;


import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

/**
 * Abstract parent class for analysis factories {@link TokenizerFactory},
 * {@link TokenFilterFactory} and {@link CharFilterFactory}.
 * <p>
 * The typical lifecycle for a factory consumer is:
 * <ol>
 *   <li>Create factory via its constructor (or via XXXFactory.forName)
 *   <li>(Optional) If the factory uses resources such as files, {@link ResourceLoaderAware#inform(ResourceLoader)} is called to initialize those resources.
 *   <li>Consumer calls create() to obtain instances.
 * </ol>
 */
public abstract class AbstractAnalysisFactory {
  public static final String LUCENE_MATCH_VERSION_PARAM = "luceneMatchVersion";

  /** The original args, before any processing */
  private final Map<String,String> originalArgs;

  /** the luceneVersion arg */
  protected final Version luceneMatchVersion;
  /** whether the luceneMatchVersion arg is explicitly specified in the serialized schema */
  private boolean isExplicitLuceneMatchVersion = false;

  /**
   * Initialize this factory via a set of key-value pairs.
   */
  protected AbstractAnalysisFactory(Map<String,String> args) {
    originalArgs = Collections.unmodifiableMap(new HashMap<>(args));
    String version = get(args, LUCENE_MATCH_VERSION_PARAM);
    if (version == null) {
      luceneMatchVersion = Version.LATEST;
    } else {
      try {
        luceneMatchVersion = Version.parseLeniently(version);
      } catch (ParseException pe) {
        throw new IllegalArgumentException(pe);
      }
    }
    args.remove(CLASS_NAME);  // consume the class arg
  }
  
  public final Map<String,String> getOriginalArgs() {
    return originalArgs;
  }

  public final Version getLuceneMatchVersion() {
    return this.luceneMatchVersion;
  }
  
  public String require(Map<String,String> args, String name) {
    String s = args.remove(name);
    if (s == null) {
      throw new IllegalArgumentException("Configuration Error: missing parameter '" + name + "'");
    }
    return s;
  }
  public String require(Map<String,String> args, String name, Collection<String> allowedValues) {
    return require(args, name, allowedValues, true);
  }
  public String require(Map<String,String> args, String name, Collection<String> allowedValues, boolean caseSensitive) {
    String s = args.remove(name);
    if (s == null) {
      throw new IllegalArgumentException("Configuration Error: missing parameter '" + name + "'");
    } else {
      for (String allowedValue : allowedValues) {
        if (caseSensitive) {
          if (s.equals(allowedValue)) {
            return s;
          }
        } else {
          if (s.equalsIgnoreCase(allowedValue)) {
            return s;
          }
        }
      }
      throw new IllegalArgumentException("Configuration Error: '" + name + "' value must be one of " + allowedValues);
    }
  }
  public String get(Map<String,String> args, String name) {
    return args.remove(name); // defaultVal = null
  }
  public String get(Map<String,String> args, String name, String defaultVal) {
    String s = args.remove(name);
    return s == null ? defaultVal : s;
  }
  public String get(Map<String,String> args, String name, Collection<String> allowedValues) {
    return get(args, name, allowedValues, null); // defaultVal = null
  }
  public String get(Map<String,String> args, String name, Collection<String> allowedValues, String defaultVal) {
    return get(args, name, allowedValues, defaultVal, true);
  }
  public String get(Map<String,String> args, String name, Collection<String> allowedValues, String defaultVal, boolean caseSensitive) {
    String s = args.remove(name);
    if (s == null) {
      return defaultVal;
    } else {
      for (String allowedValue : allowedValues) {
        if (caseSensitive) {
          if (s.equals(allowedValue)) {
            return s;
          }
        } else {
          if (s.equalsIgnoreCase(allowedValue)) {
            return s;
          }
        }
      }
      throw new IllegalArgumentException("Configuration Error: '" + name + "' value must be one of " + allowedValues);
    }
  }

  protected final int requireInt(Map<String,String> args, String name) {
    return Integer.parseInt(require(args, name));
  }
  protected final int getInt(Map<String,String> args, String name, int defaultVal) {
    String s = args.remove(name);
    return s == null ? defaultVal : Integer.parseInt(s);
  }

  protected final boolean requireBoolean(Map<String,String> args, String name) {
    return Boolean.parseBoolean(require(args, name));
  }
  protected final boolean getBoolean(Map<String,String> args, String name, boolean defaultVal) {
    String s = args.remove(name);
    return s == null ? defaultVal : Boolean.parseBoolean(s);
  }

  protected final float requireFloat(Map<String,String> args, String name) {
    return Float.parseFloat(require(args, name));
  }
  protected final float getFloat(Map<String,String> args, String name, float defaultVal) {
    String s = args.remove(name);
    return s == null ? defaultVal : Float.parseFloat(s);
  }

  public char requireChar(Map<String,String> args, String name) {
    return require(args, name).charAt(0);
  }
  public char getChar(Map<String,String> args, String name, char defaultValue) {
    String s = args.remove(name);
    if (s == null) {
      return defaultValue;
    } else { 
      if (s.length() != 1) {
        throw new IllegalArgumentException(name + " should be a char. \"" + s + "\" is invalid");
      } else {
        return s.charAt(0);
      }
    }
  }
  
  private static final Pattern ITEM_PATTERN = Pattern.compile("[^,\\s]+");

  /** Returns whitespace- and/or comma-separated set of values, or null if none are found */
  public Set<String> getSet(Map<String,String> args, String name) {
    String s = args.remove(name);
    if (s == null) {
     return null;
    } else {
      Set<String> set = null;
      Matcher matcher = ITEM_PATTERN.matcher(s);
      if (matcher.find()) {
        set = new HashSet<>();
        set.add(matcher.group(0));
        while (matcher.find()) {
          set.add(matcher.group(0));
        }
      }
      return set;
    }
  }

  /**
   * Compiles a pattern for the value of the specified argument key <code>name</code> 
   */
  protected final Pattern getPattern(Map<String,String> args, String name) {
    try {
      return Pattern.compile(require(args, name));
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException
        ("Configuration Error: '" + name + "' can not be parsed in " +
         this.getClass().getSimpleName(), e);
    }
  }

  /**
   * Returns as {@link CharArraySet} from wordFiles, which
   * can be a comma-separated list of filenames
   */
  protected final CharArraySet getWordSet(ResourceLoader loader,
      String wordFiles, boolean ignoreCase) throws IOException {
    List<String> files = splitFileNames(wordFiles);
    CharArraySet words = null;
    if (files.size() > 0) {
      // default stopwords list has 35 or so words, but maybe don't make it that
      // big to start
      words = new CharArraySet(files.size() * 10, ignoreCase);
      for (String file : files) {
        List<String> wlist = getLines(loader, file.trim());
        words.addAll(StopFilter.makeStopSet(wlist, ignoreCase));
      }
    }
    return words;
  }
  
  /**
   * Returns the resource's lines (with content treated as UTF-8)
   */
  protected final List<String> getLines(ResourceLoader loader, String resource) throws IOException {
    return WordlistLoader.getLines(loader.openResource(resource), StandardCharsets.UTF_8);
  }

  /** same as {@link #getWordSet(ResourceLoader, String, boolean)},
   * except the input is in snowball format. */
  protected final CharArraySet getSnowballWordSet(ResourceLoader loader,
      String wordFiles, boolean ignoreCase) throws IOException {
    List<String> files = splitFileNames(wordFiles);
    CharArraySet words = null;
    if (files.size() > 0) {
      // default stopwords list has 35 or so words, but maybe don't make it that
      // big to start
      words = new CharArraySet(files.size() * 10, ignoreCase);
      for (String file : files) {
        InputStream stream = null;
        Reader reader = null;
        try {
          stream = loader.openResource(file.trim());
          CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT);
          reader = new InputStreamReader(stream, decoder);
          WordlistLoader.getSnowballWordSet(reader, words);
        } finally {
          IOUtils.closeWhileHandlingException(reader, stream);
        }
      }
    }
    return words;
  }

  /**
   * Splits file names separated by comma character.
   * File names can contain comma characters escaped by backslash '\'
   *
   * @param fileNames the string containing file names
   * @return a list of file names with the escaping backslashed removed
   */
  protected final List<String> splitFileNames(String fileNames) {
    return splitAt(',', fileNames);
  }

  /**
   * Splits a list separated by zero or more given separator characters.
   * List items can contain comma characters escaped by backslash '\'.
   * Whitespace is NOT trimmed from the returned list items.
   *
   * @param list the string containing the split list items
   * @return a list of items with the escaping backslashes removed
   */
  protected final List<String> splitAt(char separator, String list) {
    if (list == null)
      return Collections.emptyList();

    List<String> result = new ArrayList<>();
    for (String item : list.split("(?<!\\\\)[" + separator + "]")) {
      result.add(item.replaceAll("\\\\(?=[" + separator + "])", ""));
    }

    return result;
  }

  private static final String CLASS_NAME = "class";
  
  /**
   * @return the string used to specify the concrete class name in a serialized representation: the class arg.  
   *         If the concrete class name was not specified via a class arg, returns {@code getClass().getName()}.
   */ 
  public String getClassArg() {
    if (null != originalArgs) {
      String className = originalArgs.get(CLASS_NAME);
      if (null != className) {
        return className;
      }
    }
    return getClass().getName();
  }

  public boolean isExplicitLuceneMatchVersion() {
    return isExplicitLuceneMatchVersion;
  }

  public void setExplicitLuceneMatchVersion(boolean isExplicitLuceneMatchVersion) {
    this.isExplicitLuceneMatchVersion = isExplicitLuceneMatchVersion;
  }
}

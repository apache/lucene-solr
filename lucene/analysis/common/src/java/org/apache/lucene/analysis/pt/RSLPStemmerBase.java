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
package org.apache.lucene.analysis.pt;


import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.CharArraySet;

import static org.apache.lucene.analysis.util.StemmerUtil.*;

/**
 * Base class for stemmers that use a set of RSLP-like stemming steps.
 * <p>
 * RSLP (Removedor de Sufixos da Lingua Portuguesa) is an algorithm designed
 * originally for stemming the Portuguese language, described in the paper
 * <i>A Stemming Algorithm for the Portuguese Language</i>, Orengo et. al.
 * <p>
 * Since this time a plural-only modification (RSLP-S) as well as a modification
 * for the Galician language have been implemented. This class parses a configuration
 * file that describes {@link Step}s, where each Step contains a set of {@link Rule}s.
 * <p>
 * The general rule format is: 
 * <blockquote>{ "suffix", N, "replacement", { "exception1", "exception2", ...}}</blockquote>
 * where:
 * <ul>
 *   <li><code>suffix</code> is the suffix to be removed (such as "inho").
 *   <li><code>N</code> is the min stem size, where stem is defined as the candidate stem 
 *       after removing the suffix (but before appending the replacement!)
 *   <li><code>replacement</code> is an optimal string to append after removing the suffix.
 *       This can be the empty string.
 *   <li><code>exceptions</code> is an optional list of exceptions, patterns that should 
 *       not be stemmed. These patterns can be specified as whole word or suffix (ends-with) 
 *       patterns, depending upon the exceptions format flag in the step header.
 * </ul>
 * <p>
 * A step is an ordered list of rules, with a structure in this format:
 * <blockquote>{ "name", N, B, { "cond1", "cond2", ... }
 *               ... rules ... };
 * </blockquote>
 * where:
 * <ul>
 *   <li><code>name</code> is a name for the step (such as "Plural").
 *   <li><code>N</code> is the min word size. Words that are less than this length bypass
 *       the step completely, as an optimization. Note: N can be zero, in this case this 
 *       implementation will automatically calculate the appropriate value from the underlying 
 *       rules.
 *   <li><code>B</code> is a "boolean" flag specifying how exceptions in the rules are matched.
 *       A value of 1 indicates whole-word pattern matching, a value of 0 indicates that 
 *       exceptions are actually suffixes and should be matched with ends-with.
 *   <li><code>conds</code> are an optional list of conditions to enter the step at all. If
 *       the list is non-empty, then a word must end with one of these conditions or it will
 *       bypass the step completely as an optimization.
 * </ul>
 * <p>
 * @see <a href="http://www.inf.ufrgs.br/~viviane/rslp/index.htm">RSLP description</a>
 * @lucene.internal
 */
public abstract class RSLPStemmerBase {
  
  /**
   * A basic rule, with no exceptions.
   */
  protected static class Rule {
    protected final char suffix[];
    protected final char replacement[];
    protected final int min;
    
    /**
     * Create a rule.
     * @param suffix suffix to remove
     * @param min minimum stem length
     * @param replacement replacement string
     */
    public Rule(String suffix, int min, String replacement) {
      this.suffix = suffix.toCharArray();
      this.replacement = replacement.toCharArray();
      this.min = min;
    }
    
    /**
     * @return true if the word matches this rule.
     */
    public boolean matches(char s[], int len) {
      return (len - suffix.length >= min && endsWith(s, len, suffix));
    }
    
    /**
     * @return new valid length of the string after firing this rule.
     */
    public int replace(char s[], int len) {
      if (replacement.length > 0) {
        System.arraycopy(replacement, 0, s, len - suffix.length, replacement.length);
      }
      return len - suffix.length + replacement.length;
    }
  }
  
  /**
   * A rule with a set of whole-word exceptions.
   */
  protected static class RuleWithSetExceptions extends Rule {
    protected final CharArraySet exceptions;
    
    public RuleWithSetExceptions(String suffix, int min, String replacement,
        String[] exceptions) {
      super(suffix, min, replacement);
      for (int i = 0; i < exceptions.length; i++) {
        if (!exceptions[i].endsWith(suffix))
          throw new RuntimeException("useless exception '" + exceptions[i] + "' does not end with '" + suffix + "'");
      }
      this.exceptions = new CharArraySet(Arrays.asList(exceptions), false);
    }

    @Override
    public boolean matches(char s[], int len) {
      return super.matches(s, len) && !exceptions.contains(s, 0, len);
    }
  }
  
  /**
   * A rule with a set of exceptional suffixes.
   */
  protected static class RuleWithSuffixExceptions extends Rule {
    // TODO: use a more efficient datastructure: automaton?
    protected final char[][] exceptions;
    
    public RuleWithSuffixExceptions(String suffix, int min, String replacement,
        String[] exceptions) {
      super(suffix, min, replacement);
      for (int i = 0; i < exceptions.length; i++) {
        if (!exceptions[i].endsWith(suffix))
          throw new RuntimeException("warning: useless exception '" + exceptions[i] + "' does not end with '" + suffix + "'");
      }
      this.exceptions = new char[exceptions.length][];
      for (int i = 0; i < exceptions.length; i++)
        this.exceptions[i] = exceptions[i].toCharArray();
    }
    
    @Override
    public boolean matches(char s[], int len) {
      if (!super.matches(s, len))
        return false;
      
      for (int i = 0; i < exceptions.length; i++)
        if (endsWith(s, len, exceptions[i]))
          return false;

      return true;
    }
  }
  
  /**
   * A step containing a list of rules.
   */
  protected static class Step {
    protected final String name;
    protected final Rule rules[];
    protected final int min;
    protected final char[][] suffixes;
    
    /**
     * Create a new step
     * @param name Step's name.
     * @param rules an ordered list of rules.
     * @param min minimum word size. if this is 0 it is automatically calculated.
     * @param suffixes optional list of conditional suffixes. may be null.
     */
    public Step(String name, Rule rules[], int min, String suffixes[]) {
      this.name = name;
      this.rules = rules;
      if (min == 0) {
        min = Integer.MAX_VALUE;
        for (Rule r : rules)
          min = Math.min(min, r.min + r.suffix.length);
      }
      this.min = min;
      
      if (suffixes == null || suffixes.length == 0) {
        this.suffixes = null;
      } else {
        this.suffixes = new char[suffixes.length][];
        for (int i = 0; i < suffixes.length; i++)
          this.suffixes[i] = suffixes[i].toCharArray();
      }
    }
    
    /**
     * @return new valid length of the string after applying the entire step.
     */
    public int apply(char s[], int len) {
      if (len < min)
        return len;
      
      if (suffixes != null) {
        boolean found = false;
        
        for (int i = 0; i < suffixes.length; i++)
          if (endsWith(s, len, suffixes[i])) {
            found = true;
            break;
          }
        
        if (!found) return len;
      }
      
      for (int i = 0; i < rules.length; i++) {
        if (rules[i].matches(s, len))
          return rules[i].replace(s, len);
      }
      
      return len;
    }
  }
  
  /**
   * Parse a resource file into an RSLP stemmer description.
   * @return a Map containing the named Steps in this description.
   */
  protected static Map<String,Step> parse(Class<? extends RSLPStemmerBase> clazz, String resource) {
    // TODO: this parser is ugly, but works. use a jflex grammar instead.
    try {
      InputStream is = clazz.getResourceAsStream(resource);
      LineNumberReader r = new LineNumberReader(new InputStreamReader(is, StandardCharsets.UTF_8));
      Map<String,Step> steps = new HashMap<>();
      String step;
      while ((step = readLine(r)) != null) {
        Step s = parseStep(r, step);
        steps.put(s.name, s);
      }
      r.close();
      return steps;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  private static final Pattern headerPattern = 
    Pattern.compile("^\\{\\s*\"([^\"]*)\",\\s*([0-9]+),\\s*(0|1),\\s*\\{(.*)\\},\\s*$");
  private static final Pattern stripPattern = 
    Pattern.compile("^\\{\\s*\"([^\"]*)\",\\s*([0-9]+)\\s*\\}\\s*(,|(\\}\\s*;))$");
  private static final Pattern repPattern = 
    Pattern.compile("^\\{\\s*\"([^\"]*)\",\\s*([0-9]+),\\s*\"([^\"]*)\"\\}\\s*(,|(\\}\\s*;))$");
  private static final Pattern excPattern = 
    Pattern.compile("^\\{\\s*\"([^\"]*)\",\\s*([0-9]+),\\s*\"([^\"]*)\",\\s*\\{(.*)\\}\\s*\\}\\s*(,|(\\}\\s*;))$");
  
  private static Step parseStep(LineNumberReader r, String header) throws IOException {
    Matcher matcher = headerPattern.matcher(header);
    if (!matcher.find()) {
      throw new RuntimeException("Illegal Step header specified at line " + r.getLineNumber());
    }
    assert matcher.groupCount() == 4;
    String name = matcher.group(1);
    int min = Integer.parseInt(matcher.group(2));
    int type = Integer.parseInt(matcher.group(3));
    String suffixes[] = parseList(matcher.group(4));
    Rule rules[] = parseRules(r, type);
    return new Step(name, rules, min, suffixes);
  }
  
  private static Rule[] parseRules(LineNumberReader r, int type) throws IOException {
    List<Rule> rules = new ArrayList<>();
    String line;
    while ((line = readLine(r)) != null) {
      Matcher matcher = stripPattern.matcher(line);
      if (matcher.matches()) {
        rules.add(new Rule(matcher.group(1), Integer.parseInt(matcher.group(2)), ""));
      } else {
        matcher = repPattern.matcher(line);
        if (matcher.matches()) {
          rules.add(new Rule(matcher.group(1), Integer.parseInt(matcher.group(2)), matcher.group(3)));
        } else {
          matcher = excPattern.matcher(line);
          if (matcher.matches()) {
            if (type == 0) {
              rules.add(new RuleWithSuffixExceptions(matcher.group(1), 
                        Integer.parseInt(matcher.group(2)), 
                        matcher.group(3), 
                        parseList(matcher.group(4))));
            } else {
              rules.add(new RuleWithSetExceptions(matcher.group(1), 
                        Integer.parseInt(matcher.group(2)), 
                        matcher.group(3), 
                        parseList(matcher.group(4))));
            }
          } else {
            throw new RuntimeException("Illegal Step rule specified at line " + r.getLineNumber());
          }
        }
      }
      if (line.endsWith(";"))
        return rules.toArray(new Rule[rules.size()]);
    }
    return null;
  }
  
  private static String[] parseList(String s) {
    if (s.length() == 0)
      return null;
    String list[] = s.split(",");
    for (int i = 0; i < list.length; i++)
      list[i] = parseString(list[i].trim());
    return list;
  }
  
  private static String parseString(String s) {
    return s.substring(1, s.length()-1);
  }
  
  private static String readLine(LineNumberReader r) throws IOException {
    String line = null;
    while ((line = r.readLine()) != null) {
      line = line.trim();
      if (line.length() > 0 && line.charAt(0) != '#')
        return line;
    }
    return line;
  }
}

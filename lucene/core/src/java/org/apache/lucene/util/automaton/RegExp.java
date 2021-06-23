/*
 * dk.brics.automaton
 * 
 * Copyright (c) 2001-2009 Anders Moeller
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.lucene.util.automaton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Regular Expression extension to <code>Automaton</code>.
 * <p>
 * Regular expressions are built from the following abstract syntax:
 * <table border=0 summary="description of regular expression grammar">
 * <tr>
 * <td><i>regexp</i></td>
 * <td>::=</td>
 * <td><i>unionexp</i></td>
 * <td></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td></td>
 * <td></td>
 * <td></td>
 * </tr>
 * 
 * <tr>
 * <td><i>unionexp</i></td>
 * <td>::=</td>
 * <td><i>interexp</i>&nbsp;<tt><b>|</b></tt>&nbsp;<i>unionexp</i></td>
 * <td>(union)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><i>interexp</i></td>
 * <td></td>
 * <td></td>
 * </tr>
 * 
 * <tr>
 * <td><i>interexp</i></td>
 * <td>::=</td>
 * <td><i>concatexp</i>&nbsp;<tt><b>&amp;</b></tt>&nbsp;<i>interexp</i></td>
 * <td>(intersection)</td>
 * <td><small>[OPTIONAL]</small></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><i>concatexp</i></td>
 * <td></td>
 * <td></td>
 * </tr>
 * 
 * <tr>
 * <td><i>concatexp</i></td>
 * <td>::=</td>
 * <td><i>repeatexp</i>&nbsp;<i>concatexp</i></td>
 * <td>(concatenation)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><i>repeatexp</i></td>
 * <td></td>
 * <td></td>
 * </tr>
 * 
 * <tr>
 * <td><i>repeatexp</i></td>
 * <td>::=</td>
 * <td><i>repeatexp</i>&nbsp;<tt><b>?</b></tt></td>
 * <td>(zero or one occurrence)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><i>repeatexp</i>&nbsp;<tt><b>*</b></tt></td>
 * <td>(zero or more occurrences)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><i>repeatexp</i>&nbsp;<tt><b>+</b></tt></td>
 * <td>(one or more occurrences)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><i>repeatexp</i>&nbsp;<tt><b>{</b><i>n</i><b>}</b></tt></td>
 * <td>(<tt><i>n</i></tt> occurrences)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><i>repeatexp</i>&nbsp;<tt><b>{</b><i>n</i><b>,}</b></tt></td>
 * <td>(<tt><i>n</i></tt> or more occurrences)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><i>repeatexp</i>&nbsp;<tt><b>{</b><i>n</i><b>,</b><i>m</i><b>}</b></tt></td>
 * <td>(<tt><i>n</i></tt> to <tt><i>m</i></tt> occurrences, including both)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><i>complexp</i></td>
 * <td></td>
 * <td></td>
 * </tr>
 * 
 * <tr>
 * <td><i>complexp</i></td>
 * <td>::=</td>
 * <td><tt><b>~</b></tt>&nbsp;<i>complexp</i></td>
 * <td>(complement)</td>
 * <td><small>[OPTIONAL]</small></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><i>charclassexp</i></td>
 * <td></td>
 * <td></td>
 * </tr>
 * 
 * <tr>
 * <td><i>charclassexp</i></td>
 * <td>::=</td>
 * <td><tt><b>[</b></tt>&nbsp;<i>charclasses</i>&nbsp;<tt><b>]</b></tt></td>
 * <td>(character class)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><tt><b>[^</b></tt>&nbsp;<i>charclasses</i>&nbsp;<tt><b>]</b></tt></td>
 * <td>(negated character class)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><i>simpleexp</i></td>
 * <td></td>
 * <td></td>
 * </tr>
 * 
 * <tr>
 * <td><i>charclasses</i></td>
 * <td>::=</td>
 * <td><i>charclass</i>&nbsp;<i>charclasses</i></td>
 * <td></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><i>charclass</i></td>
 * <td></td>
 * <td></td>
 * </tr>
 * 
 * <tr>
 * <td><i>charclass</i></td>
 * <td>::=</td>
 * <td><i>charexp</i>&nbsp;<tt><b>-</b></tt>&nbsp;<i>charexp</i></td>
 * <td>(character range, including end-points)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><i>charexp</i></td>
 * <td></td>
 * <td></td>
 * </tr>
 * 
 * <tr>
 * <td><i>simpleexp</i></td>
 * <td>::=</td>
 * <td><i>charexp</i></td>
 * <td></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><tt><b>.</b></tt></td>
 * <td>(any single character)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><tt><b>#</b></tt></td>
 * <td>(the empty language)</td>
 * <td><small>[OPTIONAL]</small></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><tt><b>@</b></tt></td>
 * <td>(any string)</td>
 * <td><small>[OPTIONAL]</small></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><tt><b>"</b></tt>&nbsp;&lt;Unicode string without double-quotes&gt;&nbsp; <tt><b>"</b></tt></td>
 * <td>(a string)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><tt><b>(</b></tt>&nbsp;<tt><b>)</b></tt></td>
 * <td>(the empty string)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><tt><b>(</b></tt>&nbsp;<i>unionexp</i>&nbsp;<tt><b>)</b></tt></td>
 * <td>(precedence override)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><tt><b>&lt;</b></tt>&nbsp;&lt;identifier&gt;&nbsp;<tt><b>&gt;</b></tt></td>
 * <td>(named automaton)</td>
 * <td><small>[OPTIONAL]</small></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><tt><b>&lt;</b><i>n</i>-<i>m</i><b>&gt;</b></tt></td>
 * <td>(numerical interval)</td>
 * <td><small>[OPTIONAL]</small></td>
 * </tr>
 * 
 * <tr>
 * <td><i>charexp</i></td>
 * <td>::=</td>
 * <td>&lt;Unicode character&gt;</td>
 * <td>(a single non-reserved character)</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>|</td>
 * <td><tt><b>\</b></tt>&nbsp;&lt;Unicode character&gt;&nbsp;</td>
 * <td>(a single character)</td>
 * <td></td>
 * </tr>
 * </table>
 * <p>
 * The productions marked <small>[OPTIONAL]</small> are only allowed if
 * specified by the syntax flags passed to the <code>RegExp</code> constructor.
 * The reserved characters used in the (enabled) syntax must be escaped with
 * backslash (<tt><b>\</b></tt>) or double-quotes (<tt><b>"..."</b></tt>). (In
 * contrast to other regexp syntaxes, this is required also in character
 * classes.) Be aware that dash (<tt><b>-</b></tt>) has a special meaning in
 * <i>charclass</i> expressions. An identifier is a string not containing right
 * angle bracket (<tt><b>&gt;</b></tt>) or dash (<tt><b>-</b></tt>). Numerical
 * intervals are specified by non-negative decimal integers and include both end
 * points, and if <tt><i>n</i></tt> and <tt><i>m</i></tt> have the same number
 * of digits, then the conforming strings must have that length (i.e. prefixed
 * by 0's).
 * 
 * @lucene.experimental
 */
public class RegExp {
  
  /**
   * The type of expression represented by a RegExp node.
   */
  public enum Kind {
    /** The union of two expressions */
    REGEXP_UNION, 
    /** A sequence of two expressions */
    REGEXP_CONCATENATION,
    /** The intersection of two expressions */
    REGEXP_INTERSECTION,
    /** An optional expression */
    REGEXP_OPTIONAL,
    /** An expression that repeats */
    REGEXP_REPEAT,
    /** An expression that repeats a minimum number of times*/
    REGEXP_REPEAT_MIN,
    /** An expression that repeats a minimum and maximum number of times*/
    REGEXP_REPEAT_MINMAX,
    /** The complement of an expression */
    REGEXP_COMPLEMENT,
    /** A Character */
    REGEXP_CHAR,
    /** A Character range*/
    REGEXP_CHAR_RANGE,
    /** Any Character allowed*/
    REGEXP_ANYCHAR,
    /** An empty expression*/
    REGEXP_EMPTY,
    /** A string expression*/
    REGEXP_STRING,
    /** Any string allowed */
    REGEXP_ANYSTRING,
    /** An Automaton expression*/
    REGEXP_AUTOMATON,
    /** An Interval expression */
    REGEXP_INTERVAL,
    /** An expression for a pre-defined class e.g. \w */
  }
  
  //-----  Syntax flags ( <= 0xff )  ------
  /**
   * Syntax flag, enables intersection (<tt>&amp;</tt>).
   */
  public static final int INTERSECTION = 0x0001;
  
  /**
   * Syntax flag, enables complement (<tt>~</tt>).
   */
  public static final int COMPLEMENT = 0x0002;
  
  /**
   * Syntax flag, enables empty language (<tt>#</tt>).
   */
  public static final int EMPTY = 0x0004;
  
  /**
   * Syntax flag, enables anystring (<tt>@</tt>).
   */
  public static final int ANYSTRING = 0x0008;
  
  /**
   * Syntax flag, enables named automata (<tt>&lt;</tt>identifier<tt>&gt;</tt>).
   */
  public static final int AUTOMATON = 0x0010;
  
  /**
   * Syntax flag, enables numerical intervals (
   * <tt>&lt;<i>n</i>-<i>m</i>&gt;</tt>).
   */
  public static final int INTERVAL = 0x0020;
  
  /**
   * Syntax flag, enables all optional regexp syntax.
   */
  public static final int ALL = 0xff;
  
  /**
   * Syntax flag, enables no optional regexp syntax.
   */
  public static final int NONE = 0x0000;

  //-----  Matching flags ( > 0xff )  ------

  /**
   * Allows case insensitive matching of ASCII characters.
   */
  public static final int ASCII_CASE_INSENSITIVE = 0x0100;   
  
  
  //Immutable parsed state
  /**
   * The type of expression
   */
  public final Kind kind;
  /**
   * Child expressions held by a container type expression
   */
  public final RegExp exp1, exp2;
  /**
   * String expression
   */
  public final String s;
  /**
   *  Character expression
   */
  public final int c;
  /**
   * Limits for repeatable type expressions
   */
  public final int min, max, digits;
  /**
   * Extents for range type expressions
   */
  public final int from, to;

  // Parser variables
  private final String originalString;
  final int flags;
  int pos;
    
  /**
   * Constructs new <code>RegExp</code> from a string. Same as
   * <code>RegExp(s, ALL)</code>.
   * 
   * @param s regexp string
   * @exception IllegalArgumentException if an error occurred while parsing the
   *              regular expression
   */
  public RegExp(String s) throws IllegalArgumentException {
    this(s, ALL);
  }
  
  /**
   * Constructs new <code>RegExp</code> from a string.
   * 
   * @param s regexp string
   * @param syntax_flags boolean 'or' of optional syntax constructs to be
   *          enabled
   * @exception IllegalArgumentException if an error occurred while parsing the
   *              regular expression
   */
  public RegExp(String s, int syntax_flags) throws IllegalArgumentException {
    this(s, syntax_flags, 0);
  }
  
  /**
   * Constructs new <code>RegExp</code> from a string.
   * 
   * @param s regexp string
   * @param syntax_flags boolean 'or' of optional syntax constructs to be
   *          enabled
   * @param match_flags boolean 'or' of match behavior options such as case insensitivity
   * @exception IllegalArgumentException if an error occurred while parsing the
   *              regular expression
   */
  public RegExp(String s, int syntax_flags, int match_flags) throws IllegalArgumentException {
    originalString = s;
    if (syntax_flags >  ALL) {
      throw new IllegalArgumentException("Illegal syntax flag");
    }
    
    if (match_flags > 0 && match_flags <= ALL) {
      throw new IllegalArgumentException("Illegal match flag");
    }
    flags = syntax_flags | match_flags;
    RegExp e;
    if (s.length() == 0) e = makeString(flags, "");
    else {
      e = parseUnionExp();
      if (pos < originalString.length()) throw new IllegalArgumentException(
          "end-of-string expected at position " + pos);
    }
    kind = e.kind;
    exp1 = e.exp1;
    exp2 = e.exp2;
    this.s = e.s;
    c = e.c;
    min = e.min;
    max = e.max;
    digits = e.digits;
    from = e.from;
    to = e.to;
  }
  
  RegExp(int flags, Kind kind, RegExp exp1, RegExp exp2, String s, int c, int min, int max, int digits, int from, int to){
    this.originalString = null;
    this.kind = kind;
    this.flags = flags;
    this.exp1 = exp1;
    this.exp2 = exp2;
    this.s = s;
    this.c = c;
    this.min = min;
    this.max = max;
    this.digits = digits;
    this.from = from;
    this.to = to;
  }

  // Simplified construction of container nodes
  static RegExp newContainerNode(int flags, Kind kind, RegExp exp1, RegExp exp2) {
    return new RegExp(flags, kind, exp1, exp2, null, 0, 0, 0, 0, 0, 0);
  }

  // Simplified construction of repeating nodes
  static RegExp newRepeatingNode(int flags, Kind kind, RegExp exp,  int min, int max) {
    return new RegExp(flags, kind, exp, null, null, 0, min, max, 0, 0, 0);
  }  
  
  
  // Simplified construction of leaf nodes
  static RegExp newLeafNode(int flags, Kind kind, String s, int c, int min, int max, int digits, int from, int to) {
    return new RegExp(flags, kind, null, null, s, c, min, max, digits, from, to);
  }  

  /**
   * Constructs new <code>Automaton</code> from this <code>RegExp</code>. Same
   * as <code>toAutomaton(null)</code> (empty automaton map).
   */
  public Automaton toAutomaton() {
    return toAutomaton(null, null, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
  }

  /**
   * Constructs new <code>Automaton</code> from this <code>RegExp</code>. The
   * constructed automaton is minimal and deterministic and has no transitions
   * to dead states.
   * 
   * @param determinizeWorkLimit maximum effort to spend while determinizing the automata. If
   *     determinizing the automata would require more than this effort,
   *     TooComplexToDeterminizeException is thrown. Higher numbers require more space but can
   *     process more complex regexes. Use {@link Operations#DEFAULT_DETERMINIZE_WORK_LIMIT} as a
   *     decent default if you don't otherwise know what to specify.
   * @exception IllegalArgumentException if this regular expression uses a named
   *     identifier that is not available from the automaton provider
   * @exception TooComplexToDeterminizeException if determinizing this regexp requires more effort
   *     than determinizeWorkLimit states
   */
  public Automaton toAutomaton(int determinizeWorkLimit)
      throws IllegalArgumentException, TooComplexToDeterminizeException {
    return toAutomaton(null, null, determinizeWorkLimit);
  }

  /**
   * Constructs new <code>Automaton</code> from this <code>RegExp</code>. The
   * constructed automaton is minimal and deterministic and has no transitions
   * to dead states.
   * 
   * @param automaton_provider provider of automata for named identifiers
   * @param determinizeWorkLimit maximum effort to spend while determinizing the automata. If
   *     determinizing the automata would require more than this effort,
   *     TooComplexToDeterminizeException is thrown. Higher numbers require more space but can
   *     process more complex regexes. Use {@link Operations#DEFAULT_DETERMINIZE_WORK_LIMIT} as a
   *     decent default if you don't otherwise know what to specify.
   * @exception IllegalArgumentException if this regular expression uses a named
   *   identifier that is not available from the automaton provider
   * @exception TooComplexToDeterminizeException if determinizing this regexp requires more effort
   *     than determinizeWorkLimit states
   */
  public Automaton toAutomaton(AutomatonProvider automaton_provider,
      int determinizeWorkLimit) throws IllegalArgumentException,
      TooComplexToDeterminizeException {
    return toAutomaton(null, automaton_provider, determinizeWorkLimit);
  }
  
  /**
   * Constructs new <code>Automaton</code> from this <code>RegExp</code>. The
   * constructed automaton is minimal and deterministic and has no transitions
   * to dead states.
   * 
   * @param automata a map from automaton identifiers to automata (of type
   *          <code>Automaton</code>).
   * @param determinizeWorkLimit maximum effort to spend while determinizing the automata. If
   *     determinizing the automata would require more than this effort,
   *     TooComplexToDeterminizeException is thrown. Higher numbers require more space but can
   *     process more complex regexes.
   * @exception IllegalArgumentException if this regular expression uses a named
   *   identifier that does not occur in the automaton map
   * @exception TooComplexToDeterminizeException if determinizing this regexp requires more effort
   *     than determinizeWorkLimit states
   */
  public Automaton toAutomaton(Map<String,Automaton> automata,
      int determinizeWorkLimit) throws IllegalArgumentException,
      TooComplexToDeterminizeException {
    return toAutomaton(automata, null, determinizeWorkLimit);
  }

  private Automaton toAutomaton(Map<String,Automaton> automata,
      AutomatonProvider automaton_provider, int determinizeWorkLimit)
      throws IllegalArgumentException, TooComplexToDeterminizeException {
    try {
      return toAutomatonInternal(automata, automaton_provider,
        determinizeWorkLimit);
    } catch (TooComplexToDeterminizeException e) {
      throw new TooComplexToDeterminizeException(this, e);
    }
  }

  private Automaton toAutomatonInternal(Map<String,Automaton> automata,
      AutomatonProvider automaton_provider, int determinizeWorkLimit)
      throws IllegalArgumentException {
    List<Automaton> list;
    Automaton a = null;
    switch (kind) {
      case REGEXP_UNION:
        list = new ArrayList<>();
        findLeaves(exp1, Kind.REGEXP_UNION, list, automata, automaton_provider,
          determinizeWorkLimit);
        findLeaves(exp2, Kind.REGEXP_UNION, list, automata, automaton_provider,
          determinizeWorkLimit);
        a = Operations.union(list);
        a = MinimizationOperations.minimize(a, determinizeWorkLimit);
        break;
      case REGEXP_CONCATENATION:
        list = new ArrayList<>();
        findLeaves(exp1, Kind.REGEXP_CONCATENATION, list, automata,
            automaton_provider, determinizeWorkLimit);
        findLeaves(exp2, Kind.REGEXP_CONCATENATION, list, automata,
            automaton_provider, determinizeWorkLimit);
        a = Operations.concatenate(list);
        a = MinimizationOperations.minimize(a, determinizeWorkLimit);
        break;
      case REGEXP_INTERSECTION:
        a = Operations.intersection(
            exp1.toAutomatonInternal(
              automata, automaton_provider, determinizeWorkLimit),
            exp2.toAutomatonInternal(
              automata, automaton_provider, determinizeWorkLimit));
        a = MinimizationOperations.minimize(a, determinizeWorkLimit);
        break;
      case REGEXP_OPTIONAL:
        a = Operations.optional(exp1.toAutomatonInternal(automata,
          automaton_provider, determinizeWorkLimit));
        a = MinimizationOperations.minimize(a, determinizeWorkLimit);
        break;
      case REGEXP_REPEAT:
        a = Operations.repeat(exp1.toAutomatonInternal(
          automata, automaton_provider, determinizeWorkLimit));
        a = MinimizationOperations.minimize(a, determinizeWorkLimit);
        break;
      case REGEXP_REPEAT_MIN:
        a = exp1.toAutomatonInternal(automata, automaton_provider, determinizeWorkLimit);
        int minNumStates = (a.getNumStates() - 1) * min;
        if (minNumStates > determinizeWorkLimit) {
          throw new TooComplexToDeterminizeException(a, minNumStates);
        }
        a = Operations.repeat(a, min);
        a = MinimizationOperations.minimize(a, determinizeWorkLimit);
        break;
      case REGEXP_REPEAT_MINMAX:
        a = exp1.toAutomatonInternal(automata, automaton_provider, determinizeWorkLimit);
        int minMaxNumStates = (a.getNumStates() - 1) * max;
        if (minMaxNumStates > determinizeWorkLimit) {
          throw new TooComplexToDeterminizeException(a, minMaxNumStates);
        }
        a = Operations.repeat(a, min, max);
        break;
      case REGEXP_COMPLEMENT:
        a = Operations.complement(
          exp1.toAutomatonInternal(automata, automaton_provider,
            determinizeWorkLimit),
          determinizeWorkLimit);
        a = MinimizationOperations.minimize(a, determinizeWorkLimit);
        break;
      case REGEXP_CHAR:
        if (check(ASCII_CASE_INSENSITIVE)) {
          a = toCaseInsensitiveChar(c, determinizeWorkLimit);
        } else {
          a = Automata.makeChar(c);          
        }
        break;
      case REGEXP_CHAR_RANGE:
        a = Automata.makeCharRange(from, to);
        break;
      case REGEXP_ANYCHAR:
        a = Automata.makeAnyChar();
        break;
      case REGEXP_EMPTY:
        a = Automata.makeEmpty();
        break;
      case REGEXP_STRING:
        if (check(ASCII_CASE_INSENSITIVE)) {
          a = toCaseInsensitiveString(determinizeWorkLimit);
        } else {
          a = Automata.makeString(s);
        }
        break;
      case REGEXP_ANYSTRING:
        a = Automata.makeAnyString();
        break;
      case REGEXP_AUTOMATON:
        Automaton aa = null;
        if (automata != null) {
          aa = automata.get(s);
        }
        if (aa == null && automaton_provider != null) {
          try {
            aa = automaton_provider.getAutomaton(s);
          } catch (IOException e) {
            throw new IllegalArgumentException(e);
          }
        }
        if (aa == null) {
          throw new IllegalArgumentException("'" + s + "' not found");
        }
        a = aa;
        break;
      case REGEXP_INTERVAL:
        a = Automata.makeDecimalInterval(min, max, digits);
        break;
    }
    return a;
  }
  
  
  private Automaton toCaseInsensitiveChar(int codepoint, int determinizeWorkLimit) {
    Automaton case1 = Automata.makeChar(codepoint);
    // For now we only work with ASCII characters
    if (codepoint > 128) {
      return case1;
    }
    int altCase = Character.isLowerCase(codepoint) ? Character.toUpperCase(codepoint) : Character.toLowerCase(codepoint);
    Automaton result;
    if (altCase != codepoint) {
      result = Operations.union(case1, Automata.makeChar(altCase));
      result = MinimizationOperations.minimize(result, determinizeWorkLimit);          
    } else {
      result = case1;                      
    }          
    return result;
  }
  
  private Automaton toCaseInsensitiveString(int determinizeWorkLimit) {
    List<Automaton> list = new ArrayList<>();
    
    Iterator<Integer> iter = s.codePoints().iterator();
    while (iter.hasNext()) {
      list.add(toCaseInsensitiveChar(iter.next(), determinizeWorkLimit));
    }
    Automaton a = Operations.concatenate(list);
    a = MinimizationOperations.minimize(a, determinizeWorkLimit);
    return a;
  }    
  
  private void findLeaves(RegExp exp, Kind kind, List<Automaton> list,
      Map<String,Automaton> automata, AutomatonProvider automaton_provider,
      int determinizeWorkLimit) {
    if (exp.kind == kind) {
      findLeaves(exp.exp1, kind, list, automata, automaton_provider,
        determinizeWorkLimit);
      findLeaves(exp.exp2, kind, list, automata, automaton_provider,
        determinizeWorkLimit);
    } else {
      list.add(exp.toAutomatonInternal(automata, automaton_provider, 
        determinizeWorkLimit));
    }
  }

  /**
   * The string that was used to construct the regex.  Compare to toString.
   */
  public String getOriginalString() {
    return originalString;
  }

  /**
   * Constructs string from parsed regular expression.
   */
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    toStringBuilder(b);
    return b.toString();
  }
  
  void toStringBuilder(StringBuilder b) {
    switch (kind) {
      case REGEXP_UNION:
        b.append("(");
        exp1.toStringBuilder(b);
        b.append("|");
        exp2.toStringBuilder(b);
        b.append(")");
        break;
      case REGEXP_CONCATENATION:
        exp1.toStringBuilder(b);
        exp2.toStringBuilder(b);
        break;
      case REGEXP_INTERSECTION:
        b.append("(");
        exp1.toStringBuilder(b);
        b.append("&");
        exp2.toStringBuilder(b);
        b.append(")");
        break;
      case REGEXP_OPTIONAL:
        b.append("(");
        exp1.toStringBuilder(b);
        b.append(")?");
        break;
      case REGEXP_REPEAT:
        b.append("(");
        exp1.toStringBuilder(b);
        b.append(")*");
        break;
      case REGEXP_REPEAT_MIN:
        b.append("(");
        exp1.toStringBuilder(b);
        b.append("){").append(min).append(",}");
        break;
      case REGEXP_REPEAT_MINMAX:
        b.append("(");
        exp1.toStringBuilder(b);
        b.append("){").append(min).append(",").append(max).append("}");
        break;
      case REGEXP_COMPLEMENT:
        b.append("~(");
        exp1.toStringBuilder(b);
        b.append(")");
        break;
      case REGEXP_CHAR:
        b.append("\\").appendCodePoint(c);
        break;
      case REGEXP_CHAR_RANGE:
        b.append("[\\").appendCodePoint(from).append("-\\").appendCodePoint(to).append("]");
        break;
      case REGEXP_ANYCHAR:
        b.append(".");
        break;
      case REGEXP_EMPTY:
        b.append("#");
        break;
      case REGEXP_STRING:
        b.append("\"").append(s).append("\"");
        break;
      case REGEXP_ANYSTRING:
        b.append("@");
        break;
      case REGEXP_AUTOMATON:
        b.append("<").append(s).append(">");
        break;
      case REGEXP_INTERVAL:
        String s1 = Integer.toString(min);
        String s2 = Integer.toString(max);
        b.append("<");
        if (digits > 0) for (int i = s1.length(); i < digits; i++)
          b.append('0');
        b.append(s1).append("-");
        if (digits > 0) for (int i = s2.length(); i < digits; i++)
          b.append('0');
        b.append(s2).append(">");
        break;
    }
  }

  /**
   * Like to string, but more verbose (shows the higherchy more clearly).
   */
  public String toStringTree() {
    StringBuilder b = new StringBuilder();
    toStringTree(b, "");
    return b.toString();
  }

  void toStringTree(StringBuilder b, String indent) {
    switch (kind) {
      // binary
      case REGEXP_UNION:
      case REGEXP_CONCATENATION:
      case REGEXP_INTERSECTION:
        b.append(indent);
        b.append(kind);
        b.append('\n');
        exp1.toStringTree(b, indent + "  ");
        exp2.toStringTree(b, indent + "  ");
        break;
      // unary
      case REGEXP_OPTIONAL:
      case REGEXP_REPEAT:
      case REGEXP_COMPLEMENT:
        b.append(indent);
        b.append(kind);
        b.append('\n');
        exp1.toStringTree(b, indent + "  ");
        break;
      case REGEXP_REPEAT_MIN:
        b.append(indent);
        b.append(kind);
        b.append(" min=");
        b.append(min);
        b.append('\n');
        exp1.toStringTree(b, indent + "  ");
        break;
      case REGEXP_REPEAT_MINMAX:
        b.append(indent);
        b.append(kind);
        b.append(" min=");
        b.append(min);
        b.append(" max=");
        b.append(max);
        b.append('\n');
        exp1.toStringTree(b, indent + "  ");
        break;
      case REGEXP_CHAR:
        b.append(indent);
        b.append(kind);
        b.append(" char=");
        b.appendCodePoint(c);
        b.append('\n');
        break;
      case REGEXP_CHAR_RANGE:
        b.append(indent);
        b.append(kind);
        b.append(" from=");
        b.appendCodePoint(from);
        b.append(" to=");
        b.appendCodePoint(to);
        b.append('\n');
        break;
      case REGEXP_ANYCHAR:
      case REGEXP_EMPTY:
        b.append(indent);
        b.append(kind);
        b.append('\n');
        break;
      case REGEXP_STRING:
        b.append(indent);
        b.append(kind);
        b.append(" string=");
        b.append(s);
        b.append('\n');
        break;
      case REGEXP_ANYSTRING:
        b.append(indent);
        b.append(kind);
        b.append('\n');
        break;
      case REGEXP_AUTOMATON:
        b.append(indent);
        b.append(kind);
        b.append('\n');
        break;
      case REGEXP_INTERVAL:
        b.append(indent);
        b.append(kind);
        String s1 = Integer.toString(min);
        String s2 = Integer.toString(max);
        b.append("<");
        if (digits > 0) for (int i = s1.length(); i < digits; i++)
          b.append('0');
        b.append(s1).append("-");
        if (digits > 0) for (int i = s2.length(); i < digits; i++)
          b.append('0');
        b.append(s2).append(">");
        b.append('\n');
        break;
    }
  }

  /**
   * Returns set of automaton identifiers that occur in this regular expression.
   */
  public Set<String> getIdentifiers() {
    HashSet<String> set = new HashSet<>();
    getIdentifiers(set);
    return set;
  }
  
  void getIdentifiers(Set<String> set) {
    switch (kind) {
      case REGEXP_UNION:
      case REGEXP_CONCATENATION:
      case REGEXP_INTERSECTION:
        exp1.getIdentifiers(set);
        exp2.getIdentifiers(set);
        break;
      case REGEXP_OPTIONAL:
      case REGEXP_REPEAT:
      case REGEXP_REPEAT_MIN:
      case REGEXP_REPEAT_MINMAX:
      case REGEXP_COMPLEMENT:
        exp1.getIdentifiers(set);
        break;
      case REGEXP_AUTOMATON:
        set.add(s);
        break;
      default:
    }
  }
  
  static RegExp makeUnion(int flags, RegExp exp1, RegExp exp2) {
    return newContainerNode(flags, Kind.REGEXP_UNION, exp1, exp2);
  }
  
  static RegExp makeConcatenation(int flags, RegExp exp1, RegExp exp2) {
    if ((exp1.kind == Kind.REGEXP_CHAR || exp1.kind == Kind.REGEXP_STRING)
        && (exp2.kind == Kind.REGEXP_CHAR || exp2.kind == Kind.REGEXP_STRING)) return makeString(
            flags, exp1, exp2);
    RegExp rexp1, rexp2;
    if (exp1.kind == Kind.REGEXP_CONCATENATION
        && (exp1.exp2.kind == Kind.REGEXP_CHAR || exp1.exp2.kind == Kind.REGEXP_STRING)
        && (exp2.kind == Kind.REGEXP_CHAR || exp2.kind == Kind.REGEXP_STRING)) {
      rexp1 = exp1.exp1;
      rexp2 = makeString(flags, exp1.exp2, exp2);
    } else if ((exp1.kind == Kind.REGEXP_CHAR || exp1.kind == Kind.REGEXP_STRING)
        && exp2.kind == Kind.REGEXP_CONCATENATION
        && (exp2.exp1.kind == Kind.REGEXP_CHAR || exp2.exp1.kind == Kind.REGEXP_STRING)) {
      rexp1 = makeString(flags, exp1, exp2.exp1);
      rexp2 = exp2.exp2;
    } else {
      rexp1 = exp1;
      rexp2 = exp2;
    }
    return newContainerNode(flags, Kind.REGEXP_CONCATENATION, rexp1, rexp2);
  }
  
  static private RegExp makeString(int flags, RegExp exp1, RegExp exp2) {
    StringBuilder b = new StringBuilder();
    if (exp1.kind == Kind.REGEXP_STRING) b.append(exp1.s);
    else b.appendCodePoint(exp1.c);
    if (exp2.kind == Kind.REGEXP_STRING) b.append(exp2.s);
    else b.appendCodePoint(exp2.c);
    return makeString(flags, b.toString());
  }
  
  static RegExp makeIntersection(int flags, RegExp exp1, RegExp exp2) {
    return newContainerNode(flags, Kind.REGEXP_INTERSECTION, exp1, exp2);
  }
  
  static RegExp makeOptional(int flags, RegExp exp) {
    return newContainerNode(flags, Kind.REGEXP_OPTIONAL, exp, null);
  }
  
  static RegExp makeRepeat(int flags, RegExp exp) {
    return newContainerNode(flags, Kind.REGEXP_REPEAT, exp, null);
  }
  
  static RegExp makeRepeat(int flags, RegExp exp, int min) {
    return newRepeatingNode(flags, Kind.REGEXP_REPEAT_MIN, exp, min, 0);
  }
  
  static RegExp makeRepeat(int flags, RegExp exp, int min, int max) {
    return newRepeatingNode(flags, Kind.REGEXP_REPEAT_MINMAX, exp, min, max);
  }
  
  static RegExp makeComplement(int flags, RegExp exp) {
    return newContainerNode(flags, Kind.REGEXP_COMPLEMENT, exp, null);
  }
  
  static RegExp makeChar(int flags, int c) {
    return newLeafNode(flags, Kind.REGEXP_CHAR, null, c, 0, 0, 0, 0, 0);
  }
  
  static RegExp makeCharRange(int flags, int from, int to) {
    if (from > to) 
      throw new IllegalArgumentException("invalid range: from (" + from + ") cannot be > to (" + to + ")");
    return newLeafNode(flags, Kind.REGEXP_CHAR_RANGE, null, 0, 0, 0, 0, from, to);
  }
  
  static RegExp makeAnyChar(int flags) {
    return newContainerNode(flags, Kind.REGEXP_ANYCHAR, null, null);
  }
  
  static RegExp makeEmpty(int flags) {
    return newContainerNode(flags, Kind.REGEXP_EMPTY, null, null);
  }
  
  static RegExp makeString(int flags, String s) {
    return newLeafNode(flags, Kind.REGEXP_STRING, s, 0, 0, 0, 0, 0, 0);
  }
  
  static RegExp makeAnyString(int flags) {
    return newContainerNode(flags, Kind.REGEXP_ANYSTRING, null, null);
  }
  
  static RegExp makeAutomaton(int flags, String s) {
    return newLeafNode(flags, Kind.REGEXP_AUTOMATON, s, 0, 0, 0, 0, 0, 0);
  }
  
  static RegExp makeInterval(int flags, int min, int max, int digits) {
  return newLeafNode(flags, Kind.REGEXP_INTERVAL, null, 0, min, max, digits, 0, 0);
  }
  
  private boolean peek(String s) {
    return more() && s.indexOf(originalString.codePointAt(pos)) != -1;
  }
  
  private boolean match(int c) {
    if (pos >= originalString.length()) return false;
    if (originalString.codePointAt(pos) == c) {
      pos += Character.charCount(c);
      return true;
    }
    return false;
  }
  
  private boolean more() {
    return pos < originalString.length();
  }
  
  private int next() throws IllegalArgumentException {
    if (!more()) throw new IllegalArgumentException("unexpected end-of-string");
    int ch = originalString.codePointAt(pos);
    pos += Character.charCount(ch);
    return ch;
  }
  
  private boolean check(int flag) {
    return (flags & flag) != 0;
  }
  
  final RegExp parseUnionExp() throws IllegalArgumentException {
    RegExp e = parseInterExp();
    if (match('|')) e = makeUnion(flags, e, parseUnionExp());
    return e;
  }
  
  final RegExp parseInterExp() throws IllegalArgumentException {
    RegExp e = parseConcatExp();
    if (check(INTERSECTION) && match('&')) e = makeIntersection(flags, e,
        parseInterExp());
    return e;
  }
  
  final RegExp parseConcatExp() throws IllegalArgumentException {
    RegExp e = parseRepeatExp();
    if (more() && !peek(")|") && (!check(INTERSECTION) || !peek("&"))) e = makeConcatenation(
        flags, e, parseConcatExp());
    return e;
  }
  
  final RegExp parseRepeatExp() throws IllegalArgumentException {
    RegExp e = parseComplExp();
    while (peek("?*+{")) {
      if (match('?')) e = makeOptional(flags, e);
      else if (match('*')) e = makeRepeat(flags, e);
      else if (match('+')) e = makeRepeat(flags, e, 1);
      else if (match('{')) {
        int start = pos;
        while (peek("0123456789"))
          next();
        if (start == pos) throw new IllegalArgumentException(
            "integer expected at position " + pos);
        int n = Integer.parseInt(originalString.substring(start, pos));
        int m = -1;
        if (match(',')) {
          start = pos;
          while (peek("0123456789"))
            next();
          if (start != pos) m = Integer.parseInt(
            originalString.substring(start, pos));
        } else m = n;
        if (!match('}')) throw new IllegalArgumentException(
            "expected '}' at position " + pos);
        if (m == -1) e = makeRepeat(flags, e, n);
        else e = makeRepeat(flags, e, n, m);
      }
    }
    return e;
  }
  
  final RegExp parseComplExp() throws IllegalArgumentException {
    if (check(COMPLEMENT) && match('~')) return makeComplement(flags, parseComplExp());
    else return parseCharClassExp();
  }
  
  final RegExp parseCharClassExp() throws IllegalArgumentException {
    if (match('[')) {
      boolean negate = false;
      if (match('^')) negate = true;
      RegExp e = parseCharClasses();
      if (negate) e = makeIntersection(flags, makeAnyChar(flags), makeComplement(flags, e));
      if (!match(']')) throw new IllegalArgumentException(
          "expected ']' at position " + pos);
      return e;
    } else return parseSimpleExp();
  }
  
  final RegExp parseCharClasses() throws IllegalArgumentException {
    RegExp e = parseCharClass();
    while (more() && !peek("]"))
      e = makeUnion(flags, e, parseCharClass());
    return e;
  }
  
  final RegExp parseCharClass() throws IllegalArgumentException {
    int c = parseCharExp();
    if (match('-')) return makeCharRange(flags, c, parseCharExp());
    else return makeChar(flags, c);
  }
  
  final RegExp parseSimpleExp() throws IllegalArgumentException {
    if (match('.')) return makeAnyChar(flags);
    else if (check(EMPTY) && match('#')) return makeEmpty(flags);
    else if (check(ANYSTRING) && match('@')) return makeAnyString(flags);
    else if (match('"')) {
      int start = pos;
      while (more() && !peek("\""))
        next();
      if (!match('"')) throw new IllegalArgumentException(
          "expected '\"' at position " + pos);
      return makeString(flags, originalString.substring(start, pos - 1));
    } else if (match('(')) {
      if (match(')')) return makeString(flags, "");
      RegExp e = parseUnionExp();
      if (!match(')')) throw new IllegalArgumentException(
          "expected ')' at position " + pos);
      return e;
    } else if ((check(AUTOMATON) || check(INTERVAL)) && match('<')) {
      int start = pos;
      while (more() && !peek(">"))
        next();
      if (!match('>')) throw new IllegalArgumentException(
          "expected '>' at position " + pos);
      String s = originalString.substring(start, pos - 1);
      int i = s.indexOf('-');
      if (i == -1) {
        if (!check(AUTOMATON)) throw new IllegalArgumentException(
            "interval syntax error at position " + (pos - 1));
        return makeAutomaton(flags, s);
      } else {
        if (!check(INTERVAL)) throw new IllegalArgumentException(
            "illegal identifier at position " + (pos - 1));
        try {
          if (i == 0 || i == s.length() - 1 || i != s.lastIndexOf('-')) throw new NumberFormatException();
          String smin = s.substring(0, i);
          String smax = s.substring(i + 1, s.length());
          int imin = Integer.parseInt(smin);
          int imax = Integer.parseInt(smax);
          int digits;
          if (smin.length() == smax.length()) digits = smin.length();
          else digits = 0;
          if (imin > imax) {
            int t = imin;
            imin = imax;
            imax = t;
          }
          return makeInterval(flags, imin, imax, digits);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "interval syntax error at position " + (pos - 1));
        }
      }
    } else return makeChar(flags, parseCharExp());
  }
  
  final int parseCharExp() throws IllegalArgumentException {
    match('\\');
    return next();
  }
}

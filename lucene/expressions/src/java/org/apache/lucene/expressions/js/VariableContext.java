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
package org.apache.lucene.expressions.js;


import java.util.ArrayList;
import java.util.List;

/**
 * A helper to parse the context of a variable name, which is the base variable, followed by the
 * sequence of array (integer or string indexed) and member accesses.
 */
public class VariableContext {

  /**
   * Represents what a piece of a variable does.
   */
  public static enum Type {
    /**
     * A member of the previous context (ie "dot" access).
     */ 
    MEMBER,

    /**
     * Brackets containing a string as the "index".
     */ 
    STR_INDEX,

    /**
     * Brackets containing an integer index (ie an array).
     */
    INT_INDEX,

    /**
     * Parenthesis represent a member method to be called.
     */
    METHOD
  }

  /**
   * The type of this piece of a variable.
   */ 
  public final Type type;

  /**
   * The text of this piece of the variable. Used for {@link Type#MEMBER} and {@link Type#STR_INDEX} types.
   */ 
  public final String text;

  /**
   * The integer value for this piece of the variable. Used for {@link Type#INT_INDEX}.
   */ 
  public final int integer;

  private VariableContext(Type c, String s, int i) {
    type = c;
    text = s;
    integer = i;
  }

  /**
   * Parses a normalized javascript variable. All strings in the variable should be single quoted,
   * and no spaces (except possibly within strings).
   */
  public static final VariableContext[] parse(String variable) {
    char[] text = variable.toCharArray();
    List<VariableContext> contexts = new ArrayList<>();
    int i = addMember(text, 0, contexts); // base variable is a "member" of the global namespace
    while (i < text.length) {
      if (text[i] == '[') {
        if (text[++i] == '\'') {
          i = addStringIndex(text, i, contexts);
        } else {
          i = addIntIndex(text, i, contexts);
        }
        ++i; // move past end bracket
      } else { // text[i] == '.', ie object member
        i = addMember(text, i + 1, contexts);
      }
    }
    return contexts.toArray(new VariableContext[contexts.size()]);
  }

  // i points to start of member name
  private static int addMember(final char[] text, int i, List<VariableContext> contexts) {
    int j = i + 1;
    while (j < text.length && text[j] != '[' && text[j] != '.' && text[j] != '(') ++j; // find first array, member access, or method call
    if (j + 1 < text.length && text[j] == '(' && text[j + 1] == ')') {
      contexts.add(new VariableContext(Type.METHOD, new String(text, i, j - i), -1));
      j += 2; //move past the parenthesis
    } else {
      contexts.add(new VariableContext(Type.MEMBER, new String(text, i, j - i), -1));
    }
    return j;
  }

  // i points to start of single quoted index
  private static int addStringIndex(final char[] text, int i, List<VariableContext> contexts) {
    ++i; // move past quote
    int j = i;
    while (text[j] != '\'') { // find end of single quoted string
      if (text[j] == '\\') ++j; // skip over escapes
      ++j;
    }
    StringBuffer buf = new StringBuffer(j - i); // space for string, without end quote
    while (i < j) { // copy string to buffer (without begin/end quotes)
      if (text[i] == '\\') ++i; // unescape escapes
      buf.append(text[i]);
      ++i;
    }
    contexts.add(new VariableContext(Type.STR_INDEX, buf.toString(), -1));
    return j + 1; // move past quote, return end bracket location
  }

  // i points to start of integer index
  private static int addIntIndex(final char[] text, int i, List<VariableContext> contexts) {
    int j = i + 1;
    while (text[j] != ']') ++j; // find end of array access
    int index = Integer.parseInt(new String(text, i, j - i));
    contexts.add(new VariableContext(Type.INT_INDEX, null, index));
    return j ;
  }
}

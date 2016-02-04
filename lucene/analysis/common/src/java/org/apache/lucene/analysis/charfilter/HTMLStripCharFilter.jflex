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
package org.apache.lucene.analysis.charfilter;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.util.CharArrayMap;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.OpenStringBuilder;

/**
 * A CharFilter that wraps another Reader and attempts to strip out HTML constructs.
 */
@SuppressWarnings("fallthrough")
%%

%unicode 6.3
%apiprivate
%type int
%final
%public
%char
%function nextChar
%class HTMLStripCharFilter
%extends BaseCharFilter
%xstate AMPERSAND, NUMERIC_CHARACTER, CHARACTER_REFERENCE_TAIL
%xstate LEFT_ANGLE_BRACKET, BANG, COMMENT, SCRIPT, SCRIPT_COMMENT
%xstate LEFT_ANGLE_BRACKET_SLASH, LEFT_ANGLE_BRACKET_SPACE, CDATA
%xstate SERVER_SIDE_INCLUDE, SINGLE_QUOTED_STRING, DOUBLE_QUOTED_STRING
%xstate END_TAG_TAIL_INCLUDE, END_TAG_TAIL_EXCLUDE, END_TAG_TAIL_SUBSTITUTE
%xstate START_TAG_TAIL_INCLUDE, START_TAG_TAIL_EXCLUDE, START_TAG_TAIL_SUBSTITUTE
%xstate STYLE, STYLE_COMMENT

// From XML 1.0 <http://www.w3.org/TR/xml/>:
//
//    [4]  NameStartChar ::= ":" | [A-Z] | "_" | [a-z] | [...]
//    [4a] NameChar      ::= NameStartChar | "-" | "." | [0-9] | [...]
//    [5]  Name          ::= NameStartChar (NameChar)*
//
// From UAX #31: Unicode Identifier and Pattern Syntax
// <http://unicode.org/reports/tr31/>:
//
//    D1. Default Identifier Syntax
//
//        <identifier> := <ID_Start> <ID_Continue>*
//
Name = [:_\p{ID_Start}] [-.:_\p{ID_Continue}]*

// From Apache httpd mod_include documentation
// <http://httpd.apache.org/docs/current/mod/mod_include.html>:
//
// Basic Elements
//
//    The document is parsed as an HTML document, with special commands
//    embedded as SGML comments. A command has the syntax:
//
//       <!--#element attribute=value attribute=value ... -->
//
//    The value will often be enclosed in double quotes, but single quotes (')
//    and backticks (`) are also possible. Many commands only allow a single
//    attribute-value pair. Note that the comment terminator (-->) should be
//    preceded by whitespace to ensure that it isn't considered part of an SSI
//    token. Note that the leading <!--# is one token and may not contain any
//    whitespaces.
//

EventAttributeSuffixes = ( [aA][bB][oO][rR][tT]                 |
                           [bB][lL][uU][rR]                     |
                           [cC][hH][aA][nN][gG][eE]             |
                           [cC][lL][iI][cC][kK]                 |
                           [dD][bB][lL][cC][lL][iI][cC][kK]     |
                           [eE][rR][rR][oO][rR]                 |
                           [fF][oO][cC][uU][sS]                 |
                           [kK][eE][yY][dD][oO][wW][nN]         |
                           [kK][eE][yY][pP][rR][eE][sS][sS]     |
                           [kK][eE][yY][uU][pP]                 |
                           [lL][oO][aA][dD]                     |
                           [mM][oO][uU][sS][eE][dD][oO][wW][nN] |
                           [mM][oO][uU][sS][eE][mM][oO][vV][eE] |
                           [mM][oO][uU][sS][eE][oO][uU][tT]     |
                           [mM][oO][uU][sS][eE][oO][vV][eE][rR] |
                           [mM][oO][uU][sS][eE][uU][pP]         |
                           [rR][eE][sS][eE][tT]                 |
                           [sS][eE][lL][eE][cC][tT]             |
                           [sS][uU][bB][mM][iI][tT]             |
                           [uU][nN][lL][oO][aA][dD]             )

SingleQuoted = ( "'" ( "\\'" | [^']* )* "'" )
DoubleQuoted = ( "\"" ( "\\\"" | [^\"]* )* "\"" )
ServerSideInclude = ( "<!--#" ( [^'\"] | {SingleQuoted} | {DoubleQuoted} )* "-->" )
EventAttribute = [oO][nN] {EventAttributeSuffixes} \s* "=" \s* ( {SingleQuoted} | {DoubleQuoted} )
OpenTagContent = ( {EventAttribute} | [^<>] | {ServerSideInclude} )*

InlineElment = ( [aAbBiIqQsSuU]                   |
                 [aA][bB][bB][rR]                 |
                 [aA][cC][rR][oO][nN][yY][mM]     |
                 [bB][aA][sS][eE][fF][oO][nN][tT] |
                 [bB][dD][oO]                     |
                 [bB][iI][gG]                     |
                 [cC][iI][tT][eE]                 |
                 [cC][oO][dD][eE]                 |
                 [dD][fF][nN]                     |
                 [eE][mM]                         |
                 [fF][oO][nN][tT]                 |
                 [iI][mM][gG]                     |
                 [iI][nN][pP][uU][tT]             |
                 [kK][bB][dD]                     |
                 [lL][aA][bB][eE][lL]             |
                 [sS][aA][mM][pP]                 |
                 [sS][eE][lL][eE][cC][tT]         |
                 [sS][mM][aA][lL][lL]             |
                 [sS][pP][aA][nN]                 |
                 [sS][tT][rR][iI][kK][eE]         |
                 [sS][tT][rR][oO][nN][gG]         |
                 [sS][uU][bB]                     |
                 [sS][uU][pP]                     |
                 [tT][eE][xX][tT][aA][rR][eE][aA] |
                 [tT][tT]                         |
                 [vV][aA][rR]                     )


%include HTMLCharacterEntities.jflex

%{
  private static final int INITIAL_INPUT_SEGMENT_SIZE = 1024;
  private static final char BLOCK_LEVEL_START_TAG_REPLACEMENT = '\n';
  private static final char BLOCK_LEVEL_END_TAG_REPLACEMENT = '\n';
  private static final char BR_START_TAG_REPLACEMENT = '\n';
  private static final char BR_END_TAG_REPLACEMENT = '\n';
  private static final char SCRIPT_REPLACEMENT = '\n';
  private static final char STYLE_REPLACEMENT = '\n';
  private static final char REPLACEMENT_CHARACTER = '\uFFFD';

  private CharArraySet escapedTags = null;
  private int inputStart;
  private int cumulativeDiff;
  private boolean escapeBR = false;
  private boolean escapeSCRIPT = false;
  private boolean escapeSTYLE = false;
  private int restoreState;
  private int previousRestoreState;
  private int outputCharCount;
  private int eofReturnValue;
  private TextSegment inputSegment
      = new TextSegment(INITIAL_INPUT_SEGMENT_SIZE);
  private TextSegment outputSegment = inputSegment;
  private TextSegment entitySegment = new TextSegment(2);

  /**
   * Creates a new HTMLStripCharFilter over the provided Reader.
   * @param source Reader to strip html tags from.
   */
  public HTMLStripCharFilter(Reader source) {
    super(source);
    this.zzReader = source;
  }

  /**
   * Creates a new HTMLStripCharFilter over the provided Reader
   * with the specified start and end tags.
   * @param source Reader to strip html tags from.
   * @param escapedTags Tags in this set (both start and end tags)
   *  will not be filtered out.
   */
  public HTMLStripCharFilter(Reader source, Set<String> escapedTags) {
    super(source);
    this.zzReader = source;
    if (null != escapedTags) {
      for (String tag : escapedTags) {
        if (tag.equalsIgnoreCase("BR")) {
          escapeBR = true;
        } else if (tag.equalsIgnoreCase("SCRIPT")) {
          escapeSCRIPT = true;
        } else if (tag.equalsIgnoreCase("STYLE")) {
          escapeSTYLE = true;
        } else {
          if (null == this.escapedTags) {
            this.escapedTags = new CharArraySet(16, true);
          }
          this.escapedTags.add(tag);
        }
      }
    }
  }

  @Override
  public int read() throws IOException {
    if (outputSegment.isRead()) {
      if (zzAtEOF) {
        return -1;
      }
      int ch = nextChar();
      ++outputCharCount;
      return ch;
    }
    int ch = outputSegment.nextChar();
    ++outputCharCount;
    return ch;
  }

  @Override
  public int read(char cbuf[], int off, int len) throws IOException {
    int i = 0;
    for ( ; i < len ; ++i) {
      int ch = read();
      if (ch == -1) break;
      cbuf[off++] = (char)ch;
    }
    return i > 0 ? i : (len == 0 ? 0 : -1);
  }

  @Override
  public void close() throws IOException {
    yyclose();
  }

  static int getInitialBufferSize() {  // Package private, for testing purposes
    return ZZ_BUFFERSIZE;
  }

  private class TextSegment extends OpenStringBuilder {
    /** The position from which the next char will be read. */
    int pos = 0;

    /** Wraps the given buffer and sets this.len to the given length. */
    TextSegment(char[] buffer, int length) {
      super(buffer, length);
    }

    /** Allocates an internal buffer of the given size. */
    TextSegment(int size) {
      super(size);
    }

    /** Sets len = 0 and pos = 0. */
    void clear() {
      reset();
      restart();
    }

    /** Sets pos = 0 */
    void restart() {
      pos = 0;
    }

    /** Returns the next char in the segment. */
    int nextChar() {
      assert (! isRead()): "Attempting to read past the end of a segment.";
      return buf[pos++];
    }

    /** Returns true when all characters in the text segment have been read */
    boolean isRead() {
      return pos >= len;
    }
  }
%}

%eofval{
  return eofReturnValue;
%eofval}
%eof{
  switch (zzLexicalState) {
    case SCRIPT:
    case COMMENT:
    case SCRIPT_COMMENT:
    case STYLE:
    case STYLE_COMMENT:
    case SINGLE_QUOTED_STRING:
    case DOUBLE_QUOTED_STRING:
    case END_TAG_TAIL_EXCLUDE:
    case END_TAG_TAIL_SUBSTITUTE:
    case START_TAG_TAIL_EXCLUDE:
    case SERVER_SIDE_INCLUDE:
    case START_TAG_TAIL_SUBSTITUTE: { // Exclude
      // add (length of input that won't be output) [ - (substitution length) = 0 ]
      cumulativeDiff += yychar - inputStart;
      // position the correction at (already output length) [ + (substitution length) = 0 ]
      addOffCorrectMap(outputCharCount, cumulativeDiff);
      outputSegment.clear();
      eofReturnValue = -1;
      break;
    }
    case CHARACTER_REFERENCE_TAIL: {        // Substitute
      // At end of file, allow char refs without semicolons
      // add (length of input that won't be output) - (substitution length)
      cumulativeDiff += inputSegment.length() - outputSegment.length();
      // position the correction at (already output length) + (substitution length)
      addOffCorrectMap(outputCharCount + outputSegment.length(), cumulativeDiff);
      eofReturnValue = ( ! outputSegment.isRead()) ? outputSegment.nextChar() : -1;
      break;
    }
    case BANG:
    case CDATA:
    case AMPERSAND:
    case NUMERIC_CHARACTER:
    case END_TAG_TAIL_INCLUDE:
    case START_TAG_TAIL_INCLUDE:
    case LEFT_ANGLE_BRACKET:
    case LEFT_ANGLE_BRACKET_SLASH:
    case LEFT_ANGLE_BRACKET_SPACE: {        // Include
      outputSegment = inputSegment;
      eofReturnValue = ( ! outputSegment.isRead()) ? outputSegment.nextChar() : -1;
      break;
    }
    default: {
      eofReturnValue = -1;
    }
  }
%eof}

%%

"&" {
  inputStart = yychar;
  inputSegment.clear();
  inputSegment.append('&');
  yybegin(AMPERSAND);
}

"<" {
  inputStart = yychar;
  inputSegment.clear();
  inputSegment.append('<');
  yybegin(LEFT_ANGLE_BRACKET);
}

<AMPERSAND> {
  {CharacterEntities} {
    int length = yylength();
    inputSegment.write(zzBuffer, zzStartRead, length);
    entitySegment.clear();
    char ch = entityValues.get(zzBuffer, zzStartRead, length).charValue();
    entitySegment.append(ch);
    outputSegment = entitySegment;
    yybegin(CHARACTER_REFERENCE_TAIL);
  }
  "#" { inputSegment.append('#'); yybegin(NUMERIC_CHARACTER); }

//                                             1   1       11              11
// 0  1   2   3       45              678  9   0   1       23              45
  "#" [xX][dD][89aAbB][0-9a-fA-F]{2} ";&#" [xX][dD][c-fC-F][0-9a-fA-F]{2} ";" {
    // Handle paired UTF-16 surrogates.
    outputSegment = entitySegment;
    outputSegment.clear();
    String surrogatePair = yytext();
    char highSurrogate = '\u0000';
    try {
      highSurrogate = (char)Integer.parseInt(surrogatePair.substring(2, 6), 16);
    } catch(Exception e) { // should never happen
      assert false: "Exception parsing high surrogate '"
                  + surrogatePair.substring(2, 6) + "'";
    }
    try {
      outputSegment.unsafeWrite
          ((char)Integer.parseInt(surrogatePair.substring(10, 14), 16));
    } catch(Exception e) { // should never happen
      assert false: "Exception parsing low surrogate '"
                  + surrogatePair.substring(10, 14) + "'";
    }
    // add (previously matched input length) + (this match length) - (substitution length)
    cumulativeDiff += inputSegment.length() + yylength() - 2;
    // position the correction at (already output length) + (substitution length)
    addOffCorrectMap(outputCharCount + 2, cumulativeDiff);
    inputSegment.clear();
    yybegin(YYINITIAL);
    return highSurrogate;
  }

//                          1   1       11              11
// 01  2    345    678  9   0   1       23              45
  "#5" [56] \d{3} ";&#" [xX][dD][c-fC-F][0-9a-fA-F]{2} ";" {
    // Handle paired UTF-16 surrogates.
    String surrogatePair = yytext();
    char highSurrogate = '\u0000';
    try { // High surrogates are in decimal range [55296, 56319]
      highSurrogate = (char)Integer.parseInt(surrogatePair.substring(1, 6));
    } catch(Exception e) { // should never happen
      assert false: "Exception parsing high surrogate '"
                  + surrogatePair.substring(1, 6) + "'";
    }
    if (Character.isHighSurrogate(highSurrogate)) {
      outputSegment = entitySegment;
      outputSegment.clear();
      try {
        outputSegment.unsafeWrite
            ((char)Integer.parseInt(surrogatePair.substring(10, 14), 16));
      } catch(Exception e) { // should never happen
        assert false: "Exception parsing low surrogate '"
                    + surrogatePair.substring(10, 14) + "'";
      }
      // add (previously matched input length) + (this match length) - (substitution length)
      cumulativeDiff += inputSegment.length() + yylength() - 2;
      // position the correction at (already output length) + (substitution length)
      addOffCorrectMap(outputCharCount + 2, cumulativeDiff);
      inputSegment.clear();
      yybegin(YYINITIAL);
      return highSurrogate;
    }
    yypushback(surrogatePair.length() - 1); // Consume only '#'
    inputSegment.append('#');
    yybegin(NUMERIC_CHARACTER);
  }

//                                          1    111     11
// 0  1   2   3       45              6789  0    123     45
  "#" [xX][dD][89aAbB][0-9a-fA-F]{2} ";&#5" [67] \d{3}  ";" {
    // Handle paired UTF-16 surrogates.
    String surrogatePair = yytext();
    char highSurrogate = '\u0000';
    char lowSurrogate = '\u0000';
    try {
      highSurrogate = (char)Integer.parseInt(surrogatePair.substring(2, 6), 16);
    } catch(Exception e) { // should never happen
      assert false: "Exception parsing high surrogate '"
                  + surrogatePair.substring(2, 6) + "'";
    }
    try { // Low surrogates are in decimal range [56320, 57343]
      lowSurrogate = (char)Integer.parseInt(surrogatePair.substring(9, 14));
    } catch(Exception e) { // should never happen
      assert false: "Exception parsing low surrogate '"
                  + surrogatePair.substring(9, 14) + "'";
    }
    if (Character.isLowSurrogate(lowSurrogate)) {
      outputSegment = entitySegment;
      outputSegment.clear();
      outputSegment.unsafeWrite(lowSurrogate);
      // add (previously matched input length) + (this match length) - (substitution length)
      cumulativeDiff += inputSegment.length() + yylength() - 2;
      // position the correction at (already output length) + (substitution length)
      addOffCorrectMap(outputCharCount + 2, cumulativeDiff);
      inputSegment.clear();
      yybegin(YYINITIAL);
      return highSurrogate;
    }
    yypushback(surrogatePair.length() - 1); // Consume only '#'
    inputSegment.append('#');
    yybegin(NUMERIC_CHARACTER);
  }

//                       1    111     11
// 01  2    345    6789  0    123     45
  "#5" [56] \d{3} ";&#5" [67] \d{3}  ";" {
    // Handle paired UTF-16 surrogates.
    String surrogatePair = yytext();
    char highSurrogate = '\u0000';
    try { // High surrogates are in decimal range [55296, 56319]
      highSurrogate = (char)Integer.parseInt(surrogatePair.substring(1, 6));
    } catch(Exception e) { // should never happen
      assert false: "Exception parsing high surrogate '"
                  + surrogatePair.substring(1, 6) + "'";
    }
    if (Character.isHighSurrogate(highSurrogate)) {
      char lowSurrogate = '\u0000';
      try { // Low surrogates are in decimal range [56320, 57343]
        lowSurrogate = (char)Integer.parseInt(surrogatePair.substring(9, 14));
      } catch(Exception e) { // should never happen
        assert false: "Exception parsing low surrogate '"
                    + surrogatePair.substring(9, 14) + "'";
      }
      if (Character.isLowSurrogate(lowSurrogate)) {
        outputSegment = entitySegment;
        outputSegment.clear();
        outputSegment.unsafeWrite(lowSurrogate);
        // add (previously matched input length) + (this match length) - (substitution length)
        cumulativeDiff += inputSegment.length() + yylength() - 2;
        // position the correction at (already output length) + (substitution length)
        addOffCorrectMap(outputCharCount + 2, cumulativeDiff);
        inputSegment.clear();
        yybegin(YYINITIAL);
        return highSurrogate;
      }
    }
    yypushback(surrogatePair.length() - 1); // Consume only '#'
    inputSegment.append('#');
    yybegin(NUMERIC_CHARACTER);
  }
}

<NUMERIC_CHARACTER> {
  [xX] [0-9A-Fa-f]+ {
    int matchLength = yylength();
    inputSegment.write(zzBuffer, zzStartRead, matchLength);
    if (matchLength <= 6) { // 10FFFF: max 6 hex chars
      String hexCharRef
          = new String(zzBuffer, zzStartRead + 1, matchLength - 1);
      int codePoint = 0;
      try {
        codePoint = Integer.parseInt(hexCharRef, 16);
      } catch(Exception e) {
        assert false: "Exception parsing hex code point '" + hexCharRef + "'";
      }
      if (codePoint <= 0x10FFFF) {
        outputSegment = entitySegment;
        outputSegment.clear();
        if (codePoint >= Character.MIN_SURROGATE
            && codePoint <= Character.MAX_SURROGATE) {
          outputSegment.unsafeWrite(REPLACEMENT_CHARACTER);
        } else {
          outputSegment.setLength
              (Character.toChars(codePoint, outputSegment.getArray(), 0));
        }
        yybegin(CHARACTER_REFERENCE_TAIL);
      } else {
        outputSegment = inputSegment;
        yybegin(YYINITIAL);
        return outputSegment.nextChar();
      }
    } else {
      outputSegment = inputSegment;
      yybegin(YYINITIAL);
      return outputSegment.nextChar();
    }
  }
  [0-9]+ {
    int matchLength = yylength();
    inputSegment.write(zzBuffer, zzStartRead, matchLength);
    if (matchLength <= 7) { // 0x10FFFF = 1114111: max 7 decimal chars
      String decimalCharRef = yytext();
      int codePoint = 0;
      try {
        codePoint = Integer.parseInt(decimalCharRef);
      } catch(Exception e) {
        assert false: "Exception parsing code point '" + decimalCharRef + "'";
      }
      if (codePoint <= 0x10FFFF) {
        outputSegment = entitySegment;
        outputSegment.clear();
        if (codePoint >= Character.MIN_SURROGATE
            && codePoint <= Character.MAX_SURROGATE) {
          outputSegment.unsafeWrite(REPLACEMENT_CHARACTER);
        } else {
          outputSegment.setLength
              (Character.toChars(codePoint, outputSegment.getArray(), 0));
        }
        yybegin(CHARACTER_REFERENCE_TAIL);
      } else {
        outputSegment = inputSegment;
        yybegin(YYINITIAL);
        return outputSegment.nextChar();
      }
    } else {
      outputSegment = inputSegment;
      yybegin(YYINITIAL);
      return outputSegment.nextChar();
    }
  }
}

<CHARACTER_REFERENCE_TAIL> {
  ";" {
    // add (previously matched input length) + (this match length) - (substitution length)
    cumulativeDiff += inputSegment.length() + yylength() - outputSegment.length();
    // position the correction at (already output length) + (substitution length)
    addOffCorrectMap(outputCharCount + outputSegment.length(), cumulativeDiff);
    yybegin(YYINITIAL);
    return outputSegment.nextChar();
  }
}

<LEFT_ANGLE_BRACKET_SLASH> {
  \s+ { inputSegment.write(zzBuffer, zzStartRead, yylength()); }
  [bB][rR] \s* ">" {
    yybegin(YYINITIAL);
    if (escapeBR) {
      inputSegment.write(zzBuffer, zzStartRead, yylength());
      outputSegment = inputSegment;
      return outputSegment.nextChar();
    } else {
      // add (previously matched input length) + (this match length) - (substitution length)
      cumulativeDiff += inputSegment.length() + yylength() - 1;
      // position the correction at (already output length) + (substitution length)
      addOffCorrectMap(outputCharCount + 1, cumulativeDiff);
      inputSegment.reset();
      return BR_END_TAG_REPLACEMENT;
    }
  }
  {InlineElment} {
    inputSegment.write(zzBuffer, zzStartRead, yylength());
    if (null != escapedTags
        && escapedTags.contains(zzBuffer, zzStartRead, yylength())) {
      yybegin(END_TAG_TAIL_INCLUDE);
    } else {
      yybegin(END_TAG_TAIL_EXCLUDE);
    }
  }
  {Name} {
    inputSegment.write(zzBuffer, zzStartRead, yylength());
    if (null != escapedTags
        && escapedTags.contains(zzBuffer, zzStartRead, yylength())) {
      yybegin(END_TAG_TAIL_INCLUDE);
    } else {
      yybegin(END_TAG_TAIL_SUBSTITUTE);
    }
  }
}

<END_TAG_TAIL_INCLUDE> {
   \s* ">" {
     inputSegment.write(zzBuffer, zzStartRead, yylength());
     outputSegment = inputSegment;
     yybegin(YYINITIAL);
     return outputSegment.nextChar();
   }
}

<END_TAG_TAIL_EXCLUDE> {
  \s* ">" {
    // add (previously matched input length) + (this match length) [ - (substitution length) = 0 ]
    cumulativeDiff += inputSegment.length() + yylength();
    // position the correction at (already output length) [ + (substitution length) = 0 ]
    addOffCorrectMap(outputCharCount, cumulativeDiff);
    inputSegment.clear();
    yybegin(YYINITIAL);
  }
}

<END_TAG_TAIL_SUBSTITUTE> {
  \s* ">" {
    // add (previously matched input length) + (this match length) - (substitution length)
    cumulativeDiff += inputSegment.length() + yylength() - 1;
    // position the correction at (already output length) + (substitution length)
    addOffCorrectMap(outputCharCount + 1, cumulativeDiff);
    inputSegment.clear();
    yybegin(YYINITIAL);
    return BLOCK_LEVEL_END_TAG_REPLACEMENT;
  }
}

<LEFT_ANGLE_BRACKET> {
  "!" { inputSegment.append('!'); yybegin(BANG); }
  "/" { inputSegment.append('/'); yybegin(LEFT_ANGLE_BRACKET_SLASH); }
  \s+ {
    inputSegment.write(zzBuffer, zzStartRead, yylength());
    yybegin(LEFT_ANGLE_BRACKET_SPACE);
  }
  "?" [^>]* [/?] ">" {
    // add (previously matched input length) + (this match length) [ - (substitution length) = 0 ]
    cumulativeDiff += inputSegment.length() + yylength();
    // position the correction at (already output length) [ + (substitution length) = 0 ]
    addOffCorrectMap(outputCharCount, cumulativeDiff);
    inputSegment.clear();
    yybegin(YYINITIAL);
  }
  \s* [bB][rR] ( ( "="\s* | \s+ ) {OpenTagContent} )? \s* "/"? ">" {
    yybegin(YYINITIAL);
    if (escapeBR) {
      inputSegment.write(zzBuffer, zzStartRead, yylength());
      outputSegment = inputSegment;
      return outputSegment.nextChar();
    } else {
      // add (previously matched input length) + (this match length) - (substitution length)
      cumulativeDiff += inputSegment.length() + yylength() - 1;
      // position the correction at (already output length) + (substitution length)
      addOffCorrectMap(outputCharCount + 1, cumulativeDiff);
      inputSegment.reset();
      return BR_START_TAG_REPLACEMENT;
    }
  }
  \s* [sS][cC][rR][iI][pP][tT] ( \s+ {OpenTagContent} )? \s*  ">" {
    yybegin(SCRIPT);
    if (escapeSCRIPT) {
      inputSegment.write(zzBuffer, zzStartRead, yylength());
      outputSegment = inputSegment;
      inputStart += 1 + yylength();
      return outputSegment.nextChar();
    }
  }
  \s* [sS][tT][yY][lL][eE] ( \s+ {OpenTagContent} )? \s* ">" {
    yybegin(STYLE);
    if (escapeSTYLE) {
      inputSegment.write(zzBuffer, zzStartRead, yylength());
      outputSegment = inputSegment;
      inputStart += 1 + yylength();
      return outputSegment.nextChar();
    }
  }
}

<LEFT_ANGLE_BRACKET, LEFT_ANGLE_BRACKET_SPACE> {
  {InlineElment} {
    inputSegment.write(zzBuffer, zzStartRead, yylength());
    if (null != escapedTags
        && escapedTags.contains(zzBuffer, zzStartRead, yylength())) {
      yybegin(START_TAG_TAIL_INCLUDE);
    } else {
      yybegin(START_TAG_TAIL_EXCLUDE);
    }
  }
  {Name} {
    inputSegment.write(zzBuffer, zzStartRead, yylength());
    if (null != escapedTags
        && escapedTags.contains(zzBuffer, zzStartRead, yylength())) {
      yybegin(START_TAG_TAIL_INCLUDE);
    } else {
      yybegin(START_TAG_TAIL_SUBSTITUTE);
    }
  }
}

<START_TAG_TAIL_INCLUDE> {
   ( ( "="\s* | \s+ ) {OpenTagContent} )? \s* "/"? ">" {
     inputSegment.write(zzBuffer, zzStartRead, yylength());
     outputSegment = inputSegment;
     yybegin(YYINITIAL);
     return outputSegment.nextChar();
   }
}

<START_TAG_TAIL_EXCLUDE> {
   ( ( "="\s* | \s+ ) {OpenTagContent} )? \s* "/"? ">" {
    // add (previously matched input length) + (this match length) [ - (substitution length) = 0 ]
    cumulativeDiff += inputSegment.length() + yylength();
    // position the correction at (already output length) [ + (substitution length) = 0 ]
    addOffCorrectMap(outputCharCount, cumulativeDiff);
    inputSegment.clear();
    outputSegment = inputSegment;
    yybegin(YYINITIAL);
  }
}

<START_TAG_TAIL_SUBSTITUTE> {
  ( ( "="\s* | \s+ ) {OpenTagContent} )? \s*  "/"? ">" {
    // add (previously matched input length) + (this match length) - (substitution length)
    cumulativeDiff += inputSegment.length() + yylength() - 1;
    // position the correction at (already output length) + (substitution length)
    addOffCorrectMap(outputCharCount + 1, cumulativeDiff);
    inputSegment.clear();
    yybegin(YYINITIAL);
    return BLOCK_LEVEL_START_TAG_REPLACEMENT;
  }
}

<BANG> {
  "--" {
    if (inputSegment.length() > 2) { // Chars between "<!" and "--" - this is not a comment
      inputSegment.append(yytext());
    } else {
      yybegin(COMMENT);
    }
  }
  ">" {
    // add (previously matched input length) + (this match length) [ - (substitution length) = 0 ]
    cumulativeDiff += inputSegment.length() + yylength();
    // position the correction at (already output length) [ + (substitution length) = 0 ]
    addOffCorrectMap(outputCharCount, cumulativeDiff);
    inputSegment.clear();
    yybegin(YYINITIAL);
  }
  // From XML 1.0 <http://www.w3.org/TR/xml/>:
  //
  // [18] CDSect  ::= CDStart CData CDEnd
  // [19] CDStart ::= '<![CDATA['
  // [20] CData   ::= (Char* - (Char* ']]>' Char*))
  // [21] CDEnd   ::= ']]>'
  //
  "[CDATA[" {
    if (inputSegment.length() > 2) { // Chars between "<!" and "[CDATA[" - this is not a CDATA section
      inputSegment.append(yytext());
    } else {
      // add (previously matched input length) + (this match length) [ - (substitution length) = 0 ]
      cumulativeDiff += inputSegment.length() + yylength();
      // position the correction at (already output length) [ + (substitution length) = 0 ]
      addOffCorrectMap(outputCharCount, cumulativeDiff);
      inputSegment.clear();
      yybegin(CDATA);
    }
  }
  [^] {
    inputSegment.append(yytext());
  }
}

<CDATA> {
  "]]>" {
    // add (this match length) [ - (substitution length) = 0 ]
    cumulativeDiff += yylength();
    // position the correction at (already output length) [ + (substitution length) = 0 ]
    addOffCorrectMap(outputCharCount, cumulativeDiff);
    yybegin(YYINITIAL);
  }
  [^] { 
    if (yylength() == 1) {
      return zzBuffer[zzStartRead];
    } else {
      outputSegment.append(yytext()); return outputSegment.nextChar();
    }
  }
}

<COMMENT> {
  "<!--#" { restoreState = COMMENT; yybegin(SERVER_SIDE_INCLUDE); }
  "-->" {
    // add (previously matched input length) + (this match length) [ - (substitution length) = 0]
    cumulativeDiff += yychar - inputStart + yylength();
    // position the correction at (already output length) [ + (substitution length) = 0]
    addOffCorrectMap(outputCharCount, cumulativeDiff);
    inputSegment.clear();
    yybegin(YYINITIAL);
  }
  [^] { }
}

<SERVER_SIDE_INCLUDE> {
  "-->" { yybegin(restoreState); }
  "'" {
    previousRestoreState = restoreState;
    restoreState = SERVER_SIDE_INCLUDE;
    yybegin(SINGLE_QUOTED_STRING);
  }
  "\"" {
    previousRestoreState = restoreState;
    restoreState = SERVER_SIDE_INCLUDE;
    yybegin(DOUBLE_QUOTED_STRING);
  }
  [^] { }
}

<SCRIPT_COMMENT> {
  "<!--#" { restoreState = SCRIPT_COMMENT; yybegin(SERVER_SIDE_INCLUDE); }
  "'"     { restoreState = SCRIPT_COMMENT; yybegin(SINGLE_QUOTED_STRING); }
  "\""    { restoreState = SCRIPT_COMMENT; yybegin(DOUBLE_QUOTED_STRING); }
  "-->"   { yybegin(SCRIPT); }
  [^] { }
}

<STYLE_COMMENT> {
  "<!--#" { restoreState = STYLE_COMMENT; yybegin(SERVER_SIDE_INCLUDE); }
  "'"     { restoreState = STYLE_COMMENT; yybegin(SINGLE_QUOTED_STRING); }
  "\""    { restoreState = STYLE_COMMENT; yybegin(DOUBLE_QUOTED_STRING); }
  "-->"   { yybegin(STYLE); }
  [^] { }
}

<SINGLE_QUOTED_STRING> {
  "\\" [^] { }
  "'" { yybegin(restoreState); restoreState = previousRestoreState; }
  [^] { }
}

<DOUBLE_QUOTED_STRING> {
  "\\" [^] { }
  "\"" { yybegin(restoreState); restoreState = previousRestoreState; }
  [^] { }
}

<SCRIPT> {
  "<!--" { yybegin(SCRIPT_COMMENT); }
  "</" \s* [sS][cC][rR][iI][pP][tT] \s* ">" {
    inputSegment.clear();
    yybegin(YYINITIAL);
    // add (previously matched input length) -- current match and substitution handled below
    cumulativeDiff += yychar - inputStart;
    // position at (already output length) -- substitution handled below
    int offsetCorrectionPos = outputCharCount;
    int returnValue;
    if (escapeSCRIPT) {
      inputSegment.write(zzBuffer, zzStartRead, yylength());
      outputSegment = inputSegment;
      returnValue = outputSegment.nextChar();
    } else {
      // add (this match length) - (substitution length)
      cumulativeDiff += yylength() - 1;
      // add (substitution length)
      ++offsetCorrectionPos;
      returnValue = SCRIPT_REPLACEMENT;
    }
    addOffCorrectMap(offsetCorrectionPos, cumulativeDiff);
    return returnValue;
  }
  [^] { }
}

<STYLE> {
  "<!--" { yybegin(STYLE_COMMENT); }
  "</" \s* [sS][tT][yY][lL][eE] \s* ">" {
    inputSegment.clear();
    yybegin(YYINITIAL);
    // add (previously matched input length) -- current match and substitution handled below
    cumulativeDiff += yychar - inputStart;
    // position the offset correction at (already output length) -- substitution handled below
    int offsetCorrectionPos = outputCharCount;
    int returnValue;
    if (escapeSTYLE) {
      inputSegment.write(zzBuffer, zzStartRead, yylength());
      outputSegment = inputSegment;
      returnValue = outputSegment.nextChar();
    } else {
      // add (this match length) - (substitution length)
      cumulativeDiff += yylength() - 1;
      // add (substitution length)
      ++offsetCorrectionPos;
      returnValue = STYLE_REPLACEMENT;
    }
    addOffCorrectMap(offsetCorrectionPos, cumulativeDiff);
    return returnValue;
  }
  [^] { }
}

<AMPERSAND,NUMERIC_CHARACTER,CHARACTER_REFERENCE_TAIL,LEFT_ANGLE_BRACKET_SLASH,END_TAG_TAIL_INCLUDE,END_TAG_TAIL_EXCLUDE,END_TAG_TAIL_SUBSTITUTE,LEFT_ANGLE_BRACKET,LEFT_ANGLE_BRACKET_SPACE,START_TAG_TAIL_INCLUDE,START_TAG_TAIL_EXCLUDE,START_TAG_TAIL_SUBSTITUTE,BANG> {
  [^] {
    yypushback(yylength());
    outputSegment = inputSegment;
    outputSegment.restart();
    yybegin(YYINITIAL);
    return outputSegment.nextChar();
  }
}

[^] { 
  if (yylength() == 1) {
    return zzBuffer[zzStartRead];
  } else {
    outputSegment.append(yytext()); return outputSegment.nextChar();
  }
}

package org.apache.lucene.queryparser.spans;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.spans.SQPClause.TYPE;
import org.apache.lucene.util.mutable.MutableValueInt;

/**
 * Tokenizer that returns a list of tokens of types:
 * Term, RegexTerm, RangeTerm
 * Boolean AND, NOT, etc
 * Field
 * <p>
 * A clause is represented as a node in the list where the clause started.
 * The clause includes offsets into the list for where its contents start and end.
 * <p>
 * Unescapes field and boolean operator tokens, but nothing else
 * 
 * <p>
 * Identifies the following types of exceptions:
 * mismatching/unmatched () "" []
 * bad unicode escape sequences
 * some illegal conj and mods (and and)
 * illegal (for this parser) boosts: term^0.6^2
 * 
 */
class SpanQueryLexer {

  private final static String AND = "AND";
  private final static String NOT = "NOT";
  private final static String OR = "OR"; //silently removed from queries...beware!

  private final static int DEFAULT_MIN_REQUIRED_IN_OR = 2;

  private final static Pattern UNESCAPE_REGEX = Pattern.compile("\\\\(.)");

  private final static String OPEN_PAREN = "(";
  private final static String OPEN_BRACKET = "[";
  private final static String OPEN_CURLY = "{";
  private final static String CLOSE_BRACKET = "]";
  private final static String CLOSE_CURLY = "}";
  private final static String DQUOTE = "\"";

  //Groups
  public enum G {
    WHOLE,
    ESCAPE,
    SPACE,
    PLUS_MINUS,
    FIELD,
    SINGLE_QUOTED,
    REGEX,
    CLOSE_PAREN,
    CLOSE_PAREN_DIGITS,
    QUOTE_OR_CLOSING_BRACKET,
    NEAR_PARAM,
    NEAR_IN_ORDER,
    NEAR_SLOP,
    NOT_NEAR_PRE,
    NOT_NEAR_POST,
    BOOST,
    OPEN_PAREN_OR_BRACKET,
  };

  //using \\p{Z} to capture wider variety of Unicode whitespace than \\s
  //DO NOT DO THIS!!! Blew heap when a string had a character beyond bmp.
  //  private final static String TERMINAL_STRING = 
  //"((?:\\\\.|(?:[-+](?![/\\(\\[\"]))|[^-+\\\\\\(\\)\\[\\]\\p{Z}\"/:\\^])+)(?:(:)|"+BOOST+")?";

  private final static Pattern ILLEGAL_END = Pattern.compile("^((?:\\\\.)|[^\\\\])*\\\\$");
  private final static Pattern ILLEGAL_UNICODE_ESCAPE = Pattern.compile("\\\\u([0-9a-fA-F]{0,4})");

  //need (?s) in case \n is escaped
  private final static String ESCAPE_STRING = "(?s)((?:\\\\.)+)";
  private final static String NO_CAPTURE_SPACE = "[\t-\r\u0085\\p{Z}]+";
  private final static String SPACE_STRING = "("+NO_CAPTURE_SPACE+")";
  private final static String FIELD_END_STRING = ("(:)");
  private final static String REGEX_STRING = "(?s)(?:/((?:\\\\.|[^/\\\\])+?)/)";

  private final static String SINGLE_QUOTE_STRING = "'((?:''|[^']+)+)'";

  private final static String BOOST_STRING = "\\^((?:\\d*\\.)?\\d+)";
  private final static Pattern BOOST_PATTERN = Pattern.compile(BOOST_STRING);

  //plus/minus must not be followed by a space (to be boolean op)
  //it must be preceded by space or start of string.  The current
  //regex doesn't take into consideration an escaped space before the +-
  private final static String PLUS_MINUS_STRING = "(?<=(?:^|"+NO_CAPTURE_SPACE+"))([-+])(?!"+NO_CAPTURE_SPACE+")";
  private final static String OPENING = "([\\(\\[{])";

  private final static String NEAR_MODIFIERS = "~(?:(>)?(\\d+)?)?";
  private final static String NOT_NEAR_MODIFIERS = "!~(?:(\\d+)(?:,(\\d+))?)?";

  private final static String NEAR_CLOSING_MODIFIERS = "("+NEAR_MODIFIERS+"|"+NOT_NEAR_MODIFIERS+")?";

  private final static String OR_CLOSING_MODIFIER = "(?:~(\\d*))?";
  private final static String CLOSING_STRING = "(?:(\\))"+OR_CLOSING_MODIFIER+")|(?:([\\]\"}])"+NEAR_CLOSING_MODIFIERS+")";

  private final static Pattern TOKENIZER = Pattern.compile(
      ESCAPE_STRING + "|" +
          SPACE_STRING + "|"+  
          PLUS_MINUS_STRING+"|"+
          FIELD_END_STRING+"|"+
          "(?:"+SINGLE_QUOTE_STRING + "|"+

      REGEX_STRING +"|"+CLOSING_STRING+")(?:"+BOOST_STRING + ")?|"+OPENING);

  public List<SQPToken> getTokens(String s) throws ParseException {

    //initial validation tests
    Matcher m = ILLEGAL_END.matcher(s);
    if (m.find()) {
      throw new ParseException("Can't end query with unescaped backslash character");
    }
    m = ILLEGAL_UNICODE_ESCAPE.matcher(s);
    while (m.find()) {
      if (m.group(1).length() != 4) {
        throw new ParseException ("Illegal escaped unicode character: "+m.group(1));
      }
    }

    //k, now let's go
    List<SQPToken> tokens = new ArrayList<SQPToken>();

    Stack<SQPOpenClause> stack = new Stack<SQPOpenClause>();
    MutableValueInt nearDepth = new MutableValueInt();
    nearDepth.value = 0;

    m = TOKENIZER.matcher(s);

    int last = 0;
    while (m.find()) {
      if (m.group(G.SPACE.ordinal()) != null) {
        //space
        if (m.start() > last) {
          String term = s.substring(last, m.start());
          addRawTerm(term, nearDepth.value, tokens);
        }
        last = m.end();
      } else if (m.group(G.ESCAPE.ordinal()) != null) {
        //don't set last; keep going
      } else if (m.group(G.FIELD.ordinal()) != null) {

        if (m.start() > 0 && m.start() > last) {
          String term = s.substring(last, m.start());
          addField(term, nearDepth.value, tokens);
          last = m.end();
        }
      } else if (m.group(G.SINGLE_QUOTED.ordinal()) != null) {
        String term = m.group(G.SINGLE_QUOTED.ordinal());
        addSingleQuotedTerm(term, nearDepth.value, tokens, m);
        last = m.end();
      } else {
        if (m.start() > last) {
          String term = s.substring(last, m.start());
          addRawTerm(term, nearDepth.value, tokens);
        }
        addOpTokens(m, tokens, stack, nearDepth);
        last = m.end();
      }

    }
    if (last < s.length()) {
      String term = s.substring(last);
      addRawTerm(term, nearDepth.value, tokens);
    }

    if (stack.size() != 0) {
      //TODO add more info
      throw new ParseException("unmatched bracket");
    } else if (nearDepth.value != 0) {
      throw new ParseException("error in nearDepth calc");
    }

    testSingle(tokens);
    return tokens;
  }


  private void addSingleQuotedTerm(String term, int value, List<SQPToken> tokens,
      Matcher m) {
    SQPTerm t = new SQPTerm(unescapeSingleQuoted(term), true);
    tryToAddBoost((SQPBoostableToken)t, m);
    tokens.add(t);
  }


  private void addOpTokens(Matcher m,  
      List<SQPToken> tokens, Stack<SQPOpenClause> stack, MutableValueInt nearDepth) 
          throws ParseException{

    //these return early
    //perhaps rearrange to more closely align with operator frequency
    if (m.group(G.CLOSE_PAREN.ordinal()) != null) {
      processCloseParen(m, tokens, stack, nearDepth.value);
      return;
    } else if (m.group(G.QUOTE_OR_CLOSING_BRACKET.ordinal()) != null) {
      processCloseBracketOrQuote(m, tokens, stack, nearDepth);
      return;
    }

    SQPToken token = null;

    if (m.group(G.OPEN_PAREN_OR_BRACKET.ordinal()) != null) {
      //open paren or open bracket
      String open = m.group(G.OPEN_PAREN_OR_BRACKET.ordinal());
      if (open.equals(OPEN_PAREN)) {
        token = new SQPOpenClause(tokens.size(), m.start()+1, TYPE.PAREN);
      } else if (open.equals(OPEN_BRACKET)) {
        token = new SQPOpenClause(tokens.size(), m.start()+1, TYPE.BRACKET);
        nearDepth.value++;
      } else if (open.equals(OPEN_CURLY)) {
        token = new SQPOpenClause(tokens.size(), m.start()+1, TYPE.CURLY);
        nearDepth.value++;
      } else {
        //uh, should never happen
      }
      stack.push((SQPOpenClause)token);
    } else if (m.group(G.PLUS_MINUS.ordinal()) != null) {
      String pm = m.group(G.PLUS_MINUS.ordinal());
      if (pm.equals("+")) {
        token = new SQPBooleanOpToken(SpanQueryParserBase.MOD_REQ);
        testBooleanTokens(tokens, (SQPBooleanOpToken)token);
      } else if (pm.equals("-")) {
        token = new SQPBooleanOpToken(SpanQueryParserBase.MOD_NOT);
        testBooleanTokens(tokens, (SQPBooleanOpToken)token);
      }
    } else if (m.group(G.REGEX.ordinal()) != null) {
      token = new SQPRegexTerm(unescapeRegex(m.group(G.REGEX.ordinal())));
      tryToAddBoost((SQPBoostableToken)token, m);
    } 

    if (token != null) {
      tokens.add(token);
    }
  }

  private void processCloseBracketOrQuote(Matcher m, List<SQPToken> tokens,
      Stack<SQPOpenClause> stack, MutableValueInt nearDepth) throws ParseException {
    //open or close quote or closing bracket
    //let's start with quote
    String close = m.group(G.QUOTE_OR_CLOSING_BRACKET.ordinal());
    if (close.equals(DQUOTE)) {
      processDQuote(m, tokens, stack, nearDepth); 
      return;
    }
    //from here on out, must be close bracket
    //test for mismatched
    if (stack.isEmpty()) {
      throw new ParseException("Couldn't find matching open bracket/quote.");
    }

    SQPOpenClause open = stack.pop();
    boolean isRange = tryToProcessRange(open, m, tokens, nearDepth, close);

    if (isRange) {
      return;
    }

    SQPClause clause = buildNearOrNotNear(m, tokens, open);

    nearDepth.value--;
    tokens.set(open.getTokenOffsetStart(), clause);
  }


  private boolean tryToProcessRange(SQPOpenClause open, Matcher m, List<SQPToken> tokens,
      MutableValueInt nearDepth, String close) throws ParseException {

    //test to see if this looks like a range
    //does it contain three items; are they all terms, is the middle one "TO"
    //if it is, handle it; if there are problems throw an exception, otherwise return false

    //if there's a curly bracket to start or end, then it
    //must be a compliant range query or else throw parse exception
    boolean hasCurly = (open.getType() == SQPClause.TYPE.CURLY ||
        close.equals(CLOSE_CURLY)) ? true : false;

    //if there are any modifiers on the close bracket
    if (m.group(G.NEAR_PARAM.ordinal()) != null) {
      if (hasCurly) {
        throw new ParseException("Can't have modifiers on a range query. "+
            "Or, you can't use curly brackets for a phrase/near query");
      }
      return false;
    }

    if (open.getTokenOffsetStart() == tokens.size()-4) {
      for (int i = 1; i < 4; i++) {
        SQPToken t = tokens.get(tokens.size()-i);
        if (t instanceof SQPTerm) {
          if (i == 2 && !((SQPTerm)t).getString().equals("TO")) {
            return testBadRange(hasCurly);
          }
        } else {
          return testBadRange(hasCurly);
        }
      }
    } else {
      return testBadRange(hasCurly);
    }
    boolean startInclusive = (open.getType() == SQPClause.TYPE.BRACKET) ? true : false;
    boolean endInclusive = (close.equals(CLOSE_BRACKET))?true:false;
    SQPTerm startTerm = (SQPTerm)tokens.get(tokens.size()-3);
    String startString = startTerm.getString();
    if (startString.equals("*")) {
      if (startTerm.isQuoted()) {
        startString = "*";
      } else {
        startString = null;
      }
    }
    SQPTerm endTerm = (SQPTerm)tokens.get(tokens.size()-1);
    String endString = endTerm.getString();
    if (endString.equals("*")) {
      if (endTerm.isQuoted()) {
        endString = "*";
      } else {
        endString = null;
      }
    }
    SQPToken range = new SQPRangeTerm(startString, endString, startInclusive, endInclusive);

    //remove last term
    tokens.remove(tokens.size()-1);
    //remove TO
    tokens.remove(tokens.size()-1);
    //remove first term
    tokens.remove(tokens.size()-1);
    //remove start clause marker
    tokens.remove(tokens.size()-1);
    tokens.add(range);
    tryToAddBoost((SQPBoostableToken)range, m);
    nearDepth.value--;
    return true;
  }

  private boolean testBadRange(boolean hasCurly) throws ParseException{
    if (hasCurly == true) {
      throw new ParseException("Curly brackets should only be used in range queries");
    }
    return false;
  }
  private void processDQuote(Matcher m, List<SQPToken> tokens,
      Stack<SQPOpenClause> stack, MutableValueInt nearDepth) throws ParseException {
    //If a double-quote, don't know if open or closing yet
    //first test to see if there's a matching open quote on the stack
    //if there is, this must be a closing quote
    //if there isn't, push whatever was on the stack back and
    //treat this as an opening quote
    if (stack.size() > 0) {
      SQPOpenClause openCand = stack.pop();
      if (openCand.getType() == TYPE.QUOTE) {
        processDQuoteClose(m, tokens, openCand, nearDepth);
        return;

      }
      //put candidate back on the stack
      stack.push(openCand);
    }
    //by this point, we know that this double quote must be an opener
    SQPOpenClause token = new SQPOpenClause(tokens.size(), m.start()+1, TYPE.QUOTE);

    stack.push(token);
    nearDepth.value++;
    tokens.add(token);

    //if for some crazy reason there are ending-like modifiers on an opening quote
    //treat those mods as a plain sqpterm:
    //the "~2 quick brown fox"
    if (m.group(G.NEAR_PARAM.ordinal())!= null) {
      tokens.add(new SQPTerm(m.group(G.NEAR_PARAM.ordinal()), false));
    }
  }

  private void processDQuoteClose(Matcher m, List<SQPToken> tokens,
      SQPOpenClause open, MutableValueInt nearDepth) throws ParseException {
    SQPClause clause = buildNearOrNotNear(m, tokens, open);
    //special handling if a single term between double quotes
    //and the double quotes don't have any parameters
    if (clause instanceof SQPNearClause &&
        ! ((SQPNearClause)clause).hasParams() &&
        open.getTokenOffsetStart() == tokens.size()-2 &&
        tokens.size()-2 >=0) {
      boolean abort = false;
      SQPToken content = tokens.get(tokens.size()-1);
      if (content instanceof SQPRegexTerm) {
        //add back in the original / and /
        content = new SQPTerm(escapeDQuote("/"+((SQPRegexTerm)content).getString())+"/", false);
      } else if (content instanceof SQPTerm) {
        content = new SQPTerm(escapeDQuote(((SQPTerm)content).getString()), true);
      } else {
        abort = true;
      }
      if (abort == false) {
        //remove the last content token
        tokens.remove(tokens.size()-1);
        //remove the opening clause marker
        tokens.remove(tokens.size()-1);
        tryToAddBoost((SQPBoostableToken)content, m);
        tokens.add(content);
        ((SQPTerm)content).setIsQuoted(true);
        nearDepth.value--;
        return;
      }
    }

    nearDepth.value--;
    tryToAddBoost((SQPBoostableToken)open, m);
    tokens.set(open.getTokenOffsetStart(), clause);
  }

  private SQPClause buildNearOrNotNear(Matcher m, List<SQPToken> tokens, SQPOpenClause open) 
      throws ParseException {
    //try for not near first, return early
    if (m.group(G.NEAR_PARAM.ordinal()) != null && m.group(G.NEAR_PARAM.ordinal()).startsWith("!")) {
      int notPre = SQPNotNearClause.NOT_DEFAULT;
      int notPost = SQPNotNearClause.NOT_DEFAULT;
      if (m.group(G.NOT_NEAR_PRE.ordinal()) != null) {
        notPre = Integer.parseInt(m.group(G.NOT_NEAR_PRE.ordinal()));
        notPost = notPre;
      }
      if (m.group(G.NOT_NEAR_POST.ordinal()) != null) {
        notPost = Integer.parseInt(m.group(G.NOT_NEAR_POST.ordinal()));
      }
      //contents of this clause start at 1 after tokenOffsetStart 
      SQPNotNearClause clause = new SQPNotNearClause(open.getTokenOffsetStart()+1, tokens.size(), 
          open.getType(), notPre, notPost);
      tryToAddBoost((SQPBoostableToken)clause, m);
      return clause;
    }

    //must be near
    //if nothing is specified, inOrder == true
    Boolean inOrder = SQPNearClause.UNSPECIFIED_IN_ORDER;
    int slop = AbstractSpanQueryParser.UNSPECIFIED_SLOP;
    boolean hasParams = false;
    if (m.group(G.NEAR_PARAM.ordinal()) != null) {
      hasParams = true;
      inOrder = new Boolean(false);
    }

    if (m.group(G.NEAR_SLOP.ordinal()) != null) {
      slop = Integer.parseInt(m.group(G.NEAR_SLOP.ordinal()));
    }

    if (m.group(G.NEAR_IN_ORDER.ordinal()) != null) {
      inOrder = new Boolean(true);    
    }
    SQPNearClause clause = new SQPNearClause(open.getTokenOffsetStart()+1, tokens.size(),
        open.getStartCharOffset(), m.start(),
        open.getType(), hasParams, inOrder, slop);
    tryToAddBoost((SQPBoostableToken)clause, m);
    return clause;
  }


  private void processCloseParen(Matcher m, List<SQPToken> tokens,
      Stack<SQPOpenClause> stack, int nearDepth) throws ParseException {
    if (stack.isEmpty()) {
      throw new ParseException("Mismatched closing paren");
    }
    SQPOpenClause openCand = stack.pop();
    if (openCand.getType() == TYPE.PAREN) {
      SQPOrClause clause = new SQPOrClause(openCand.getTokenOffsetStart()+1,
          tokens.size());
      if (m.group(G.CLOSE_PAREN_DIGITS.ordinal()) != null) {
        throwIfNear(nearDepth,
            "Can't specify minimum number of terms for an 'or' clause within a 'near' clause");

        if (m.group(G.CLOSE_PAREN_DIGITS.ordinal()).length() > 0) {
          clause.setMinimumNumberShouldMatch(Integer.parseInt(m.group(G.CLOSE_PAREN_DIGITS.ordinal())));
        } else {
          clause.setMinimumNumberShouldMatch(DEFAULT_MIN_REQUIRED_IN_OR);
        }
      }
      tryToAddBoost((SQPBoostableToken)clause, m);
      tokens.set(openCand.getTokenOffsetStart(), clause);
      return;
    }
    throw new ParseException("Was expecting \")\" but found " + openCand.getType());
  }

  private void throwIfNear(int nearDepth, String string) throws ParseException {
    if (nearDepth != 0) {
      throw new ParseException(string);
    }
  }

  private void addField(String term, int nearDepth, List<SQPToken> tokens) throws ParseException {
    if (nearDepth != 0) {
      throw new ParseException("Can't specify a field within a \"Near\" clause");
    }
    if (tokens.size() > 0 && tokens.get(tokens.size()-1) instanceof SQPField) {
      throw new ParseException("A field must contain a terminal");
    }
    SQPToken token = new SQPField(SpanQueryParserBase.unescape(term));
    tokens.add(token);
  } 

  private void addRawTerm(String term, int nearDepth, List<SQPToken> tokens)
      throws ParseException {
    //The regex over-captures on a term...Term could be:
    //AND or NOT boolean operator; and term could have boost

    //does the term == AND or NOT or OR
    if (nearDepth == 0) {
      SQPToken token = null;
      if (term.equals(AND)) {
        token = new SQPBooleanOpToken(SpanQueryParserBase.CONJ_AND);
      } else if (term.equals(NOT)) {
        token = new SQPBooleanOpToken(SpanQueryParserBase.MOD_NOT);
      } else if (term.equals(OR)) {
        token = new SQPBooleanOpToken(SpanQueryParserBase.CONJ_OR);
      }
      if (token != null) {
        testBooleanTokens(tokens, (SQPBooleanOpToken)token);
        tokens.add(token);
        return;
      }
    }

    //now deal with potential boost at end of term
    Matcher boostMatcher = BOOST_PATTERN.matcher(term);
    int boostStart = -1;
    while (boostMatcher.find()) {
      boostStart = boostMatcher.start();
      if (boostMatcher.end() != term.length()) {
        throw new ParseException("Must escape boost within a term");
      }
    }
    if (boostStart == 0) {
      throw new ParseException("Boost must be attached to terminal or clause");
    }

    float boost = SpanQueryParserBase.UNSPECIFIED_BOOST;
    if (boostStart > -1) {
      String boostString = term.substring(boostStart+1);
      try{
        boost = Float.parseFloat(boostString);
      } catch (NumberFormatException e) {
        //swallow
      }
      term = term.substring(0, boostStart);
    }
    SQPToken token = new SQPTerm(unescape(term), false);
    if (boost != SpanQueryParserBase.UNSPECIFIED_BOOST) {
      ((SQPBoostableToken)token).setBoost(boost);
    }

    tokens.add(token);
  }

  private void tryToAddBoost(SQPBoostableToken t, Matcher m) {
    if (m.group(G.BOOST.ordinal()) != null) {
      try {
        t.setBoost(Float.parseFloat(m.group(G.BOOST.ordinal())));
      } catch (NumberFormatException e) {
      }
    }
  }

  /**
   * Test whether this token can be added to the list of tokens
   * based on classic queryparser rules
   */
  private void testBooleanTokens(List<SQPToken> tokens, SQPBooleanOpToken token) 
      throws ParseException {
    //there are possible exceptions with tokens.size()==0, but they
    //are the same exceptions as at clause beginning.  
    //Need to test elsewhere for start of clause issues.
    if (tokens.size() == 0) {
      return;
    }
    SQPToken t = tokens.get(tokens.size()-1);
    if (t instanceof SQPBooleanOpToken) {
      int curr = ((SQPBooleanOpToken)t).getType();
      int nxt = token.getType();
      boolean ex = false;
      if (SQPBooleanOpToken.isMod(curr)) {
        ex = true;
      } else if (curr == SpanQueryParser.CONJ_AND &&
          nxt == SpanQueryParser.CONJ_AND) {
        ex = true;
      } else if (curr == SpanQueryParser.CONJ_OR &&
          ! SQPBooleanOpToken.isMod(nxt) ) {
        ex = true;
      } else if (curr == SpanQueryParser.MOD_NOT) {
        ex = true;
      }
      if (ex == true) {
        throw new ParseException("Illegal combination of boolean conjunctions and modifiers");
      }
    }
  }

  private void testSingle(List<SQPToken> tokens) throws ParseException {
    if (tokens.size() == 0) {
      return;
    }
    if (tokens.size() == 1) {
      SQPToken t = tokens.get(0);
      if (t instanceof SQPTerminal) { 
      } else {
        throw new ParseException("Must have at least one terminal:" + tokens.get(0).toString());
      }
    }
  }

  private String unescapeSingleQuoted(String s) {
    if (s == null) {
      return "";
    }
    return s.replaceAll("''", "'");     
  }

  private String unescape(String s) {

    if (s.equals("\\AND")) {
      return "AND";
    } 
    if (s.equals("\\NOT")) {
      return "NOT";
    }
    if (s.equals("\\OR")) {
      return "OR";
    }
    return s;
  }

  private String unescapeRegex(String s) {

    Matcher m = UNESCAPE_REGEX.matcher(s);
    StringBuilder sb = new StringBuilder();
    int last = 0;
    while (m.find()) {
      sb.append(s.substring(last, m.start(0)));
      if (m.group(1).equals("/")) {
        sb.append("/");
      } else {
        sb.append("\\").append(m.group(1));
      }

      last = m.end(1);
    }
    if (last == 0) {
      return s;
    }
    sb.append(s.substring(last));
    return sb.toString();
  }

  private String escapeDQuote(String s) {
    //copied from escape in QueryParserBase.  Had to remove \\
    //to handle quoted single terms.
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      // These characters are part of the query syntax and must be escaped
      if (c == '+' || c == '-' || c == '!' || c == '(' || c == ')' || c == ':'
          || c == '^' || c == '[' || c == ']' || c == '\"' || c == '{' || c == '}' || c == '~'
          || c == '*' || c == '?' || c == '|' || c == '&' || c == '/') {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }
}

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

import java.util.List;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.spans.SpanQueryLexer;
import org.apache.lucene.queryparser.spans.SpanQueryParserBase;
import org.apache.lucene.queryparser.spans.SQPClause.TYPE;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Low level tests of the lexer.
 */
public class TestSpanQueryParserLexer extends LuceneTestCase {
  SpanQueryLexer lexer = new SpanQueryLexer();

  public void testFields() throws ParseException{
    executeSingleTokenTest(
        "the quick f1: brown fox", 
        2,
        new SQPField("f1")
        );

    //no space
    executeSingleTokenTest(
        "the quick f1:brown fox", 
        2,
        new SQPField("f1")
        );

    //non-escaped colon
    testParseException("the quick f1:f2:brown fox");

    //escaped colon
    executeSingleTokenTest(
        "the quick f1\\:f2:brown fox", 
        2,
        new SQPField("f1:f2")
        );

    //escaped colon
    executeSingleTokenTest(
        "the quick f1\\:f2:brown fox", 
        3,
        new SQPTerm("brown", false)
        );
    executeSingleTokenTest(
        "the quick f1\\ f2: brown fox", 
        2,
        new SQPField("f1 f2")
        );

    //fields should not be tokenized within a regex
    executeSingleTokenTest(
        "the quick /f1: brown/ fox", 
        2,
        new SQPRegexTerm("f1: brown")
        );

    //fields are tokenized within parens
    executeSingleTokenTest(
        "the quick (f1: brown fox)", 
        3,
        new SQPField("f1")
        );

    testParseException("the quick \"f1: brown fox\"");
    testParseException("the quick [f1: brown fox]");

  }

  public void testRegexes() throws ParseException {
    executeSingleTokenTest(
        "the quick [brown (/rabb.?t/ /f?x/)]", 
        5,
        new SQPRegexTerm("rabb.?t")
        );

    executeSingleTokenTest(
        "the quick [brown (ab/rabb.?t/cd /f?x/)]", 
        6,
        new SQPRegexTerm("rabb.?t")
        );

    //test regex unescape
    executeSingleTokenTest(
        "the quick [brown (/ra\\wb\\db\\/t/ /f?x/)]", 
        5,
        new SQPRegexTerm("ra\\wb\\db/t")
        );

    //test operators within regex
    executeSingleTokenTest(
        "the quick [brown (/(?i)a(b)+[c-e]*(f|g){0,3}/ /f?x/)]", 
        5,
        new SQPRegexTerm("(?i)a(b)+[c-e]*(f|g){0,3}")
        );

    //test non-regex
    executeSingleTokenTest(
        "'/quick/'", 
        0,
        new SQPTerm("/quick/", true)
        );
  }

  public void testOr() throws ParseException {
    SQPOrClause truth = new SQPOrClause(2,5);
    truth.setMinimumNumberShouldMatch(SQPOrClause.DEFAULT_MINIMUM_NUMBER_SHOULD_MATCH);

    executeSingleTokenTest(
        "the quick (brown fox) jumped", 
        2,
        truth
        );

    truth.setMinimumNumberShouldMatch(23);
    executeSingleTokenTest(
        "the quick (brown fox)~23 jumped", 
        2,
        truth
        );

    truth.setMinimumNumberShouldMatch(2);
    executeSingleTokenTest(
        "the quick (brown fox)~ jumped", 
        2,
        truth
        );

    //can't specify min number of ORs within a spannear phrase
    testParseException("the [quick (brown fox)~23 jumped]");
    testParseException("the [quick (brown fox)~ jumped]");
    testParseException("the \"quick (brown fox)~23 jumped\"");
    testParseException("the \"quick (brown fox)~ jumped\"");
  }

  public void testNear() throws ParseException {

    SQPNearClause truth = new SQPNearClause(3, 5, 0, 0, TYPE.QUOTE, false, 
        SQPNearClause.UNSPECIFIED_IN_ORDER, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);
    executeSingleTokenTest(
        "the quick \"brown fox\" jumped", 
        2,
        truth
        );

    truth = new SQPNearClause(3, 5, 0, 0,TYPE.QUOTE, true, 
        false, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);
    executeSingleTokenTest(
        "the quick \"brown fox\"~ jumped", 
        2,
        truth 
        );

    truth = new SQPNearClause(3, 5, 0, 0, TYPE.QUOTE, true, 
        true, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);
    executeSingleTokenTest(
        "the quick \"brown fox\"~> jumped", 
        2,
        truth
        );

    truth = new SQPNearClause(3, 5, 0, 0, TYPE.QUOTE, true, 
        false, 
        3);
    executeSingleTokenTest(
        "the quick \"brown fox\"~3 jumped", 
        2,
        truth
        );

    truth = new SQPNearClause(3, 5, 0, 0, TYPE.QUOTE, true, 
        true, 
        3);
    executeSingleTokenTest(
        "the quick \"brown fox\"~>3 jumped", 
        2,
        truth
        );

    //now try with boosts
    truth = new SQPNearClause(3, 5, 0, 0, TYPE.QUOTE, false, 
        SQPNearClause.UNSPECIFIED_IN_ORDER, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);
    ((SQPBoostableToken)truth).setBoost(2.5f);

    executeSingleTokenTest(
        "the quick \"brown fox\"^2.5 jumped", 
        2,
        truth
        );

    truth = new SQPNearClause(3, 5, 0, 0, TYPE.QUOTE, true, 
        false, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);
    ((SQPBoostableToken)truth).setBoost(2.5f);

    executeSingleTokenTest(
        "the quick \"brown fox\"~^2.5 jumped", 
        2,
        truth 
        );

    truth = new SQPNearClause(3, 5, 0, 0, TYPE.QUOTE, true, 
        true, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);
    ((SQPBoostableToken)truth).setBoost(2.5f);

    executeSingleTokenTest(
        "the quick \"brown fox\"~>^2.5 jumped", 
        2,
        truth
        );


    truth = new SQPNearClause(3, 5, 0, 0, TYPE.QUOTE, true, 
        false, 
        3);
    ((SQPBoostableToken)truth).setBoost(2.5f);

    executeSingleTokenTest(
        "the quick \"brown fox\"~3^2.5 jumped", 
        2,
        truth
        );


    truth = new SQPNearClause(3, 5, 0, 0, TYPE.QUOTE, true, 
        true, 
        3);
    ((SQPBoostableToken)truth).setBoost(2.5f);

    executeSingleTokenTest(
        "the quick \"brown fox\"~>3^2.5 jumped", 
        2,
        truth
        );

    //now test brackets
    truth = new SQPNearClause(3, 5, 0, 0, TYPE.BRACKET, false, 
        SQPNearClause.UNSPECIFIED_IN_ORDER, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);


    executeSingleTokenTest(
        "the quick [brown fox] jumped", 
        2,
        truth
        );

    truth = new SQPNearClause(3, 5, 0, 0, TYPE.BRACKET, true, 
        false, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);
    executeSingleTokenTest(
        "the quick [brown fox]~ jumped", 
        2,
        truth 
        );

    truth = new SQPNearClause(3, 5, 0, 0, TYPE.BRACKET, true, 
        true, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);

    executeSingleTokenTest(
        "the quick [brown fox]~> jumped", 
        2,
        truth
        );

    truth = new SQPNearClause(3, 5, 0, 0, TYPE.BRACKET, true, 
        false, 
        3);

    executeSingleTokenTest(
        "the quick [brown fox]~3 jumped", 
        2,
        truth
        );

    truth = new SQPNearClause(3, 5, 0, 0, TYPE.BRACKET, true, 
        true, 
        3);

    executeSingleTokenTest(
        "the quick [brown fox]~>3 jumped", 
        2,
        truth
        );

    //now brackets with boosts
    truth = new SQPNearClause(3, 5, 0, 0, TYPE.BRACKET, false, 
        SQPNearClause.UNSPECIFIED_IN_ORDER, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);
    ((SQPBoostableToken)truth).setBoost(2.5f);

    executeSingleTokenTest(
        "the quick [brown fox]^2.5 jumped", 
        2,
        truth
        );

    truth = new SQPNearClause(3, 5, 0, 0, TYPE.BRACKET, true, 
        false, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);
    ((SQPBoostableToken)truth).setBoost(2.5f);

    executeSingleTokenTest(
        "the quick [brown fox]~^2.5 jumped", 
        2,
        truth 
        );

    truth = new SQPNearClause(3, 5, 0, 0, TYPE.BRACKET, true, 
        true, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);
    ((SQPBoostableToken)truth).setBoost(2.5f);

    executeSingleTokenTest(
        "the quick [brown fox]~>^2.5 jumped", 
        2,
        truth
        );

    truth = new SQPNearClause(3, 5, 0, 0, TYPE.BRACKET, true, 
        false, 
        3);
    ((SQPBoostableToken)truth).setBoost(2.5f);

    executeSingleTokenTest(
        "the quick [brown fox]~3^2.5 jumped", 
        2,
        truth
        );

    truth = new SQPNearClause(3, 5, 0, 0, TYPE.BRACKET, true, 
        true, 
        3);
    ((SQPBoostableToken)truth).setBoost(2.5f);

    executeSingleTokenTest(
        "the quick [brown fox]~>3^2.5 jumped", 
        2,
        truth
        );

    //now test crazy apparent mods on first dquote
    SQPTerm tTerm = new SQPTerm("~2", false);

    executeSingleTokenTest(
        "the \"~2 quick brown\"", 
        2,
        tTerm
        );

    tTerm = new SQPTerm("!~2", false);

    executeSingleTokenTest(
        "the \"!~2 quick brown\"", 
        2,
        tTerm
        );
  }

  public void testBoosts() throws Exception {
    SQPToken truth = new SQPTerm("apache", false);
    ((SQPBoostableToken)truth).setBoost(4.0f);

    executeSingleTokenTest(
        "apache^4", 
        0,
        truth
        );

    truth = new SQPRegexTerm("apache");
    ((SQPBoostableToken)truth).setBoost(4.0f);

    executeSingleTokenTest(
        "/apache/^4", 
        0,
        truth
        );

    truth = new SQPRangeTerm("abc", "efg", true, true);
    ((SQPBoostableToken)truth).setBoost(4.0f);

    executeSingleTokenTest(
        "the [abc TO efg]^4 cat", 
        1,
        truth
        );

    truth = new SQPTerm("apache", false);
    ((SQPBoostableToken)truth).setBoost(0.4f);

    executeSingleTokenTest(
        "apache^.4", 
        0,
        truth
        );

    executeSingleTokenTest(
        "apache^0.4", 
        0,
        truth
        );

    //negatives should not be parsed as boosts, boost for these should be UNSPECIFIED_BOOST
    executeSingleTokenTest(
        "apache^-4", 
        0,
        new SQPTerm("apache^-4", false)
        );

    executeSingleTokenTest(
        "apache^-.4", 
        0,
        new SQPTerm("apache^-.4", false)
        );

    executeSingleTokenTest(
        "apache^-0.4", 
        0,
        new SQPTerm("apache^-0.4", false)
        );
  }

  public void testNotNear() throws ParseException {
    SQPNotNearClause truth = new SQPNotNearClause(3, 5, TYPE.QUOTE, 
        SQPNotNearClause.NOT_DEFAULT,  SQPNotNearClause.NOT_DEFAULT);

    executeSingleTokenTest(
        "the quick \"brown fox\"!~ jumped", 
        2,
        truth
        );

    truth = new SQPNotNearClause(3, 5, TYPE.QUOTE, 
        3, 3);
    executeSingleTokenTest(
        "the quick \"brown fox\"!~3 jumped", 
        2,
        truth
        );

    truth = new SQPNotNearClause(3, 5, TYPE.QUOTE, 
        3, 4);
    executeSingleTokenTest(
        "the quick \"brown fox\"!~3,4 jumped", 
        2,
        truth
        );

    truth = new SQPNotNearClause(3, 5, TYPE.BRACKET, 
        SQPNotNearClause.NOT_DEFAULT, 
        SQPNotNearClause.NOT_DEFAULT);

    executeSingleTokenTest(
        "the quick [brown fox]!~ jumped", 
        2,
        truth
        );

    truth = new SQPNotNearClause(3, 5, TYPE.BRACKET, 
        3, 
        3);
    executeSingleTokenTest(
        "the quick [brown fox]!~3 jumped", 
        2,
        truth
        );

    truth = new SQPNotNearClause(3, 5, TYPE.BRACKET, 
        3, 
        4);
    executeSingleTokenTest(
        "the quick [brown fox]!~3,4 jumped", 
        2,
        truth
        );
  }

  public void testUnescapes() throws ParseException {
    //lexer should unescape field names
    //and boolean operators but nothing else
    //the parser may need the escapes for determining type of multiterm
    //and a few other things

    executeSingleTokenTest(
        "the qu\\(ck", 
        1,
        new SQPTerm("qu\\(ck", false)
        );

    executeSingleTokenTest(
        "the qu\\[ck", 
        1,
        new SQPTerm("qu\\[ck", false)
        );

    executeSingleTokenTest(
        "the qu\\+ck", 
        1,
        new SQPTerm("qu\\+ck", false)
        );
    executeSingleTokenTest(
        "the qu\\-ck", 
        1,
        new SQPTerm("qu\\-ck", false)
        );

    executeSingleTokenTest(
        "the qu\\\\ck", 
        1,
        new SQPTerm("qu\\\\ck", false)
        );

    executeSingleTokenTest(
        "the qu\\ ck", 
        1,
        new SQPTerm("qu\\ ck", false)
        );

    executeSingleTokenTest(
        "the field\\: quick", 
        1,
        new SQPTerm("field\\:", false)
        );

    executeSingleTokenTest(
        "the quick \\AND nimble", 
        2,
        new SQPTerm("AND", false)
        );

    executeSingleTokenTest(
        "the quick \\NOT nimble", 
        2,
        new SQPTerm("NOT", false)
        );

    executeSingleTokenTest(
        "the quick \\OR nimble", 
        2,
        new SQPTerm("OR", false)
        );

    executeSingleTokenTest(
        "the \\+ (quick -nimble)", 
        1,
        new SQPTerm("\\+", false)
        );
  }

  public void testBoolean() throws Exception {

    executeSingleTokenTest(
        "the quick AND nimble", 
        2,
        new SQPBooleanOpToken(SpanQueryParserBase.CONJ_AND)
        );

    executeSingleTokenTest(
        "the quick NOT nimble", 
        2,
        new SQPBooleanOpToken(SpanQueryParserBase.MOD_NOT)
        );

    executeSingleTokenTest(
        "the (quick NOT nimble) fox", 
        3,
        new SQPBooleanOpToken(SpanQueryParserBase.MOD_NOT)
        );


    //not sure this is the right behavior
    //lexer knows when it is in a near clause and doesn't parse
    //boolean operators
    executeSingleTokenTest(
        "the [quick NOT nimble] fox", 
        3,
        new SQPTerm("NOT", false)
        );

    executeSingleTokenTest(
        "the +quick +nimble", 
        1,
        new SQPBooleanOpToken(SpanQueryParserBase.MOD_REQ)
        );

    executeSingleTokenTest(
        "the +quick -nimble", 
        3,
        new SQPBooleanOpToken(SpanQueryParserBase.MOD_NOT)
        );

    executeSingleTokenTest(
        "the +(quick -nimble)", 
        1,
        new SQPBooleanOpToken(SpanQueryParserBase.MOD_REQ)
        );

    executeSingleTokenTest(
        "the +(quick -nimble)", 
        4,
        new SQPBooleanOpToken(SpanQueryParserBase.MOD_NOT)
        );

    //test non-operators
    executeSingleTokenTest(
        "the 10-22+02", 
        1,
        new SQPTerm("10-22+02", false)
        );
  }

  public void testRange() throws ParseException {
    executeSingleTokenTest(
        "the [abc TO def] cat", 
        1,
        new SQPRangeTerm("abc", "def", true, true)
        );

    executeSingleTokenTest(
        "the [quick brown ([abc TO def] fox)] cat", 
        5,
        new SQPRangeTerm("abc", "def", true, true)
        );

    SQPNearClause nearClause = new SQPNearClause(2, 5, 
        0, 0, TYPE.BRACKET, false, 
        SQPNearClause.UNSPECIFIED_IN_ORDER, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);


    executeSingleTokenTest(
        "the [abc to def] cat", 
        1,
        nearClause
        );

    executeSingleTokenTest(
        "the [abc \\TO def] cat", 
        1,
        nearClause
        );

    nearClause = new SQPNearClause(1, 4, 
        0, 0, TYPE.BRACKET, false, 
        SQPNearClause.UNSPECIFIED_IN_ORDER, 
        SpanQueryParserBase.UNSPECIFIED_SLOP);
    executeSingleTokenTest(
        "[abc to def]", 
        0,
        nearClause
        );

    //not ranges
    nearClause = new SQPNearClause(2, 5, 
        0, 0, TYPE.BRACKET, true, 
        false, 
        3);

    executeSingleTokenTest(
        "the [abc to def]~3 cat", 
        1,
        nearClause
        );

    executeSingleTokenTest(
        "the [abc TO def]~3 cat", 
        1,
        nearClause
        );

    SQPNotNearClause notNear = new SQPNotNearClause(2, 
        5, TYPE.BRACKET, 
        1, 
        2);

    executeSingleTokenTest(
        "the [abc TO def]!~1,2 cat", 
        1,
        notNear
        );


    //terms in range queries aren't checked for multiterm-hood
    executeSingleTokenTest(
        "the [abc~2 TO def] cat", 
        1,
        new SQPRangeTerm("abc~2", "def", true, true)
        );

    //terms in range queries aren't checked for multiterm-hood
    executeSingleTokenTest(
        "the [abc* TO *def] cat", 
        1,
        new SQPRangeTerm("abc*", "*def", true, true)
        );

    //\\TO is not unescaped currently
    executeSingleTokenTest(
        "the [abc \\TO def] cat", 
        3,
        new SQPTerm("\\TO", false)
        );

    //exception
    executeSingleTokenTest(
        "the [abc \\TO def] cat", 
        3,
        new SQPTerm("\\TO", false)
        );

    testParseException("some stuff [abc def ghi} some other");
    testParseException("some stuff {abc def ghi] some other");
    testParseException("some stuff {abc def ghi} some other");

    testParseException("some stuff [abc} some other");
    testParseException("some stuff [abc \\TO ghi} some other");

    //can't have modifiers on range queries
    testParseException("some stuff [abc TO ghi}~2 some other");
    testParseException("some stuff {abc TO ghi]~2 some other");
  }

  public void testBeyondBMP() throws Exception {
    //this blew out regex during development
    String bigChar = new String(new int[]{100000}, 0, 1);
    String s = "ab"+bigChar+"cd";
    executeSingleTokenTest(
        s, 
        0,
        new SQPTerm(s, false)
        );
  }
  
  private void executeSingleTokenTest(String q, int targetOffset, SQPToken truth) 
      throws ParseException {
    List<SQPToken> tokens = lexer.getTokens(q);
    SQPToken target = tokens.get(targetOffset);
    if (truth instanceof SQPBoostableToken && target instanceof SQPBoostableToken) {
      assertEquals(((SQPBoostableToken)truth).getBoost(), ((SQPBoostableToken)target).getBoost(), 0.0001f);
    }
    assertEquals(truth, target);
  }

  private void testParseException(String qString) {
    boolean ex = false;
    try{
      executeSingleTokenTest(
          qString, 
          0,
          new SQPTerm("anything", false)
          );
    } catch (ParseException e) {
      ex = true;
    }

    assertTrue("ParseException: "+qString, ex);
  }
}

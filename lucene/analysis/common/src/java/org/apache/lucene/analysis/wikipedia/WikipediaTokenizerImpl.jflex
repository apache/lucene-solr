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
package org.apache.lucene.analysis.wikipedia;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * JFlex-generated tokenizer that is aware of Wikipedia syntax.
 */
@SuppressWarnings("fallthrough")
%%

%class WikipediaTokenizerImpl
%unicode 3.0
%integer
%function getNextToken
%pack
%char
%buffer 4096

%{

public static final int ALPHANUM          = WikipediaTokenizer.ALPHANUM_ID;
public static final int APOSTROPHE        = WikipediaTokenizer.APOSTROPHE_ID;
public static final int ACRONYM           = WikipediaTokenizer.ACRONYM_ID;
public static final int COMPANY           = WikipediaTokenizer.COMPANY_ID;
public static final int EMAIL             = WikipediaTokenizer.EMAIL_ID;
public static final int HOST              = WikipediaTokenizer.HOST_ID;
public static final int NUM               = WikipediaTokenizer.NUM_ID;
public static final int CJ                = WikipediaTokenizer.CJ_ID;
public static final int INTERNAL_LINK     = WikipediaTokenizer.INTERNAL_LINK_ID;
public static final int EXTERNAL_LINK     = WikipediaTokenizer.EXTERNAL_LINK_ID;
public static final int CITATION          = WikipediaTokenizer.CITATION_ID;
public static final int CATEGORY          = WikipediaTokenizer.CATEGORY_ID;
public static final int BOLD              = WikipediaTokenizer.BOLD_ID;
public static final int ITALICS           = WikipediaTokenizer.ITALICS_ID;
public static final int BOLD_ITALICS      = WikipediaTokenizer.BOLD_ITALICS_ID;
public static final int HEADING           = WikipediaTokenizer.HEADING_ID;
public static final int SUB_HEADING       = WikipediaTokenizer.SUB_HEADING_ID;
public static final int EXTERNAL_LINK_URL = WikipediaTokenizer.EXTERNAL_LINK_URL_ID;


private int currentTokType;
private int numBalanced = 0;
private int positionInc = 1;
private int numLinkToks = 0;
//Anytime we start a new on a Wiki reserved token (category, link, etc.) this value will be 0, otherwise it will be the number of tokens seen
//this can be useful for detecting when a new reserved token is encountered
//see https://issues.apache.org/jira/browse/LUCENE-1133
private int numWikiTokensSeen = 0;

public static final String [] TOKEN_TYPES = WikipediaTokenizer.TOKEN_TYPES;

/**
Returns the number of tokens seen inside a category or link, etc.
@return the number of tokens seen inside the context of wiki syntax.
**/
public final int getNumWikiTokensSeen(){
  return numWikiTokensSeen;
}

public final int yychar()
{
    return yychar;
}

public final int getPositionIncrement(){
  return positionInc;
}

/**
 * Fills Lucene token with the current token text.
 */
final void getText(CharTermAttribute t) {
  t.copyBuffer(zzBuffer, zzStartRead, zzMarkedPos-zzStartRead);
}

final int setText(StringBuilder buffer){
  int length = zzMarkedPos - zzStartRead;
  buffer.append(zzBuffer, zzStartRead, length);
  return length;
}

final void reset() {
  currentTokType = 0;
  numBalanced = 0;
  positionInc = 1;
  numLinkToks = 0;
  numWikiTokensSeen = 0;
}


%}

// basic word: a sequence of digits & letters
ALPHANUM   = ({LETTER}|{DIGIT}|{KOREAN})+

// internal apostrophes: O'Reilly, you're, O'Reilly's
// use a post-filter to remove possesives
APOSTROPHE =  {ALPHA} ("'" {ALPHA})+

// acronyms: U.S.A., I.B.M., etc.
// use a post-filter to remove dots
ACRONYM    =  {ALPHA} "." ({ALPHA} ".")+

// company names like AT&T and Excite@Home.
COMPANY    =  {ALPHA} ("&"|"@") {ALPHA}

// email addresses
EMAIL      =  {ALPHANUM} (("."|"-"|"_") {ALPHANUM})* "@" {ALPHANUM} (("."|"-") {ALPHANUM})+

// hostname
HOST       =  {ALPHANUM} ((".") {ALPHANUM})+

// floating point, serial, model numbers, ip addresses, etc.
// every other segment must have at least one digit
NUM        = ({ALPHANUM} {P} {HAS_DIGIT}
           | {DIGIT}+ {P} {DIGIT}+
           | {HAS_DIGIT} {P} {ALPHANUM}
           | {ALPHANUM} ({P} {HAS_DIGIT} {P} {ALPHANUM})+
           | {HAS_DIGIT} ({P} {ALPHANUM} {P} {HAS_DIGIT})+
           | {ALPHANUM} {P} {HAS_DIGIT} ({P} {ALPHANUM} {P} {HAS_DIGIT})+
           | {HAS_DIGIT} {P} {ALPHANUM} ({P} {HAS_DIGIT} {P} {ALPHANUM})+)

TAGS = "<"\/?{ALPHANUM}({WHITESPACE}*{ALPHANUM}=\"{ALPHANUM}\")*">"

// punctuation
P           = ("_"|"-"|"/"|"."|",")

// at least one digit
HAS_DIGIT  =
    ({LETTER}|{DIGIT})*
    {DIGIT}
    ({LETTER}|{DIGIT})*

ALPHA      = ({LETTER})+


LETTER     = [\u0041-\u005a\u0061-\u007a\u00c0-\u00d6\u00d8-\u00f6\u00f8-\u00ff\u0100-\u1fff\uffa0-\uffdc]

DIGIT      = [\u0030-\u0039\u0660-\u0669\u06f0-\u06f9\u0966-\u096f\u09e6-\u09ef\u0a66-\u0a6f\u0ae6-\u0aef\u0b66-\u0b6f\u0be7-\u0bef\u0c66-\u0c6f\u0ce6-\u0cef\u0d66-\u0d6f\u0e50-\u0e59\u0ed0-\u0ed9\u1040-\u1049]

KOREAN     = [\uac00-\ud7af\u1100-\u11ff]

// Chinese, Japanese
CJ         = [\u3040-\u318f\u3100-\u312f\u3040-\u309F\u30A0-\u30FF\u31F0-\u31FF\u3300-\u337f\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\uff65-\uff9f]

WHITESPACE = \r\n | [ \r\n\t\f]

//Wikipedia
DOUBLE_BRACKET = "["{2}
DOUBLE_BRACKET_CLOSE = "]"{2}
DOUBLE_BRACKET_CAT = "["{2}":"?"Category:"
EXTERNAL_LINK = "["
TWO_SINGLE_QUOTES = "'"{2}
CITATION = "<ref>"
CITATION_CLOSE = "</ref>"
INFOBOX = {DOUBLE_BRACE}("I"|"i")nfobox_

DOUBLE_BRACE = "{"{2}
DOUBLE_BRACE_CLOSE = "}"{2}
PIPE = "|"
DOUBLE_EQUALS = "="{2}


%state CATEGORY_STATE
%state INTERNAL_LINK_STATE
%state EXTERNAL_LINK_STATE

%state TWO_SINGLE_QUOTES_STATE
%state THREE_SINGLE_QUOTES_STATE
%state FIVE_SINGLE_QUOTES_STATE
%state DOUBLE_EQUALS_STATE
%state DOUBLE_BRACE_STATE
%state STRING

%%

<YYINITIAL>{ALPHANUM}                                                     {positionInc = 1; return ALPHANUM; }
<YYINITIAL>{APOSTROPHE}                                                   {positionInc = 1; return APOSTROPHE; }
<YYINITIAL>{ACRONYM}                                                      {positionInc = 1; return ACRONYM; }
<YYINITIAL>{COMPANY}                                                      {positionInc = 1; return COMPANY; }
<YYINITIAL>{EMAIL}                                                        {positionInc = 1; return EMAIL; }
<YYINITIAL>{NUM}                                                          {positionInc = 1; return NUM; }
<YYINITIAL>{HOST}                                                         {positionInc = 1; return HOST; }
<YYINITIAL>{CJ}                                                           {positionInc = 1; return CJ; }

//wikipedia
<YYINITIAL>{
  //First {ALPHANUM} is always the link, set positioninc to 1 for double bracket, but then inside the internal link state
  //set it to 0 for the next token, such that the link and the first token are in the same position, but then subsequent
  //tokens within the link are incremented
  {DOUBLE_BRACKET} {numWikiTokensSeen = 0; positionInc = 1; currentTokType = INTERNAL_LINK; yybegin(INTERNAL_LINK_STATE);/* Break so we don't hit fall-through warning: */ break;}
  {DOUBLE_BRACKET_CAT} {numWikiTokensSeen = 0; positionInc = 1; currentTokType = CATEGORY; yybegin(CATEGORY_STATE);/* Break so we don't hit fall-through warning: */ break;}
  {EXTERNAL_LINK} {numWikiTokensSeen = 0; positionInc = 1; currentTokType = EXTERNAL_LINK_URL; yybegin(EXTERNAL_LINK_STATE);/* Break so we don't hit fall-through warning: */ break;}
  {TWO_SINGLE_QUOTES} {numWikiTokensSeen = 0; positionInc = 1; if (numBalanced == 0){numBalanced++;yybegin(TWO_SINGLE_QUOTES_STATE);} else{numBalanced = 0;}/* Break so we don't hit fall-through warning: */ break;}
  {DOUBLE_EQUALS} {numWikiTokensSeen = 0; positionInc = 1; yybegin(DOUBLE_EQUALS_STATE);/* Break so we don't hit fall-through warning: */ break;}
  {DOUBLE_BRACE} {numWikiTokensSeen = 0; positionInc = 1; currentTokType = CITATION; yybegin(DOUBLE_BRACE_STATE);/* Break so we don't hit fall-through warning: */ break;}
  {CITATION} {numWikiTokensSeen = 0; positionInc = 1; currentTokType = CITATION; yybegin(DOUBLE_BRACE_STATE);/* Break so we don't hit fall-through warning: */ break;}
//ignore
  [^] |{INFOBOX}                                               {numWikiTokensSeen = 0;  positionInc = 1; /* Break so we don't hit fall-through warning: */ break;}
}

<INTERNAL_LINK_STATE>{
//First {ALPHANUM} is always the link, set position to 0 for these
//This is slightly different from EXTERNAL_LINK_STATE because that one has an explicit grammar for capturing the URL
  {ALPHANUM} {yybegin(INTERNAL_LINK_STATE); numWikiTokensSeen++; return currentTokType;}
  {DOUBLE_BRACKET_CLOSE} {numLinkToks = 0; yybegin(YYINITIAL); /* Break so we don't hit fall-through warning: */ break;}
  //ignore
  [^]                                               { positionInc = 1; /* Break so we don't hit fall-through warning: */ break;}
}

<EXTERNAL_LINK_STATE>{
//increment the link token, but then don't increment the tokens after that which are still in the link
  ("http://"|"https://"){HOST}("/"?({ALPHANUM}|{P}|\?|"&"|"="|"#")*)* {positionInc = 1; numWikiTokensSeen++; yybegin(EXTERNAL_LINK_STATE); return currentTokType;}
  {ALPHANUM} {if (numLinkToks == 0){positionInc = 0;} else{positionInc = 1;} numWikiTokensSeen++; currentTokType = EXTERNAL_LINK; yybegin(EXTERNAL_LINK_STATE); numLinkToks++; return currentTokType;}
  "]" {numLinkToks = 0; positionInc = 0; yybegin(YYINITIAL); /* Break so we don't hit fall-through warning: */ break;}
  {WHITESPACE}                                               { positionInc = 1; /* Break so we don't hit fall-through warning: */ break;}
}

<CATEGORY_STATE>{
  {ALPHANUM} {yybegin(CATEGORY_STATE); numWikiTokensSeen++; return currentTokType;}
  {DOUBLE_BRACKET_CLOSE} {yybegin(YYINITIAL);/* Break so we don't hit fall-through warning: */ break;}
  //ignore
  [^]                                               { positionInc = 1; /* Break so we don't hit fall-through warning: */ break;}
}
//italics
<TWO_SINGLE_QUOTES_STATE>{
  "'" {currentTokType = BOLD;  yybegin(THREE_SINGLE_QUOTES_STATE); /* Break so we don't hit fall-through warning: */ break;}
   "'''" {currentTokType = BOLD_ITALICS;  yybegin(FIVE_SINGLE_QUOTES_STATE); /* Break so we don't hit fall-through warning: */ break;}
   {ALPHANUM} {currentTokType = ITALICS; numWikiTokensSeen++;  yybegin(STRING); return currentTokType;/*italics*/}
   //we can have links inside, let those override
   {DOUBLE_BRACKET} {currentTokType = INTERNAL_LINK; numWikiTokensSeen = 0; yybegin(INTERNAL_LINK_STATE); /* Break so we don't hit fall-through warning: */ break;}
   {DOUBLE_BRACKET_CAT} {currentTokType = CATEGORY; numWikiTokensSeen = 0; yybegin(CATEGORY_STATE); /* Break so we don't hit fall-through warning: */ break;}
   {EXTERNAL_LINK} {currentTokType = EXTERNAL_LINK; numWikiTokensSeen = 0; yybegin(EXTERNAL_LINK_STATE); /* Break so we don't hit fall-through warning: */ break;}

   //ignore
   [^]                                               { /* Break so we don't hit fall-through warning: */ break;/* ignore */ }
}
//bold
<THREE_SINGLE_QUOTES_STATE>{
  {ALPHANUM} {yybegin(STRING); numWikiTokensSeen++; return currentTokType;}
  //we can have links inside, let those override
   {DOUBLE_BRACKET} {currentTokType = INTERNAL_LINK; numWikiTokensSeen = 0; yybegin(INTERNAL_LINK_STATE); /* Break so we don't hit fall-through warning: */ break;}
   {DOUBLE_BRACKET_CAT} {currentTokType = CATEGORY; numWikiTokensSeen = 0; yybegin(CATEGORY_STATE); /* Break so we don't hit fall-through warning: */ break;}
   {EXTERNAL_LINK} {currentTokType = EXTERNAL_LINK; numWikiTokensSeen = 0; yybegin(EXTERNAL_LINK_STATE); /* Break so we don't hit fall-through warning: */ break;}

   //ignore
   [^]                                               { /* Break so we don't hit fall-through warning: */ break;/* ignore */ }

}
//bold italics
<FIVE_SINGLE_QUOTES_STATE>{
  {ALPHANUM} {yybegin(STRING); numWikiTokensSeen++; return currentTokType;}
  //we can have links inside, let those override
   {DOUBLE_BRACKET} {currentTokType = INTERNAL_LINK; numWikiTokensSeen = 0;  yybegin(INTERNAL_LINK_STATE); /* Break so we don't hit fall-through warning: */ break;}
   {DOUBLE_BRACKET_CAT} {currentTokType = CATEGORY; numWikiTokensSeen = 0; yybegin(CATEGORY_STATE); /* Break so we don't hit fall-through warning: */ break;}
   {EXTERNAL_LINK} {currentTokType = EXTERNAL_LINK; numWikiTokensSeen = 0; yybegin(EXTERNAL_LINK_STATE); /* Break so we don't hit fall-through warning: */ break;}

   //ignore
   [^]                                               { /* Break so we don't hit fall-through warning: */ break;/* ignore */ }
}

<DOUBLE_EQUALS_STATE>{
 "=" {currentTokType = SUB_HEADING; numWikiTokensSeen = 0; yybegin(STRING); /* Break so we don't hit fall-through warning: */ break;}
 {ALPHANUM} {currentTokType = HEADING; yybegin(DOUBLE_EQUALS_STATE); numWikiTokensSeen++; return currentTokType;}
 {DOUBLE_EQUALS} {yybegin(YYINITIAL); /* Break so we don't hit fall-through warning: */ break;}
  //ignore
  [^]                                               { /* Break so we don't hit fall-through warning: */ break;/* ignore */ }
}

<DOUBLE_BRACE_STATE>{
  {ALPHANUM} {yybegin(DOUBLE_BRACE_STATE); numWikiTokensSeen = 0; return currentTokType;}
  {DOUBLE_BRACE_CLOSE} {yybegin(YYINITIAL); /* Break so we don't hit fall-through warning: */ break;}
  {CITATION_CLOSE} {yybegin(YYINITIAL); /* Break so we don't hit fall-through warning: */ break;}
  //ignore
  [^]                                               { /* Break so we don't hit fall-through warning: */ break;/* ignore */ }
}

<STRING> {
  "'''''" {numBalanced = 0;currentTokType = ALPHANUM; yybegin(YYINITIAL); /* Break so we don't hit fall-through warning: */ break;/*end bold italics*/}
  "'''" {numBalanced = 0;currentTokType = ALPHANUM;yybegin(YYINITIAL); /* Break so we don't hit fall-through warning: */ break;/*end bold*/}
  "''" {numBalanced = 0;currentTokType = ALPHANUM; yybegin(YYINITIAL); /* Break so we don't hit fall-through warning: */ break;/*end italics*/}
  "===" {numBalanced = 0;currentTokType = ALPHANUM; yybegin(YYINITIAL); /* Break so we don't hit fall-through warning: */ break;/*end sub header*/}
  {ALPHANUM} {yybegin(STRING); numWikiTokensSeen++; return currentTokType;/* STRING ALPHANUM*/}
  //we can have links inside, let those override
   {DOUBLE_BRACKET} {numBalanced = 0; numWikiTokensSeen = 0; currentTokType = INTERNAL_LINK;yybegin(INTERNAL_LINK_STATE); /* Break so we don't hit fall-through warning: */ break;}
   {DOUBLE_BRACKET_CAT} {numBalanced = 0; numWikiTokensSeen = 0; currentTokType = CATEGORY;yybegin(CATEGORY_STATE); /* Break so we don't hit fall-through warning: */ break;}
   {EXTERNAL_LINK} {numBalanced = 0; numWikiTokensSeen = 0; currentTokType = EXTERNAL_LINK;yybegin(EXTERNAL_LINK_STATE); /* Break so we don't hit fall-through warning: */ break;}


  {PIPE} {yybegin(STRING); return currentTokType;/*pipe*/}

  [^]                                              { /* Break so we don't hit fall-through warning: */ break;/* ignore STRING */ }
}




/*
{INTERNAL_LINK}                                                { return curentTokType; }

{CITATION}                                                { return currentTokType; }
{CATEGORY}                                                { return currentTokType; }

{BOLD}                                                { return currentTokType; }
{ITALICS}                                                { return currentTokType; }
{BOLD_ITALICS}                                                { return currentTokType; }
{HEADING}                                                { return currentTokType; }
{SUB_HEADING}                                                { return currentTokType; }

*/
//end wikipedia

/** Ignore the rest */
[^] | {TAGS}                                          { /* Break so we don't hit fall-through warning: */ break;/* ignore */ }


//INTERNAL_LINK = "["{2}({ALPHANUM}+{WHITESPACE}*)+"]"{2}
//EXTERNAL_LINK = "["http://"{HOST}.*?"]"
//CITATION = "{"{2}({ALPHANUM}+{WHITESPACE}*)+"}"{2}
//CATEGORY = "["{2}"Category:"({ALPHANUM}+{WHITESPACE}*)+"]"{2}
//CATEGORY_COLON = "["{2}":Category:"({ALPHANUM}+{WHITESPACE}*)+"]"{2}
//BOLD = '''({ALPHANUM}+{WHITESPACE}*)+'''
//ITALICS = ''({ALPHANUM}+{WHITESPACE}*)+''
//BOLD_ITALICS = '''''({ALPHANUM}+{WHITESPACE}*)+'''''
//HEADING = "="{2}({ALPHANUM}+{WHITESPACE}*)+"="{2}
//SUB_HEADING ="="{3}({ALPHANUM}+{WHITESPACE}*)+"="{3}

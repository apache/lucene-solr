package org.apache.lucene.analysis.ko;

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

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

%%

%class KoreanTokenizerImpl
%unicode
%integer
%function getNextToken
%pack
%char

%{

public static final int ALPHANUM          = 0;
public static final int APOSTROPHE        = 1;
public static final int ACRONYM           = 2;
public static final int COMPANY           = 3;
public static final int EMAIL             = 4;
public static final int HOST              = 5;
public static final int NUM               = 6;
public static final int CJ                = 7;
/**
 * @deprecated this solves a bug where HOSTs that end with '.' are identified
 *             as ACRONYMs. It is deprecated and will be removed in the next
 *             release.
 */
public static final int ACRONYM_DEP       = 8;
public static final int KOREAN            = 9;
public static final int CHINESE            = 10;

public static final String [] TOKEN_TYPES = new String [] {
    "<ALPHANUM>",
    "<APOSTROPHE>",
    "<ACRONYM>",
    "<COMPANY>",
    "<EMAIL>",
    "<HOST>",
    "<NUM>", 
    "<CJ>",   
    "<ACRONYM_DEP>",
    "<KOREAN>" ,
    "<CHINESE>"     
};

public final int yychar() {
  return yychar;
}

/**
 * Fills Lucene token with the current token text.
 */
final void getText(CharTermAttribute t) {
  t.copyBuffer(zzBuffer, zzStartRead, zzMarkedPos-zzStartRead);
}
%}

// korean word: a sequence of digits & letters & 
KOREAN        = ({LETTER}|{NUM}|{DIGIT})* {HANLETTER}+ ({LETTER}|{DIGIT})* {HANLETTER}*

// basic word: a sequence of digits & letters
ALPHANUM      = ({LETTER}|{DIGIT})+

// chinese word: a sequence of digits & letters & 
CHINESE       = {CHINESELETTER}+ ({LETTER}|{DIGIT})*

// internal apostrophes: O'Reilly, you're, O'Reilly's
// use a post-filter to remove possesives
APOSTROPHE    =  {ALPHA} ("'" {ALPHA})+

// acronyms: U.S.A., I.B.M., etc.
// use a post-filter to remove dots
ACRONYM       =  {LETTER} "." ({LETTER} ".")+

ACRONYM_DEP   = {ALPHANUM} "." ({ALPHANUM} ".")+

// company names like AT&T and Excite@Home.
COMPANY       =  {ALPHA} ("&"|"@") {ALPHA}

// email addresses
EMAIL         =  {ALPHANUM} (("."|"-"|"_") {ALPHANUM})* "@" {ALPHANUM} (("."|"-") {ALPHANUM})+

// hostname
HOST          =  {ALPHANUM} ((".") {ALPHANUM})+

// floating point, serial, model numbers, ip addresses, etc.
// every other segment must have at least one digit
NUM           = ({ALPHANUM} {P} {HAS_DIGIT}
              | "." {DIGIT}
              | {DIGIT} "."        
              | {HAS_DIGIT} {P} {ALPHANUM}
              | {ALPHANUM} ({P} {HAS_DIGIT} {P} {ALPHANUM})+
              | {HAS_DIGIT} ({P} {ALPHANUM} {P} {HAS_DIGIT})+
              | {ALPHANUM} {P} {HAS_DIGIT} ({P} {ALPHANUM} {P} {HAS_DIGIT})+
              | {HAS_DIGIT} {P} {ALPHANUM} ({P} {HAS_DIGIT} {P} {ALPHANUM})+)

// punctuation
P             = ("_"|"-"|"/"|"."|",")

// at least one digit
HAS_DIGIT     =
    ({LETTER}|{DIGIT})*
    {DIGIT}
    ({LETTER}|{DIGIT})*

ALPHA         = ({LETTER})+


LETTER        = [\u0041-\u005a\u0061-\u007a\u00c0-\u00d6\u00d8-\u00f6\u00f8-\u00ff\u0100-\u1fff\uffa0-\uffdc]

DIGIT         = [\u0030-\u0039\u0660-\u0669\u06f0-\u06f9\u0966-\u096f\u09e6-\u09ef\u0a66-\u0a6f\u0ae6-\u0aef\u0b66-\u0b6f\u0be7-\u0bef\u0c66-\u0c6f\u0ce6-\u0cef\u0d66-\u0d6f\u0e50-\u0e59\u0ed0-\u0ed9\u1040-\u1049]

HANLETTER     = [\uac00-\ud7af\u1100-\u11ff]

CHINESELETTER = [\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]

// Chinese, Japanese
CJ            = ([\u3040-\u318f\u3100-\u312f\u3040-\u309F\u30A0-\u30FF\u31F0-\u31FF\u3300-\u337f\uff65-\uff9f])+

WHITESPACE    = \r\n | [ \r\n\t\f]

%%

{KOREAN}                                                       { return KOREAN; }
{CHINESE}                                                      { return CHINESE; }
{ALPHANUM}                                                     { return ALPHANUM; }
{APOSTROPHE}                                                   { return APOSTROPHE; }
{ACRONYM}                                                      { return ACRONYM; }
{COMPANY}                                                      { return COMPANY; }
{EMAIL}                                                        { return EMAIL; }
{HOST}                                                         { return HOST; }
{NUM}                                                          { return NUM; }
{CJ}                                                           { return CJ; }
{ACRONYM_DEP}                                                  { return ACRONYM_DEP; }


/** Ignore the rest */
. | {WHITESPACE}                                               { /* ignore */ }

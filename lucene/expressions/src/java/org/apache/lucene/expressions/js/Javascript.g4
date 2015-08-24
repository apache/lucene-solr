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

/*
 * ANTLRv4 grammar for the Lucene expressions language
 */

grammar Javascript;

compile
    : expression EOF
    ;

expression
    : LP expression RP                                          # precedence
    | ( OCTAL | HEX | DECIMAL )                                 # numeric
    | VARIABLE ( LP (expression (COMMA expression)*)? RP )?     # external
    | ( BOOLNOT | BWNOT | ADD | SUB ) expression                # unary
    | expression ( MUL | DIV | REM ) expression                 # muldiv
    | expression ( ADD | SUB ) expression                       # addsub
    | expression ( LSH | RSH | USH ) expression                 # bwshift
    | expression ( LT | LTE | GT | GTE ) expression             # boolcomp
    | expression ( EQ | NE ) expression                         # booleqne
    | expression BWAND expression                               # bwand
    | expression BWXOR expression                               # bwxor
    | expression BWOR expression                                # bwor
    | expression BOOLAND expression                             # booland
    | expression BOOLOR expression                              # boolor
    | <assoc=right> expression COND expression COLON expression # conditional
    ;

LP:      [(];
RP:      [)];
COMMA:   [,];
BOOLNOT: [!];
BWNOT:   [~];
MUL:     [*];
DIV:     [/];
REM:     [%];
ADD:     [+];
SUB:     [\-];
LSH:     '<<';
RSH:     '>>';
USH:     '>>>';
LT:      [<];
LTE:     '<=';
GT:      [>];
GTE:     '>=';
EQ:      '==';
NE:      '!=';
BWAND:   [&];
BWXOR:   [^];
BWOR:    [|];
BOOLAND: '&&';
BOOLOR:  '||';
COND:    [?];
COLON:   [:];

WS: [ \t\n\r]+ -> skip;

VARIABLE: ID ARRAY* ( [.] ID ARRAY* )*;
fragment ARRAY: [[] ( STRING | INTEGER ) [\]];
fragment ID: [_$a-zA-Z] [_$a-zA-Z0-9]*;
fragment STRING
    : ['] ( '\\\'' | '\\\\' | ~[\\'] )*? [']
    | ["] ( '\\"' | '\\\\' | ~[\\"] )*? ["]
    ;

OCTAL: [0] [0-7]+;
HEX: [0] [xX] [0-9a-fA-F]+;
DECIMAL: ( INTEGER ( [.] [0-9]* )? | [.] [0-9]+ ) ( [eE] [+\-]? [0-9]+ )?;
fragment INTEGER
    : [0]
    | [1-9] [0-9]*
    ;

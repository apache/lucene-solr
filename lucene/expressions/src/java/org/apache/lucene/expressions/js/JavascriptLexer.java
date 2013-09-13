// ANTLR GENERATED CODE: DO NOT EDIT

package org.apache.lucene.expressions.js;

import java.text.ParseException;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
class JavascriptLexer extends Lexer {
  public static final int EOF=-1;
  public static final int AT_ADD=4;
  public static final int AT_BIT_AND=5;
  public static final int AT_BIT_NOT=6;
  public static final int AT_BIT_OR=7;
  public static final int AT_BIT_SHL=8;
  public static final int AT_BIT_SHR=9;
  public static final int AT_BIT_SHU=10;
  public static final int AT_BIT_XOR=11;
  public static final int AT_BOOL_AND=12;
  public static final int AT_BOOL_NOT=13;
  public static final int AT_BOOL_OR=14;
  public static final int AT_CALL=15;
  public static final int AT_COLON=16;
  public static final int AT_COMMA=17;
  public static final int AT_COMP_EQ=18;
  public static final int AT_COMP_GT=19;
  public static final int AT_COMP_GTE=20;
  public static final int AT_COMP_LT=21;
  public static final int AT_COMP_LTE=22;
  public static final int AT_COMP_NEQ=23;
  public static final int AT_COND_QUE=24;
  public static final int AT_DIVIDE=25;
  public static final int AT_DOT=26;
  public static final int AT_LPAREN=27;
  public static final int AT_MODULO=28;
  public static final int AT_MULTIPLY=29;
  public static final int AT_NEGATE=30;
  public static final int AT_RPAREN=31;
  public static final int AT_SUBTRACT=32;
  public static final int DECIMAL=33;
  public static final int DECIMALDIGIT=34;
  public static final int DECIMALINTEGER=35;
  public static final int EXPONENT=36;
  public static final int HEX=37;
  public static final int HEXDIGIT=38;
  public static final int ID=39;
  public static final int OCTAL=40;
  public static final int OCTALDIGIT=41;
  public static final int WS=42;


  @Override
  public void displayRecognitionError(String[] tokenNames, RecognitionException re) {  
      String message = " unexpected character '" + (char)re.c 
                     + "' at position (" + re.charPositionInLine + ").";
      ParseException parseException = new ParseException(message, re.charPositionInLine);
      parseException.initCause(re);
      throw new RuntimeException(parseException);
  }



  // delegates
  // delegators
  public Lexer[] getDelegates() {
    return new Lexer[] {};
  }

  public JavascriptLexer() {} 
  public JavascriptLexer(CharStream input) {
    this(input, new RecognizerSharedState());
  }
  public JavascriptLexer(CharStream input, RecognizerSharedState state) {
    super(input,state);
  }
  @Override public String getGrammarFileName() { return "src/java/org/apache/lucene/expressions/js/Javascript.g"; }

  // $ANTLR start "AT_ADD"
  public final void mAT_ADD() throws RecognitionException {
    try {
      int _type = AT_ADD;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:25:8: ( '+' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:25:10: '+'
      {
      match('+'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_ADD"

  // $ANTLR start "AT_BIT_AND"
  public final void mAT_BIT_AND() throws RecognitionException {
    try {
      int _type = AT_BIT_AND;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:26:12: ( '&' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:26:14: '&'
      {
      match('&'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_BIT_AND"

  // $ANTLR start "AT_BIT_NOT"
  public final void mAT_BIT_NOT() throws RecognitionException {
    try {
      int _type = AT_BIT_NOT;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:27:12: ( '~' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:27:14: '~'
      {
      match('~'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_BIT_NOT"

  // $ANTLR start "AT_BIT_OR"
  public final void mAT_BIT_OR() throws RecognitionException {
    try {
      int _type = AT_BIT_OR;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:28:11: ( '|' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:28:13: '|'
      {
      match('|'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_BIT_OR"

  // $ANTLR start "AT_BIT_SHL"
  public final void mAT_BIT_SHL() throws RecognitionException {
    try {
      int _type = AT_BIT_SHL;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:29:12: ( '<<' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:29:14: '<<'
      {
      match("<<"); 

      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_BIT_SHL"

  // $ANTLR start "AT_BIT_SHR"
  public final void mAT_BIT_SHR() throws RecognitionException {
    try {
      int _type = AT_BIT_SHR;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:30:12: ( '>>' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:30:14: '>>'
      {
      match(">>"); 

      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_BIT_SHR"

  // $ANTLR start "AT_BIT_SHU"
  public final void mAT_BIT_SHU() throws RecognitionException {
    try {
      int _type = AT_BIT_SHU;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:31:12: ( '>>>' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:31:14: '>>>'
      {
      match(">>>"); 

      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_BIT_SHU"

  // $ANTLR start "AT_BIT_XOR"
  public final void mAT_BIT_XOR() throws RecognitionException {
    try {
      int _type = AT_BIT_XOR;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:32:12: ( '^' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:32:14: '^'
      {
      match('^'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_BIT_XOR"

  // $ANTLR start "AT_BOOL_AND"
  public final void mAT_BOOL_AND() throws RecognitionException {
    try {
      int _type = AT_BOOL_AND;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:33:13: ( '&&' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:33:15: '&&'
      {
      match("&&"); 

      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_BOOL_AND"

  // $ANTLR start "AT_BOOL_NOT"
  public final void mAT_BOOL_NOT() throws RecognitionException {
    try {
      int _type = AT_BOOL_NOT;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:34:13: ( '!' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:34:15: '!'
      {
      match('!'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_BOOL_NOT"

  // $ANTLR start "AT_BOOL_OR"
  public final void mAT_BOOL_OR() throws RecognitionException {
    try {
      int _type = AT_BOOL_OR;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:35:12: ( '||' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:35:14: '||'
      {
      match("||"); 

      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_BOOL_OR"

  // $ANTLR start "AT_COLON"
  public final void mAT_COLON() throws RecognitionException {
    try {
      int _type = AT_COLON;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:36:10: ( ':' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:36:12: ':'
      {
      match(':'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_COLON"

  // $ANTLR start "AT_COMMA"
  public final void mAT_COMMA() throws RecognitionException {
    try {
      int _type = AT_COMMA;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:37:10: ( ',' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:37:12: ','
      {
      match(','); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_COMMA"

  // $ANTLR start "AT_COMP_EQ"
  public final void mAT_COMP_EQ() throws RecognitionException {
    try {
      int _type = AT_COMP_EQ;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:38:12: ( '==' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:38:14: '=='
      {
      match("=="); 

      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_COMP_EQ"

  // $ANTLR start "AT_COMP_GT"
  public final void mAT_COMP_GT() throws RecognitionException {
    try {
      int _type = AT_COMP_GT;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:39:12: ( '>' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:39:14: '>'
      {
      match('>'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_COMP_GT"

  // $ANTLR start "AT_COMP_GTE"
  public final void mAT_COMP_GTE() throws RecognitionException {
    try {
      int _type = AT_COMP_GTE;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:40:13: ( '>=' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:40:15: '>='
      {
      match(">="); 

      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_COMP_GTE"

  // $ANTLR start "AT_COMP_LT"
  public final void mAT_COMP_LT() throws RecognitionException {
    try {
      int _type = AT_COMP_LT;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:41:12: ( '<' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:41:14: '<'
      {
      match('<'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_COMP_LT"

  // $ANTLR start "AT_COMP_LTE"
  public final void mAT_COMP_LTE() throws RecognitionException {
    try {
      int _type = AT_COMP_LTE;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:42:13: ( '<=' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:42:15: '<='
      {
      match("<="); 

      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_COMP_LTE"

  // $ANTLR start "AT_COMP_NEQ"
  public final void mAT_COMP_NEQ() throws RecognitionException {
    try {
      int _type = AT_COMP_NEQ;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:43:13: ( '!=' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:43:15: '!='
      {
      match("!="); 

      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_COMP_NEQ"

  // $ANTLR start "AT_COND_QUE"
  public final void mAT_COND_QUE() throws RecognitionException {
    try {
      int _type = AT_COND_QUE;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:44:13: ( '?' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:44:15: '?'
      {
      match('?'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_COND_QUE"

  // $ANTLR start "AT_DIVIDE"
  public final void mAT_DIVIDE() throws RecognitionException {
    try {
      int _type = AT_DIVIDE;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:45:11: ( '/' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:45:13: '/'
      {
      match('/'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_DIVIDE"

  // $ANTLR start "AT_DOT"
  public final void mAT_DOT() throws RecognitionException {
    try {
      int _type = AT_DOT;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:46:8: ( '.' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:46:10: '.'
      {
      match('.'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_DOT"

  // $ANTLR start "AT_LPAREN"
  public final void mAT_LPAREN() throws RecognitionException {
    try {
      int _type = AT_LPAREN;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:47:11: ( '(' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:47:13: '('
      {
      match('('); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_LPAREN"

  // $ANTLR start "AT_MODULO"
  public final void mAT_MODULO() throws RecognitionException {
    try {
      int _type = AT_MODULO;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:48:11: ( '%' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:48:13: '%'
      {
      match('%'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_MODULO"

  // $ANTLR start "AT_MULTIPLY"
  public final void mAT_MULTIPLY() throws RecognitionException {
    try {
      int _type = AT_MULTIPLY;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:49:13: ( '*' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:49:15: '*'
      {
      match('*'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_MULTIPLY"

  // $ANTLR start "AT_RPAREN"
  public final void mAT_RPAREN() throws RecognitionException {
    try {
      int _type = AT_RPAREN;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:50:11: ( ')' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:50:13: ')'
      {
      match(')'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_RPAREN"

  // $ANTLR start "AT_SUBTRACT"
  public final void mAT_SUBTRACT() throws RecognitionException {
    try {
      int _type = AT_SUBTRACT;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:51:13: ( '-' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:51:15: '-'
      {
      match('-'); 
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "AT_SUBTRACT"

  // $ANTLR start "ID"
  public final void mID() throws RecognitionException {
    try {
      int _type = ID;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:334:5: ( ( 'a' .. 'z' | 'A' .. 'Z' | '_' ) ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' )* )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:334:7: ( 'a' .. 'z' | 'A' .. 'Z' | '_' ) ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' )*
      {
      if ( (input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
        input.consume();
      }
      else {
        MismatchedSetException mse = new MismatchedSetException(null,input);
        recover(mse);
        throw mse;
      }
      // src/java/org/apache/lucene/expressions/js/Javascript.g:334:31: ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' )*
      loop1:
      while (true) {
        int alt1=2;
        int LA1_0 = input.LA(1);
        if ( ((LA1_0 >= '0' && LA1_0 <= '9')||(LA1_0 >= 'A' && LA1_0 <= 'Z')||LA1_0=='_'||(LA1_0 >= 'a' && LA1_0 <= 'z')) ) {
          alt1=1;
        }

        switch (alt1) {
        case 1 :
          // src/java/org/apache/lucene/expressions/js/Javascript.g:
          {
          if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
            input.consume();
          }
          else {
            MismatchedSetException mse = new MismatchedSetException(null,input);
            recover(mse);
            throw mse;
          }
          }
          break;

        default :
          break loop1;
        }
      }

      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "ID"

  // $ANTLR start "WS"
  public final void mWS() throws RecognitionException {
    try {
      int _type = WS;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:338:5: ( ( ' ' | '\\t' | '\\n' | '\\r' )+ )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:338:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
      {
      // src/java/org/apache/lucene/expressions/js/Javascript.g:338:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
      int cnt2=0;
      loop2:
      while (true) {
        int alt2=2;
        int LA2_0 = input.LA(1);
        if ( ((LA2_0 >= '\t' && LA2_0 <= '\n')||LA2_0=='\r'||LA2_0==' ') ) {
          alt2=1;
        }

        switch (alt2) {
        case 1 :
          // src/java/org/apache/lucene/expressions/js/Javascript.g:
          {
          if ( (input.LA(1) >= '\t' && input.LA(1) <= '\n')||input.LA(1)=='\r'||input.LA(1)==' ' ) {
            input.consume();
          }
          else {
            MismatchedSetException mse = new MismatchedSetException(null,input);
            recover(mse);
            throw mse;
          }
          }
          break;

        default :
          if ( cnt2 >= 1 ) break loop2;
          EarlyExitException eee = new EarlyExitException(2, input);
          throw eee;
        }
        cnt2++;
      }

      skip();
      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "WS"

  // $ANTLR start "DECIMAL"
  public final void mDECIMAL() throws RecognitionException {
    try {
      int _type = DECIMAL;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:342:5: ( DECIMALINTEGER AT_DOT ( DECIMALDIGIT )* ( EXPONENT )? | AT_DOT ( DECIMALDIGIT )+ ( EXPONENT )? | DECIMALINTEGER ( EXPONENT )? )
      int alt8=3;
      alt8 = dfa8.predict(input);
      switch (alt8) {
        case 1 :
          // src/java/org/apache/lucene/expressions/js/Javascript.g:342:7: DECIMALINTEGER AT_DOT ( DECIMALDIGIT )* ( EXPONENT )?
          {
          mDECIMALINTEGER(); 

          mAT_DOT(); 

          // src/java/org/apache/lucene/expressions/js/Javascript.g:342:29: ( DECIMALDIGIT )*
          loop3:
          while (true) {
            int alt3=2;
            int LA3_0 = input.LA(1);
            if ( ((LA3_0 >= '0' && LA3_0 <= '9')) ) {
              alt3=1;
            }

            switch (alt3) {
            case 1 :
              // src/java/org/apache/lucene/expressions/js/Javascript.g:
              {
              if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                input.consume();
              }
              else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
              }
              }
              break;

            default :
              break loop3;
            }
          }

          // src/java/org/apache/lucene/expressions/js/Javascript.g:342:43: ( EXPONENT )?
          int alt4=2;
          int LA4_0 = input.LA(1);
          if ( (LA4_0=='E'||LA4_0=='e') ) {
            alt4=1;
          }
          switch (alt4) {
            case 1 :
              // src/java/org/apache/lucene/expressions/js/Javascript.g:342:43: EXPONENT
              {
              mEXPONENT(); 

              }
              break;

          }

          }
          break;
        case 2 :
          // src/java/org/apache/lucene/expressions/js/Javascript.g:343:7: AT_DOT ( DECIMALDIGIT )+ ( EXPONENT )?
          {
          mAT_DOT(); 

          // src/java/org/apache/lucene/expressions/js/Javascript.g:343:14: ( DECIMALDIGIT )+
          int cnt5=0;
          loop5:
          while (true) {
            int alt5=2;
            int LA5_0 = input.LA(1);
            if ( ((LA5_0 >= '0' && LA5_0 <= '9')) ) {
              alt5=1;
            }

            switch (alt5) {
            case 1 :
              // src/java/org/apache/lucene/expressions/js/Javascript.g:
              {
              if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                input.consume();
              }
              else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
              }
              }
              break;

            default :
              if ( cnt5 >= 1 ) break loop5;
              EarlyExitException eee = new EarlyExitException(5, input);
              throw eee;
            }
            cnt5++;
          }

          // src/java/org/apache/lucene/expressions/js/Javascript.g:343:28: ( EXPONENT )?
          int alt6=2;
          int LA6_0 = input.LA(1);
          if ( (LA6_0=='E'||LA6_0=='e') ) {
            alt6=1;
          }
          switch (alt6) {
            case 1 :
              // src/java/org/apache/lucene/expressions/js/Javascript.g:343:28: EXPONENT
              {
              mEXPONENT(); 

              }
              break;

          }

          }
          break;
        case 3 :
          // src/java/org/apache/lucene/expressions/js/Javascript.g:344:7: DECIMALINTEGER ( EXPONENT )?
          {
          mDECIMALINTEGER(); 

          // src/java/org/apache/lucene/expressions/js/Javascript.g:344:22: ( EXPONENT )?
          int alt7=2;
          int LA7_0 = input.LA(1);
          if ( (LA7_0=='E'||LA7_0=='e') ) {
            alt7=1;
          }
          switch (alt7) {
            case 1 :
              // src/java/org/apache/lucene/expressions/js/Javascript.g:344:22: EXPONENT
              {
              mEXPONENT(); 

              }
              break;

          }

          }
          break;

      }
      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "DECIMAL"

  // $ANTLR start "OCTAL"
  public final void mOCTAL() throws RecognitionException {
    try {
      int _type = OCTAL;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:348:5: ( '0' ( OCTALDIGIT )+ )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:348:7: '0' ( OCTALDIGIT )+
      {
      match('0'); 
      // src/java/org/apache/lucene/expressions/js/Javascript.g:348:11: ( OCTALDIGIT )+
      int cnt9=0;
      loop9:
      while (true) {
        int alt9=2;
        int LA9_0 = input.LA(1);
        if ( ((LA9_0 >= '0' && LA9_0 <= '7')) ) {
          alt9=1;
        }

        switch (alt9) {
        case 1 :
          // src/java/org/apache/lucene/expressions/js/Javascript.g:
          {
          if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
            input.consume();
          }
          else {
            MismatchedSetException mse = new MismatchedSetException(null,input);
            recover(mse);
            throw mse;
          }
          }
          break;

        default :
          if ( cnt9 >= 1 ) break loop9;
          EarlyExitException eee = new EarlyExitException(9, input);
          throw eee;
        }
        cnt9++;
      }

      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "OCTAL"

  // $ANTLR start "HEX"
  public final void mHEX() throws RecognitionException {
    try {
      int _type = HEX;
      int _channel = DEFAULT_TOKEN_CHANNEL;
      // src/java/org/apache/lucene/expressions/js/Javascript.g:352:5: ( ( '0x' | '0X' ) ( HEXDIGIT )+ )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:352:7: ( '0x' | '0X' ) ( HEXDIGIT )+
      {
      // src/java/org/apache/lucene/expressions/js/Javascript.g:352:7: ( '0x' | '0X' )
      int alt10=2;
      int LA10_0 = input.LA(1);
      if ( (LA10_0=='0') ) {
        int LA10_1 = input.LA(2);
        if ( (LA10_1=='x') ) {
          alt10=1;
        }
        else if ( (LA10_1=='X') ) {
          alt10=2;
        }

        else {
          int nvaeMark = input.mark();
          try {
            input.consume();
            NoViableAltException nvae =
              new NoViableAltException("", 10, 1, input);
            throw nvae;
          } finally {
            input.rewind(nvaeMark);
          }
        }

      }

      else {
        NoViableAltException nvae =
          new NoViableAltException("", 10, 0, input);
        throw nvae;
      }

      switch (alt10) {
        case 1 :
          // src/java/org/apache/lucene/expressions/js/Javascript.g:352:8: '0x'
          {
          match("0x"); 

          }
          break;
        case 2 :
          // src/java/org/apache/lucene/expressions/js/Javascript.g:352:13: '0X'
          {
          match("0X"); 

          }
          break;

      }

      // src/java/org/apache/lucene/expressions/js/Javascript.g:352:19: ( HEXDIGIT )+
      int cnt11=0;
      loop11:
      while (true) {
        int alt11=2;
        int LA11_0 = input.LA(1);
        if ( ((LA11_0 >= '0' && LA11_0 <= '9')||(LA11_0 >= 'A' && LA11_0 <= 'F')||(LA11_0 >= 'a' && LA11_0 <= 'f')) ) {
          alt11=1;
        }

        switch (alt11) {
        case 1 :
          // src/java/org/apache/lucene/expressions/js/Javascript.g:
          {
          if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
            input.consume();
          }
          else {
            MismatchedSetException mse = new MismatchedSetException(null,input);
            recover(mse);
            throw mse;
          }
          }
          break;

        default :
          if ( cnt11 >= 1 ) break loop11;
          EarlyExitException eee = new EarlyExitException(11, input);
          throw eee;
        }
        cnt11++;
      }

      }

      state.type = _type;
      state.channel = _channel;
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "HEX"

  // $ANTLR start "DECIMALINTEGER"
  public final void mDECIMALINTEGER() throws RecognitionException {
    try {
      // src/java/org/apache/lucene/expressions/js/Javascript.g:358:5: ( '0' | '1' .. '9' ( DECIMALDIGIT )* )
      int alt13=2;
      int LA13_0 = input.LA(1);
      if ( (LA13_0=='0') ) {
        alt13=1;
      }
      else if ( ((LA13_0 >= '1' && LA13_0 <= '9')) ) {
        alt13=2;
      }

      else {
        NoViableAltException nvae =
          new NoViableAltException("", 13, 0, input);
        throw nvae;
      }

      switch (alt13) {
        case 1 :
          // src/java/org/apache/lucene/expressions/js/Javascript.g:358:7: '0'
          {
          match('0'); 
          }
          break;
        case 2 :
          // src/java/org/apache/lucene/expressions/js/Javascript.g:359:7: '1' .. '9' ( DECIMALDIGIT )*
          {
          matchRange('1','9'); 
          // src/java/org/apache/lucene/expressions/js/Javascript.g:359:16: ( DECIMALDIGIT )*
          loop12:
          while (true) {
            int alt12=2;
            int LA12_0 = input.LA(1);
            if ( ((LA12_0 >= '0' && LA12_0 <= '9')) ) {
              alt12=1;
            }

            switch (alt12) {
            case 1 :
              // src/java/org/apache/lucene/expressions/js/Javascript.g:
              {
              if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                input.consume();
              }
              else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
              }
              }
              break;

            default :
              break loop12;
            }
          }

          }
          break;

      }
    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "DECIMALINTEGER"

  // $ANTLR start "EXPONENT"
  public final void mEXPONENT() throws RecognitionException {
    try {
      // src/java/org/apache/lucene/expressions/js/Javascript.g:364:5: ( ( 'e' | 'E' ) ( '+' | '-' )? ( DECIMALDIGIT )+ )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:364:7: ( 'e' | 'E' ) ( '+' | '-' )? ( DECIMALDIGIT )+
      {
      if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
        input.consume();
      }
      else {
        MismatchedSetException mse = new MismatchedSetException(null,input);
        recover(mse);
        throw mse;
      }
      // src/java/org/apache/lucene/expressions/js/Javascript.g:364:17: ( '+' | '-' )?
      int alt14=2;
      int LA14_0 = input.LA(1);
      if ( (LA14_0=='+'||LA14_0=='-') ) {
        alt14=1;
      }
      switch (alt14) {
        case 1 :
          // src/java/org/apache/lucene/expressions/js/Javascript.g:
          {
          if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
            input.consume();
          }
          else {
            MismatchedSetException mse = new MismatchedSetException(null,input);
            recover(mse);
            throw mse;
          }
          }
          break;

      }

      // src/java/org/apache/lucene/expressions/js/Javascript.g:364:28: ( DECIMALDIGIT )+
      int cnt15=0;
      loop15:
      while (true) {
        int alt15=2;
        int LA15_0 = input.LA(1);
        if ( ((LA15_0 >= '0' && LA15_0 <= '9')) ) {
          alt15=1;
        }

        switch (alt15) {
        case 1 :
          // src/java/org/apache/lucene/expressions/js/Javascript.g:
          {
          if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
            input.consume();
          }
          else {
            MismatchedSetException mse = new MismatchedSetException(null,input);
            recover(mse);
            throw mse;
          }
          }
          break;

        default :
          if ( cnt15 >= 1 ) break loop15;
          EarlyExitException eee = new EarlyExitException(15, input);
          throw eee;
        }
        cnt15++;
      }

      }

    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "EXPONENT"

  // $ANTLR start "DECIMALDIGIT"
  public final void mDECIMALDIGIT() throws RecognitionException {
    try {
      // src/java/org/apache/lucene/expressions/js/Javascript.g:369:5: ( '0' .. '9' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:
      {
      if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
        input.consume();
      }
      else {
        MismatchedSetException mse = new MismatchedSetException(null,input);
        recover(mse);
        throw mse;
      }
      }

    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "DECIMALDIGIT"

  // $ANTLR start "HEXDIGIT"
  public final void mHEXDIGIT() throws RecognitionException {
    try {
      // src/java/org/apache/lucene/expressions/js/Javascript.g:374:5: ( DECIMALDIGIT | 'a' .. 'f' | 'A' .. 'F' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:
      {
      if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
        input.consume();
      }
      else {
        MismatchedSetException mse = new MismatchedSetException(null,input);
        recover(mse);
        throw mse;
      }
      }

    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "HEXDIGIT"

  // $ANTLR start "OCTALDIGIT"
  public final void mOCTALDIGIT() throws RecognitionException {
    try {
      // src/java/org/apache/lucene/expressions/js/Javascript.g:381:5: ( '0' .. '7' )
      // src/java/org/apache/lucene/expressions/js/Javascript.g:
      {
      if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
        input.consume();
      }
      else {
        MismatchedSetException mse = new MismatchedSetException(null,input);
        recover(mse);
        throw mse;
      }
      }

    }
    finally {
      // do for sure before leaving
    }
  }
  // $ANTLR end "OCTALDIGIT"

  @Override
  public void mTokens() throws RecognitionException {
    // src/java/org/apache/lucene/expressions/js/Javascript.g:1:8: ( AT_ADD | AT_BIT_AND | AT_BIT_NOT | AT_BIT_OR | AT_BIT_SHL | AT_BIT_SHR | AT_BIT_SHU | AT_BIT_XOR | AT_BOOL_AND | AT_BOOL_NOT | AT_BOOL_OR | AT_COLON | AT_COMMA | AT_COMP_EQ | AT_COMP_GT | AT_COMP_GTE | AT_COMP_LT | AT_COMP_LTE | AT_COMP_NEQ | AT_COND_QUE | AT_DIVIDE | AT_DOT | AT_LPAREN | AT_MODULO | AT_MULTIPLY | AT_RPAREN | AT_SUBTRACT | ID | WS | DECIMAL | OCTAL | HEX )
    int alt16=32;
    switch ( input.LA(1) ) {
    case '+':
      {
      alt16=1;
      }
      break;
    case '&':
      {
      int LA16_2 = input.LA(2);
      if ( (LA16_2=='&') ) {
        alt16=9;
      }

      else {
        alt16=2;
      }

      }
      break;
    case '~':
      {
      alt16=3;
      }
      break;
    case '|':
      {
      int LA16_4 = input.LA(2);
      if ( (LA16_4=='|') ) {
        alt16=11;
      }

      else {
        alt16=4;
      }

      }
      break;
    case '<':
      {
      switch ( input.LA(2) ) {
      case '<':
        {
        alt16=5;
        }
        break;
      case '=':
        {
        alt16=18;
        }
        break;
      default:
        alt16=17;
      }
      }
      break;
    case '>':
      {
      switch ( input.LA(2) ) {
      case '>':
        {
        int LA16_31 = input.LA(3);
        if ( (LA16_31=='>') ) {
          alt16=7;
        }

        else {
          alt16=6;
        }

        }
        break;
      case '=':
        {
        alt16=16;
        }
        break;
      default:
        alt16=15;
      }
      }
      break;
    case '^':
      {
      alt16=8;
      }
      break;
    case '!':
      {
      int LA16_8 = input.LA(2);
      if ( (LA16_8=='=') ) {
        alt16=19;
      }

      else {
        alt16=10;
      }

      }
      break;
    case ':':
      {
      alt16=12;
      }
      break;
    case ',':
      {
      alt16=13;
      }
      break;
    case '=':
      {
      alt16=14;
      }
      break;
    case '?':
      {
      alt16=20;
      }
      break;
    case '/':
      {
      alt16=21;
      }
      break;
    case '.':
      {
      int LA16_14 = input.LA(2);
      if ( ((LA16_14 >= '0' && LA16_14 <= '9')) ) {
        alt16=30;
      }

      else {
        alt16=22;
      }

      }
      break;
    case '(':
      {
      alt16=23;
      }
      break;
    case '%':
      {
      alt16=24;
      }
      break;
    case '*':
      {
      alt16=25;
      }
      break;
    case ')':
      {
      alt16=26;
      }
      break;
    case '-':
      {
      alt16=27;
      }
      break;
    case 'A':
    case 'B':
    case 'C':
    case 'D':
    case 'E':
    case 'F':
    case 'G':
    case 'H':
    case 'I':
    case 'J':
    case 'K':
    case 'L':
    case 'M':
    case 'N':
    case 'O':
    case 'P':
    case 'Q':
    case 'R':
    case 'S':
    case 'T':
    case 'U':
    case 'V':
    case 'W':
    case 'X':
    case 'Y':
    case 'Z':
    case '_':
    case 'a':
    case 'b':
    case 'c':
    case 'd':
    case 'e':
    case 'f':
    case 'g':
    case 'h':
    case 'i':
    case 'j':
    case 'k':
    case 'l':
    case 'm':
    case 'n':
    case 'o':
    case 'p':
    case 'q':
    case 'r':
    case 's':
    case 't':
    case 'u':
    case 'v':
    case 'w':
    case 'x':
    case 'y':
    case 'z':
      {
      alt16=28;
      }
      break;
    case '\t':
    case '\n':
    case '\r':
    case ' ':
      {
      alt16=29;
      }
      break;
    case '0':
      {
      switch ( input.LA(2) ) {
      case 'X':
      case 'x':
        {
        alt16=32;
        }
        break;
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
        {
        alt16=31;
        }
        break;
      default:
        alt16=30;
      }
      }
      break;
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
      {
      alt16=30;
      }
      break;
    default:
      NoViableAltException nvae =
        new NoViableAltException("", 16, 0, input);
      throw nvae;
    }
    switch (alt16) {
      case 1 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:10: AT_ADD
        {
        mAT_ADD(); 

        }
        break;
      case 2 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:17: AT_BIT_AND
        {
        mAT_BIT_AND(); 

        }
        break;
      case 3 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:28: AT_BIT_NOT
        {
        mAT_BIT_NOT(); 

        }
        break;
      case 4 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:39: AT_BIT_OR
        {
        mAT_BIT_OR(); 

        }
        break;
      case 5 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:49: AT_BIT_SHL
        {
        mAT_BIT_SHL(); 

        }
        break;
      case 6 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:60: AT_BIT_SHR
        {
        mAT_BIT_SHR(); 

        }
        break;
      case 7 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:71: AT_BIT_SHU
        {
        mAT_BIT_SHU(); 

        }
        break;
      case 8 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:82: AT_BIT_XOR
        {
        mAT_BIT_XOR(); 

        }
        break;
      case 9 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:93: AT_BOOL_AND
        {
        mAT_BOOL_AND(); 

        }
        break;
      case 10 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:105: AT_BOOL_NOT
        {
        mAT_BOOL_NOT(); 

        }
        break;
      case 11 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:117: AT_BOOL_OR
        {
        mAT_BOOL_OR(); 

        }
        break;
      case 12 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:128: AT_COLON
        {
        mAT_COLON(); 

        }
        break;
      case 13 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:137: AT_COMMA
        {
        mAT_COMMA(); 

        }
        break;
      case 14 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:146: AT_COMP_EQ
        {
        mAT_COMP_EQ(); 

        }
        break;
      case 15 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:157: AT_COMP_GT
        {
        mAT_COMP_GT(); 

        }
        break;
      case 16 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:168: AT_COMP_GTE
        {
        mAT_COMP_GTE(); 

        }
        break;
      case 17 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:180: AT_COMP_LT
        {
        mAT_COMP_LT(); 

        }
        break;
      case 18 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:191: AT_COMP_LTE
        {
        mAT_COMP_LTE(); 

        }
        break;
      case 19 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:203: AT_COMP_NEQ
        {
        mAT_COMP_NEQ(); 

        }
        break;
      case 20 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:215: AT_COND_QUE
        {
        mAT_COND_QUE(); 

        }
        break;
      case 21 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:227: AT_DIVIDE
        {
        mAT_DIVIDE(); 

        }
        break;
      case 22 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:237: AT_DOT
        {
        mAT_DOT(); 

        }
        break;
      case 23 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:244: AT_LPAREN
        {
        mAT_LPAREN(); 

        }
        break;
      case 24 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:254: AT_MODULO
        {
        mAT_MODULO(); 

        }
        break;
      case 25 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:264: AT_MULTIPLY
        {
        mAT_MULTIPLY(); 

        }
        break;
      case 26 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:276: AT_RPAREN
        {
        mAT_RPAREN(); 

        }
        break;
      case 27 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:286: AT_SUBTRACT
        {
        mAT_SUBTRACT(); 

        }
        break;
      case 28 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:298: ID
        {
        mID(); 

        }
        break;
      case 29 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:301: WS
        {
        mWS(); 

        }
        break;
      case 30 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:304: DECIMAL
        {
        mDECIMAL(); 

        }
        break;
      case 31 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:312: OCTAL
        {
        mOCTAL(); 

        }
        break;
      case 32 :
        // src/java/org/apache/lucene/expressions/js/Javascript.g:1:318: HEX
        {
        mHEX(); 

        }
        break;

    }
  }


  protected DFA8 dfa8 = new DFA8(this);
  static final String DFA8_eotS =
    "\1\uffff\2\4\3\uffff\1\4";
  static final String DFA8_eofS =
    "\7\uffff";
  static final String DFA8_minS =
    "\3\56\3\uffff\1\56";
  static final String DFA8_maxS =
    "\1\71\1\56\1\71\3\uffff\1\71";
  static final String DFA8_acceptS =
    "\3\uffff\1\2\1\3\1\1\1\uffff";
  static final String DFA8_specialS =
    "\7\uffff}>";
  static final String[] DFA8_transitionS = {
      "\1\3\1\uffff\1\1\11\2",
      "\1\5",
      "\1\5\1\uffff\12\6",
      "",
      "",
      "",
      "\1\5\1\uffff\12\6"
  };

  static final short[] DFA8_eot = DFA.unpackEncodedString(DFA8_eotS);
  static final short[] DFA8_eof = DFA.unpackEncodedString(DFA8_eofS);
  static final char[] DFA8_min = DFA.unpackEncodedStringToUnsignedChars(DFA8_minS);
  static final char[] DFA8_max = DFA.unpackEncodedStringToUnsignedChars(DFA8_maxS);
  static final short[] DFA8_accept = DFA.unpackEncodedString(DFA8_acceptS);
  static final short[] DFA8_special = DFA.unpackEncodedString(DFA8_specialS);
  static final short[][] DFA8_transition;

  static {
    int numStates = DFA8_transitionS.length;
    DFA8_transition = new short[numStates][];
    for (int i=0; i<numStates; i++) {
      DFA8_transition[i] = DFA.unpackEncodedString(DFA8_transitionS[i]);
    }
  }

  protected class DFA8 extends DFA {

    public DFA8(BaseRecognizer recognizer) {
      this.recognizer = recognizer;
      this.decisionNumber = 8;
      this.eot = DFA8_eot;
      this.eof = DFA8_eof;
      this.min = DFA8_min;
      this.max = DFA8_max;
      this.accept = DFA8_accept;
      this.special = DFA8_special;
      this.transition = DFA8_transition;
    }
    @Override
    public String getDescription() {
      return "341:1: DECIMAL : ( DECIMALINTEGER AT_DOT ( DECIMALDIGIT )* ( EXPONENT )? | AT_DOT ( DECIMALDIGIT )+ ( EXPONENT )? | DECIMALINTEGER ( EXPONENT )? );";
    }
  }

}

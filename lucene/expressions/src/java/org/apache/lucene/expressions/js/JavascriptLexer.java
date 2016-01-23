// ANTLR GENERATED CODE: DO NOT EDIT
package org.apache.lucene.expressions.js;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
class JavascriptLexer extends Lexer {
  static { RuntimeMetaData.checkVersion("4.5.1", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    LP=1, RP=2, COMMA=3, BOOLNOT=4, BWNOT=5, MUL=6, DIV=7, REM=8, ADD=9, SUB=10, 
    LSH=11, RSH=12, USH=13, LT=14, LTE=15, GT=16, GTE=17, EQ=18, NE=19, BWAND=20, 
    BWXOR=21, BWOR=22, BOOLAND=23, BOOLOR=24, COND=25, COLON=26, WS=27, VARIABLE=28, 
    OCTAL=29, HEX=30, DECIMAL=31;
  public static String[] modeNames = {
    "DEFAULT_MODE"
  };

  public static final String[] ruleNames = {
    "LP", "RP", "COMMA", "BOOLNOT", "BWNOT", "MUL", "DIV", "REM", "ADD", "SUB", 
    "LSH", "RSH", "USH", "LT", "LTE", "GT", "GTE", "EQ", "NE", "BWAND", "BWXOR", 
    "BWOR", "BOOLAND", "BOOLOR", "COND", "COLON", "WS", "VARIABLE", "ARRAY", 
    "ID", "STRING", "OCTAL", "HEX", "DECIMAL", "INTEGER"
  };

  private static final String[] _LITERAL_NAMES = {
    null, null, null, null, null, null, null, null, null, null, null, "'<<'", 
    "'>>'", "'>>>'", null, "'<='", null, "'>='", "'=='", "'!='", null, null, 
    null, "'&&'", "'||'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
    null, "LP", "RP", "COMMA", "BOOLNOT", "BWNOT", "MUL", "DIV", "REM", "ADD", 
    "SUB", "LSH", "RSH", "USH", "LT", "LTE", "GT", "GTE", "EQ", "NE", "BWAND", 
    "BWXOR", "BWOR", "BOOLAND", "BOOLOR", "COND", "COLON", "WS", "VARIABLE", 
    "OCTAL", "HEX", "DECIMAL"
  };
  public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

  /**
   * @deprecated Use {@link #VOCABULARY} instead.
   */
  @Deprecated
  public static final String[] tokenNames;
  static {
    tokenNames = new String[_SYMBOLIC_NAMES.length];
    for (int i = 0; i < tokenNames.length; i++) {
      tokenNames[i] = VOCABULARY.getLiteralName(i);
      if (tokenNames[i] == null) {
        tokenNames[i] = VOCABULARY.getSymbolicName(i);
      }

      if (tokenNames[i] == null) {
        tokenNames[i] = "<INVALID>";
      }
    }
  }

  @Override
  @Deprecated
  public String[] getTokenNames() {
    return tokenNames;
  }

  @Override

  public Vocabulary getVocabulary() {
    return VOCABULARY;
  }


  public JavascriptLexer(CharStream input) {
    super(input);
    _interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }

  @Override
  public String getGrammarFileName() { return "Javascript.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public String[] getModeNames() { return modeNames; }

  @Override
  public ATN getATN() { return _ATN; }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2!\u00fe\b\1\4\2\t"+
    "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
    "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
    "\t!\4\"\t\"\4#\t#\4$\t$\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3"+
    "\7\3\b\3\b\3\t\3\t\3\n\3\n\3\13\3\13\3\f\3\f\3\f\3\r\3\r\3\r\3\16\3\16"+
    "\3\16\3\16\3\17\3\17\3\20\3\20\3\20\3\21\3\21\3\22\3\22\3\22\3\23\3\23"+
    "\3\23\3\24\3\24\3\24\3\25\3\25\3\26\3\26\3\27\3\27\3\30\3\30\3\30\3\31"+
    "\3\31\3\31\3\32\3\32\3\33\3\33\3\34\6\34\u0089\n\34\r\34\16\34\u008a\3"+
    "\34\3\34\3\35\3\35\7\35\u0091\n\35\f\35\16\35\u0094\13\35\3\35\3\35\3"+
    "\35\7\35\u0099\n\35\f\35\16\35\u009c\13\35\7\35\u009e\n\35\f\35\16\35"+
    "\u00a1\13\35\3\36\3\36\3\36\5\36\u00a6\n\36\3\36\3\36\3\37\3\37\7\37\u00ac"+
    "\n\37\f\37\16\37\u00af\13\37\3 \3 \3 \3 \3 \3 \7 \u00b7\n \f \16 \u00ba"+
    "\13 \3 \3 \3 \3 \3 \3 \3 \7 \u00c3\n \f \16 \u00c6\13 \3 \5 \u00c9\n "+
    "\3!\3!\6!\u00cd\n!\r!\16!\u00ce\3\"\3\"\3\"\6\"\u00d4\n\"\r\"\16\"\u00d5"+
    "\3#\3#\3#\7#\u00db\n#\f#\16#\u00de\13#\5#\u00e0\n#\3#\3#\6#\u00e4\n#\r"+
    "#\16#\u00e5\5#\u00e8\n#\3#\3#\5#\u00ec\n#\3#\6#\u00ef\n#\r#\16#\u00f0"+
    "\5#\u00f3\n#\3$\3$\3$\7$\u00f8\n$\f$\16$\u00fb\13$\5$\u00fd\n$\4\u00b8"+
    "\u00c4\2%\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33"+
    "\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67"+
    "\359\36;\2=\2?\2A\37C E!G\2\3\2%\3\2**\3\2++\3\2..\3\2##\3\2\u0080\u0080"+
    "\3\2,,\3\2\61\61\3\2\'\'\3\2--\3\2//\3\2>>\3\2@@\3\2((\3\2``\3\2~~\3\2"+
    "AA\3\2<<\5\2\13\f\17\17\"\"\3\2\60\60\3\2]]\3\2^_\6\2&&C\\aac|\7\2&&\62"+
    ";C\\aac|\3\2))\4\2))^^\3\2$$\4\2$$^^\3\2\62\62\3\2\629\4\2ZZzz\5\2\62"+
    ";CHch\3\2\62;\4\2GGgg\4\2--//\3\2\63;\u0111\2\3\3\2\2\2\2\5\3\2\2\2\2"+
    "\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2"+
    "\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2"+
    "\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2"+
    "\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2"+
    "\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2\2E\3\2\2"+
    "\2\3I\3\2\2\2\5K\3\2\2\2\7M\3\2\2\2\tO\3\2\2\2\13Q\3\2\2\2\rS\3\2\2\2"+
    "\17U\3\2\2\2\21W\3\2\2\2\23Y\3\2\2\2\25[\3\2\2\2\27]\3\2\2\2\31`\3\2\2"+
    "\2\33c\3\2\2\2\35g\3\2\2\2\37i\3\2\2\2!l\3\2\2\2#n\3\2\2\2%q\3\2\2\2\'"+
    "t\3\2\2\2)w\3\2\2\2+y\3\2\2\2-{\3\2\2\2/}\3\2\2\2\61\u0080\3\2\2\2\63"+
    "\u0083\3\2\2\2\65\u0085\3\2\2\2\67\u0088\3\2\2\29\u008e\3\2\2\2;\u00a2"+
    "\3\2\2\2=\u00a9\3\2\2\2?\u00c8\3\2\2\2A\u00ca\3\2\2\2C\u00d0\3\2\2\2E"+
    "\u00e7\3\2\2\2G\u00fc\3\2\2\2IJ\t\2\2\2J\4\3\2\2\2KL\t\3\2\2L\6\3\2\2"+
    "\2MN\t\4\2\2N\b\3\2\2\2OP\t\5\2\2P\n\3\2\2\2QR\t\6\2\2R\f\3\2\2\2ST\t"+
    "\7\2\2T\16\3\2\2\2UV\t\b\2\2V\20\3\2\2\2WX\t\t\2\2X\22\3\2\2\2YZ\t\n\2"+
    "\2Z\24\3\2\2\2[\\\t\13\2\2\\\26\3\2\2\2]^\7>\2\2^_\7>\2\2_\30\3\2\2\2"+
    "`a\7@\2\2ab\7@\2\2b\32\3\2\2\2cd\7@\2\2de\7@\2\2ef\7@\2\2f\34\3\2\2\2"+
    "gh\t\f\2\2h\36\3\2\2\2ij\7>\2\2jk\7?\2\2k \3\2\2\2lm\t\r\2\2m\"\3\2\2"+
    "\2no\7@\2\2op\7?\2\2p$\3\2\2\2qr\7?\2\2rs\7?\2\2s&\3\2\2\2tu\7#\2\2uv"+
    "\7?\2\2v(\3\2\2\2wx\t\16\2\2x*\3\2\2\2yz\t\17\2\2z,\3\2\2\2{|\t\20\2\2"+
    "|.\3\2\2\2}~\7(\2\2~\177\7(\2\2\177\60\3\2\2\2\u0080\u0081\7~\2\2\u0081"+
    "\u0082\7~\2\2\u0082\62\3\2\2\2\u0083\u0084\t\21\2\2\u0084\64\3\2\2\2\u0085"+
    "\u0086\t\22\2\2\u0086\66\3\2\2\2\u0087\u0089\t\23\2\2\u0088\u0087\3\2"+
    "\2\2\u0089\u008a\3\2\2\2\u008a\u0088\3\2\2\2\u008a\u008b\3\2\2\2\u008b"+
    "\u008c\3\2\2\2\u008c\u008d\b\34\2\2\u008d8\3\2\2\2\u008e\u0092\5=\37\2"+
    "\u008f\u0091\5;\36\2\u0090\u008f\3\2\2\2\u0091\u0094\3\2\2\2\u0092\u0090"+
    "\3\2\2\2\u0092\u0093\3\2\2\2\u0093\u009f\3\2\2\2\u0094\u0092\3\2\2\2\u0095"+
    "\u0096\t\24\2\2\u0096\u009a\5=\37\2\u0097\u0099\5;\36\2\u0098\u0097\3"+
    "\2\2\2\u0099\u009c\3\2\2\2\u009a\u0098\3\2\2\2\u009a\u009b\3\2\2\2\u009b"+
    "\u009e\3\2\2\2\u009c\u009a\3\2\2\2\u009d\u0095\3\2\2\2\u009e\u00a1\3\2"+
    "\2\2\u009f\u009d\3\2\2\2\u009f\u00a0\3\2\2\2\u00a0:\3\2\2\2\u00a1\u009f"+
    "\3\2\2\2\u00a2\u00a5\t\25\2\2\u00a3\u00a6\5? \2\u00a4\u00a6\5G$\2\u00a5"+
    "\u00a3\3\2\2\2\u00a5\u00a4\3\2\2\2\u00a6\u00a7\3\2\2\2\u00a7\u00a8\t\26"+
    "\2\2\u00a8<\3\2\2\2\u00a9\u00ad\t\27\2\2\u00aa\u00ac\t\30\2\2\u00ab\u00aa"+
    "\3\2\2\2\u00ac\u00af\3\2\2\2\u00ad\u00ab\3\2\2\2\u00ad\u00ae\3\2\2\2\u00ae"+
    ">\3\2\2\2\u00af\u00ad\3\2\2\2\u00b0\u00b8\t\31\2\2\u00b1\u00b2\7^\2\2"+
    "\u00b2\u00b7\7)\2\2\u00b3\u00b4\7^\2\2\u00b4\u00b7\7^\2\2\u00b5\u00b7"+
    "\n\32\2\2\u00b6\u00b1\3\2\2\2\u00b6\u00b3\3\2\2\2\u00b6\u00b5\3\2\2\2"+
    "\u00b7\u00ba\3\2\2\2\u00b8\u00b9\3\2\2\2\u00b8\u00b6\3\2\2\2\u00b9\u00bb"+
    "\3\2\2\2\u00ba\u00b8\3\2\2\2\u00bb\u00c9\t\31\2\2\u00bc\u00c4\t\33\2\2"+
    "\u00bd\u00be\7^\2\2\u00be\u00c3\7$\2\2\u00bf\u00c0\7^\2\2\u00c0\u00c3"+
    "\7^\2\2\u00c1\u00c3\n\34\2\2\u00c2\u00bd\3\2\2\2\u00c2\u00bf\3\2\2\2\u00c2"+
    "\u00c1\3\2\2\2\u00c3\u00c6\3\2\2\2\u00c4\u00c5\3\2\2\2\u00c4\u00c2\3\2"+
    "\2\2\u00c5\u00c7\3\2\2\2\u00c6\u00c4\3\2\2\2\u00c7\u00c9\t\33\2\2\u00c8"+
    "\u00b0\3\2\2\2\u00c8\u00bc\3\2\2\2\u00c9@\3\2\2\2\u00ca\u00cc\t\35\2\2"+
    "\u00cb\u00cd\t\36\2\2\u00cc\u00cb\3\2\2\2\u00cd\u00ce\3\2\2\2\u00ce\u00cc"+
    "\3\2\2\2\u00ce\u00cf\3\2\2\2\u00cfB\3\2\2\2\u00d0\u00d1\t\35\2\2\u00d1"+
    "\u00d3\t\37\2\2\u00d2\u00d4\t \2\2\u00d3\u00d2\3\2\2\2\u00d4\u00d5\3\2"+
    "\2\2\u00d5\u00d3\3\2\2\2\u00d5\u00d6\3\2\2\2\u00d6D\3\2\2\2\u00d7\u00df"+
    "\5G$\2\u00d8\u00dc\t\24\2\2\u00d9\u00db\t!\2\2\u00da\u00d9\3\2\2\2\u00db"+
    "\u00de\3\2\2\2\u00dc\u00da\3\2\2\2\u00dc\u00dd\3\2\2\2\u00dd\u00e0\3\2"+
    "\2\2\u00de\u00dc\3\2\2\2\u00df\u00d8\3\2\2\2\u00df\u00e0\3\2\2\2\u00e0"+
    "\u00e8\3\2\2\2\u00e1\u00e3\t\24\2\2\u00e2\u00e4\t!\2\2\u00e3\u00e2\3\2"+
    "\2\2\u00e4\u00e5\3\2\2\2\u00e5\u00e3\3\2\2\2\u00e5\u00e6\3\2\2\2\u00e6"+
    "\u00e8\3\2\2\2\u00e7\u00d7\3\2\2\2\u00e7\u00e1\3\2\2\2\u00e8\u00f2\3\2"+
    "\2\2\u00e9\u00eb\t\"\2\2\u00ea\u00ec\t#\2\2\u00eb\u00ea\3\2\2\2\u00eb"+
    "\u00ec\3\2\2\2\u00ec\u00ee\3\2\2\2\u00ed\u00ef\t!\2\2\u00ee\u00ed\3\2"+
    "\2\2\u00ef\u00f0\3\2\2\2\u00f0\u00ee\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1"+
    "\u00f3\3\2\2\2\u00f2\u00e9\3\2\2\2\u00f2\u00f3\3\2\2\2\u00f3F\3\2\2\2"+
    "\u00f4\u00fd\t\35\2\2\u00f5\u00f9\t$\2\2\u00f6\u00f8\t!\2\2\u00f7\u00f6"+
    "\3\2\2\2\u00f8\u00fb\3\2\2\2\u00f9\u00f7\3\2\2\2\u00f9\u00fa\3\2\2\2\u00fa"+
    "\u00fd\3\2\2\2\u00fb\u00f9\3\2\2\2\u00fc\u00f4\3\2\2\2\u00fc\u00f5\3\2"+
    "\2\2\u00fdH\3\2\2\2\31\2\u008a\u0092\u009a\u009f\u00a5\u00ad\u00b6\u00b8"+
    "\u00c2\u00c4\u00c8\u00ce\u00d5\u00dc\u00df\u00e5\u00e7\u00eb\u00f0\u00f2"+
    "\u00f9\u00fc\3\b\2\2";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}

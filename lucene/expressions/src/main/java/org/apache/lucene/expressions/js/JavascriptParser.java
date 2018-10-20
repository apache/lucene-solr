// ANTLR GENERATED CODE: DO NOT EDIT
package org.apache.lucene.expressions.js;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
class JavascriptParser extends Parser {
  static { RuntimeMetaData.checkVersion("4.5.1", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    LP=1, RP=2, COMMA=3, BOOLNOT=4, BWNOT=5, MUL=6, DIV=7, REM=8, ADD=9, SUB=10, 
    LSH=11, RSH=12, USH=13, LT=14, LTE=15, GT=16, GTE=17, EQ=18, NE=19, BWAND=20, 
    BWXOR=21, BWOR=22, BOOLAND=23, BOOLOR=24, COND=25, COLON=26, WS=27, VARIABLE=28, 
    OCTAL=29, HEX=30, DECIMAL=31;
  public static final int
    RULE_compile = 0, RULE_expression = 1;
  public static final String[] ruleNames = {
    "compile", "expression"
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

  @Override
  public String getGrammarFileName() { return "Javascript.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public ATN getATN() { return _ATN; }

  public JavascriptParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }
  public static class CompileContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode EOF() { return getToken(JavascriptParser.EOF, 0); }
    public CompileContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_compile; }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitCompile(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CompileContext compile() throws RecognitionException {
    CompileContext _localctx = new CompileContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_compile);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(4);
      expression(0);
      setState(5);
      match(EOF);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ExpressionContext extends ParserRuleContext {
    public ExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_expression; }
   
    public ExpressionContext() { }
    public void copyFrom(ExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  public static class ConditionalContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode COND() { return getToken(JavascriptParser.COND, 0); }
    public TerminalNode COLON() { return getToken(JavascriptParser.COLON, 0); }
    public ConditionalContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitConditional(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BoolorContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode BOOLOR() { return getToken(JavascriptParser.BOOLOR, 0); }
    public BoolorContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitBoolor(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BoolcompContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode LT() { return getToken(JavascriptParser.LT, 0); }
    public TerminalNode LTE() { return getToken(JavascriptParser.LTE, 0); }
    public TerminalNode GT() { return getToken(JavascriptParser.GT, 0); }
    public TerminalNode GTE() { return getToken(JavascriptParser.GTE, 0); }
    public BoolcompContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitBoolcomp(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class NumericContext extends ExpressionContext {
    public TerminalNode OCTAL() { return getToken(JavascriptParser.OCTAL, 0); }
    public TerminalNode HEX() { return getToken(JavascriptParser.HEX, 0); }
    public TerminalNode DECIMAL() { return getToken(JavascriptParser.DECIMAL, 0); }
    public NumericContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitNumeric(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class AddsubContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode ADD() { return getToken(JavascriptParser.ADD, 0); }
    public TerminalNode SUB() { return getToken(JavascriptParser.SUB, 0); }
    public AddsubContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitAddsub(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class UnaryContext extends ExpressionContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode BOOLNOT() { return getToken(JavascriptParser.BOOLNOT, 0); }
    public TerminalNode BWNOT() { return getToken(JavascriptParser.BWNOT, 0); }
    public TerminalNode ADD() { return getToken(JavascriptParser.ADD, 0); }
    public TerminalNode SUB() { return getToken(JavascriptParser.SUB, 0); }
    public UnaryContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitUnary(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class PrecedenceContext extends ExpressionContext {
    public TerminalNode LP() { return getToken(JavascriptParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(JavascriptParser.RP, 0); }
    public PrecedenceContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitPrecedence(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class MuldivContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode MUL() { return getToken(JavascriptParser.MUL, 0); }
    public TerminalNode DIV() { return getToken(JavascriptParser.DIV, 0); }
    public TerminalNode REM() { return getToken(JavascriptParser.REM, 0); }
    public MuldivContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitMuldiv(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class ExternalContext extends ExpressionContext {
    public TerminalNode VARIABLE() { return getToken(JavascriptParser.VARIABLE, 0); }
    public TerminalNode LP() { return getToken(JavascriptParser.LP, 0); }
    public TerminalNode RP() { return getToken(JavascriptParser.RP, 0); }
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(JavascriptParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(JavascriptParser.COMMA, i);
    }
    public ExternalContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitExternal(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BwshiftContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode LSH() { return getToken(JavascriptParser.LSH, 0); }
    public TerminalNode RSH() { return getToken(JavascriptParser.RSH, 0); }
    public TerminalNode USH() { return getToken(JavascriptParser.USH, 0); }
    public BwshiftContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitBwshift(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BworContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode BWOR() { return getToken(JavascriptParser.BWOR, 0); }
    public BworContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitBwor(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BoolandContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode BOOLAND() { return getToken(JavascriptParser.BOOLAND, 0); }
    public BoolandContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitBooland(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BwxorContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode BWXOR() { return getToken(JavascriptParser.BWXOR, 0); }
    public BwxorContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitBwxor(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BwandContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode BWAND() { return getToken(JavascriptParser.BWAND, 0); }
    public BwandContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitBwand(this);
      else return visitor.visitChildren(this);
    }
  }
  public static class BooleqneContext extends ExpressionContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode EQ() { return getToken(JavascriptParser.EQ, 0); }
    public TerminalNode NE() { return getToken(JavascriptParser.NE, 0); }
    public BooleqneContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof JavascriptVisitor ) return ((JavascriptVisitor<? extends T>)visitor).visitBooleqne(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionContext expression() throws RecognitionException {
    return expression(0);
  }

  private ExpressionContext expression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
    ExpressionContext _prevctx = _localctx;
    int _startState = 2;
    enterRecursionRule(_localctx, 2, RULE_expression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(30);
      switch (_input.LA(1)) {
      case BOOLNOT:
      case BWNOT:
      case ADD:
      case SUB:
        {
        _localctx = new UnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(8);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        setState(9);
        expression(12);
        }
        break;
      case LP:
        {
        _localctx = new PrecedenceContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(10);
        match(LP);
        setState(11);
        expression(0);
        setState(12);
        match(RP);
        }
        break;
      case OCTAL:
      case HEX:
      case DECIMAL:
        {
        _localctx = new NumericContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(14);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << OCTAL) | (1L << HEX) | (1L << DECIMAL))) != 0)) ) {
        _errHandler.recoverInline(this);
        } else {
          consume();
        }
        }
        break;
      case VARIABLE:
        {
        _localctx = new ExternalContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(15);
        match(VARIABLE);
        setState(28);
        switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
        case 1:
          {
          setState(16);
          match(LP);
          setState(25);
          _la = _input.LA(1);
          if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LP) | (1L << BOOLNOT) | (1L << BWNOT) | (1L << ADD) | (1L << SUB) | (1L << VARIABLE) | (1L << OCTAL) | (1L << HEX) | (1L << DECIMAL))) != 0)) {
            {
            setState(17);
            expression(0);
            setState(22);
            _errHandler.sync(this);
            _la = _input.LA(1);
            while (_la==COMMA) {
              {
              {
              setState(18);
              match(COMMA);
              setState(19);
              expression(0);
              }
              }
              setState(24);
              _errHandler.sync(this);
              _la = _input.LA(1);
            }
            }
          }

          setState(27);
          match(RP);
          }
          break;
        }
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(70);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,5,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(68);
          switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
          case 1:
            {
            _localctx = new MuldivContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(32);
            if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
            setState(33);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << REM))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(34);
            expression(12);
            }
            break;
          case 2:
            {
            _localctx = new AddsubContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(35);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(36);
            _la = _input.LA(1);
            if ( !(_la==ADD || _la==SUB) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(37);
            expression(11);
            }
            break;
          case 3:
            {
            _localctx = new BwshiftContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(38);
            if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
            setState(39);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LSH) | (1L << RSH) | (1L << USH))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(40);
            expression(10);
            }
            break;
          case 4:
            {
            _localctx = new BoolcompContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(41);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(42);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LTE) | (1L << GT) | (1L << GTE))) != 0)) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(43);
            expression(9);
            }
            break;
          case 5:
            {
            _localctx = new BooleqneContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(44);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(45);
            _la = _input.LA(1);
            if ( !(_la==EQ || _la==NE) ) {
            _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(46);
            expression(8);
            }
            break;
          case 6:
            {
            _localctx = new BwandContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(47);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(48);
            match(BWAND);
            setState(49);
            expression(7);
            }
            break;
          case 7:
            {
            _localctx = new BwxorContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(50);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(51);
            match(BWXOR);
            setState(52);
            expression(6);
            }
            break;
          case 8:
            {
            _localctx = new BworContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(53);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(54);
            match(BWOR);
            setState(55);
            expression(5);
            }
            break;
          case 9:
            {
            _localctx = new BoolandContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(56);
            if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(57);
            match(BOOLAND);
            setState(58);
            expression(4);
            }
            break;
          case 10:
            {
            _localctx = new BoolorContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(59);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(60);
            match(BOOLOR);
            setState(61);
            expression(3);
            }
            break;
          case 11:
            {
            _localctx = new ConditionalContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(62);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(63);
            match(COND);
            setState(64);
            expression(0);
            setState(65);
            match(COLON);
            setState(66);
            expression(1);
            }
            break;
          }
          } 
        }
        setState(72);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,5,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 1:
      return expression_sempred((ExpressionContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return precpred(_ctx, 11);
    case 1:
      return precpred(_ctx, 10);
    case 2:
      return precpred(_ctx, 9);
    case 3:
      return precpred(_ctx, 8);
    case 4:
      return precpred(_ctx, 7);
    case 5:
      return precpred(_ctx, 6);
    case 6:
      return precpred(_ctx, 5);
    case 7:
      return precpred(_ctx, 4);
    case 8:
      return precpred(_ctx, 3);
    case 9:
      return precpred(_ctx, 2);
    case 10:
      return precpred(_ctx, 1);
    }
    return true;
  }

  public static final String _serializedATN =
    "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3!L\4\2\t\2\4\3\t\3"+
    "\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\7\3\27"+
    "\n\3\f\3\16\3\32\13\3\5\3\34\n\3\3\3\5\3\37\n\3\5\3!\n\3\3\3\3\3\3\3\3"+
    "\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
    "\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\7\3G\n\3"+
    "\f\3\16\3J\13\3\3\3\2\3\4\4\2\4\2\t\4\2\6\7\13\f\3\2\37!\3\2\b\n\3\2\13"+
    "\f\3\2\r\17\3\2\20\23\3\2\24\25Z\2\6\3\2\2\2\4 \3\2\2\2\6\7\5\4\3\2\7"+
    "\b\7\2\2\3\b\3\3\2\2\2\t\n\b\3\1\2\n\13\t\2\2\2\13!\5\4\3\16\f\r\7\3\2"+
    "\2\r\16\5\4\3\2\16\17\7\4\2\2\17!\3\2\2\2\20!\t\3\2\2\21\36\7\36\2\2\22"+
    "\33\7\3\2\2\23\30\5\4\3\2\24\25\7\5\2\2\25\27\5\4\3\2\26\24\3\2\2\2\27"+
    "\32\3\2\2\2\30\26\3\2\2\2\30\31\3\2\2\2\31\34\3\2\2\2\32\30\3\2\2\2\33"+
    "\23\3\2\2\2\33\34\3\2\2\2\34\35\3\2\2\2\35\37\7\4\2\2\36\22\3\2\2\2\36"+
    "\37\3\2\2\2\37!\3\2\2\2 \t\3\2\2\2 \f\3\2\2\2 \20\3\2\2\2 \21\3\2\2\2"+
    "!H\3\2\2\2\"#\f\r\2\2#$\t\4\2\2$G\5\4\3\16%&\f\f\2\2&\'\t\5\2\2\'G\5\4"+
    "\3\r()\f\13\2\2)*\t\6\2\2*G\5\4\3\f+,\f\n\2\2,-\t\7\2\2-G\5\4\3\13./\f"+
    "\t\2\2/\60\t\b\2\2\60G\5\4\3\n\61\62\f\b\2\2\62\63\7\26\2\2\63G\5\4\3"+
    "\t\64\65\f\7\2\2\65\66\7\27\2\2\66G\5\4\3\b\678\f\6\2\289\7\30\2\29G\5"+
    "\4\3\7:;\f\5\2\2;<\7\31\2\2<G\5\4\3\6=>\f\4\2\2>?\7\32\2\2?G\5\4\3\5@"+
    "A\f\3\2\2AB\7\33\2\2BC\5\4\3\2CD\7\34\2\2DE\5\4\3\3EG\3\2\2\2F\"\3\2\2"+
    "\2F%\3\2\2\2F(\3\2\2\2F+\3\2\2\2F.\3\2\2\2F\61\3\2\2\2F\64\3\2\2\2F\67"+
    "\3\2\2\2F:\3\2\2\2F=\3\2\2\2F@\3\2\2\2GJ\3\2\2\2HF\3\2\2\2HI\3\2\2\2I"+
    "\5\3\2\2\2JH\3\2\2\2\b\30\33\36 FH";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}

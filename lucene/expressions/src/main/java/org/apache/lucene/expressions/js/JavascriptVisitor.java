// ANTLR GENERATED CODE: DO NOT EDIT
package org.apache.lucene.expressions.js;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link JavascriptParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
interface JavascriptVisitor<T> extends ParseTreeVisitor<T> {
  /**
   * Visit a parse tree produced by {@link JavascriptParser#compile}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCompile(JavascriptParser.CompileContext ctx);
  /**
   * Visit a parse tree produced by the {@code conditional}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitConditional(JavascriptParser.ConditionalContext ctx);
  /**
   * Visit a parse tree produced by the {@code boolor}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBoolor(JavascriptParser.BoolorContext ctx);
  /**
   * Visit a parse tree produced by the {@code boolcomp}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBoolcomp(JavascriptParser.BoolcompContext ctx);
  /**
   * Visit a parse tree produced by the {@code numeric}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNumeric(JavascriptParser.NumericContext ctx);
  /**
   * Visit a parse tree produced by the {@code addsub}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAddsub(JavascriptParser.AddsubContext ctx);
  /**
   * Visit a parse tree produced by the {@code unary}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUnary(JavascriptParser.UnaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code precedence}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPrecedence(JavascriptParser.PrecedenceContext ctx);
  /**
   * Visit a parse tree produced by the {@code muldiv}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMuldiv(JavascriptParser.MuldivContext ctx);
  /**
   * Visit a parse tree produced by the {@code external}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExternal(JavascriptParser.ExternalContext ctx);
  /**
   * Visit a parse tree produced by the {@code bwshift}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBwshift(JavascriptParser.BwshiftContext ctx);
  /**
   * Visit a parse tree produced by the {@code bwor}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBwor(JavascriptParser.BworContext ctx);
  /**
   * Visit a parse tree produced by the {@code booland}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooland(JavascriptParser.BoolandContext ctx);
  /**
   * Visit a parse tree produced by the {@code bwxor}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBwxor(JavascriptParser.BwxorContext ctx);
  /**
   * Visit a parse tree produced by the {@code bwand}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBwand(JavascriptParser.BwandContext ctx);
  /**
   * Visit a parse tree produced by the {@code booleqne}
   * labeled alternative in {@link JavascriptParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooleqne(JavascriptParser.BooleqneContext ctx);
}

package org.apache.lucene.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * A SinkTokenizer can be used to cache Tokens for use in an Analyzer
 *
 * @see TeeTokenFilter
 *
 **/
public class SinkTokenizer extends Tokenizer {
  protected List/*<Token>*/ lst = new ArrayList/*<Token>*/();
  protected Iterator/*<Token>*/ iter;

  public SinkTokenizer(List/*<Token>*/ input) {
    this.lst = input;
    if (this.lst == null) this.lst = new ArrayList/*<Token>*/();
  }

  /**
   * only valid if tokens have not been consumed,
   * i.e. if this tokenizer is not part of another tokenstream
   *
   * @return A List of {@link org.apache.lucene.analysis.Token}s
   */
  public List/*<Token>*/ getTokens() {
    return lst;
  }

  /**
   * Ignores the input result Token
   * @param result
   * @return The next {@link org.apache.lucene.analysis.Token} in the Sink.
   * @throws IOException
   */
  public Token next(Token result) throws IOException {
    if (iter == null) iter = lst.iterator();
    return iter.hasNext() ? (Token) iter.next() : null;
  }

  /**
   * Override this method to cache only certain tokens, or new tokens based
   * on the old tokens.
   *
   * @param t The {@link org.apache.lucene.analysis.Token} to add to the sink
   */
  public void add(Token t) {
    if (t == null) return;
    lst.add((Token) t.clone());
  }

  public void reset() throws IOException {
    iter = lst.iterator();
  }
}


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

  public SinkTokenizer() {
    this.lst = new ArrayList();
  }

  public SinkTokenizer(int initCap){
    this.lst = new ArrayList(initCap);
  }

  /**
   * Get the tokens in the internal List.
   * <p/>
   * WARNING: Adding tokens to this list requires the {@link #reset()} method to be called in order for them
   * to be made available.  Also, this Tokenizer does nothing to protect against {@link java.util.ConcurrentModificationException}s
   * in the case of adds happening while {@link #next(org.apache.lucene.analysis.Token)} is being called.
   *
   * @return A List of {@link org.apache.lucene.analysis.Token}s
   */
  public List/*<Token>*/ getTokens() {
    return lst;
  }

  /**
   * Returns the next token out of the list of cached tokens
   * @return The next {@link org.apache.lucene.analysis.Token} in the Sink.
   * @throws IOException
   */
  public Token next() throws IOException {
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

  public void close() throws IOException {
    //nothing to close
    input = null;
    lst = null;
  }

  /**
   * Reset the internal data structures to the start at the front of the list of tokens.  Should be called
   * if tokens were added to the list after an invocation of {@link #next(Token)}
   * @throws IOException
   */
  public void reset() throws IOException {
    iter = lst.iterator();
  }
}


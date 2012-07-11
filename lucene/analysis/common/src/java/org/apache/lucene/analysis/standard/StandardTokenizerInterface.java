package org.apache.lucene.analysis.standard;

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

import java.io.Reader;
import java.io.IOException;

/** 
 * Internal interface for supporting versioned grammars.
 * @lucene.internal 
 */
public interface StandardTokenizerInterface {

  /** This character denotes the end of file */
  public static final int YYEOF = -1;

  /**
   * Copies the matched text into the CharTermAttribute
   */
  public void getText(CharTermAttribute t);

  /**
   * Returns the current position.
   */
  public int yychar();

  /**
   * Resets the scanner to read from a new input stream.
   * Does not close the old reader.
   *
   * All internal variables are reset, the old input stream 
   * <b>cannot</b> be reused (internal buffer is discarded and lost).
   * Lexical state is set to <tt>ZZ_INITIAL</tt>.
   *
   * @param reader   the new input stream 
   */
  public void yyreset(Reader reader);

  /**
   * Returns the length of the matched text region.
   */
  public int yylength();

  /**
   * Resumes scanning until the next regular expression is matched,
   * the end of input is encountered or an I/O-Error occurs.
   *
   * @return      the next token, {@link #YYEOF} on end of stream
   * @exception   IOException  if any I/O-Error occurs
   */
  public int getNextToken() throws IOException;

}

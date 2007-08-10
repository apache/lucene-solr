/**
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

package org.apache.lucene.analysis.standard;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.Tokenizer;

/** A grammar-based tokenizer constructed with JFlex
 *
 * <p> This should be a good tokenizer for most European-language documents:
 *
 * <ul>
 *   <li>Splits words at punctuation characters, removing punctuation. However, a 
 *     dot that's not followed by whitespace is considered part of a token.
 *   <li>Splits words at hyphens, unless there's a number in the token, in which case
 *     the whole token is interpreted as a product number and is not split.
 *   <li>Recognizes email addresses and internet hostnames as one token.
 * </ul>
 *
 * <p>Many applications have specific tokenizer needs.  If this tokenizer does
 * not suit your application, please consider copying this source code
 * directory to your project and maintaining your own grammar-based tokenizer.
 */

public class StandardTokenizer extends Tokenizer {
    /** A private instance of the JFlex-constructed scanner */
    private final StandardTokenizerImpl scanner;
  void setInput(Reader reader) {
    this.input = reader;
  }

    /**
     * Creates a new instance of the {@link StandardTokenizer}. Attaches the
     * <code>input</code> to a newly created JFlex scanner.
     */
    public StandardTokenizer(Reader input) {
	this.input = input;
	this.scanner = new StandardTokenizerImpl(input);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.analysis.TokenStream#next()
     */
    public Token next(Token result) throws IOException {
	int tokenType = scanner.getNextToken();

	if (tokenType == StandardTokenizerImpl.YYEOF) {
	    return null;
	}

        scanner.getText(result);
        final int start = scanner.yychar();
        result.setStartOffset(start);
        result.setEndOffset(start+result.termLength());
        result.setType(StandardTokenizerImpl.TOKEN_TYPES[tokenType]);
        return result;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.analysis.TokenStream#reset()
     */
    public void reset() throws IOException {
	super.reset();
	scanner.yyreset(input);
    }

    public void reset(Reader reader) throws IOException {
        input = reader;
        reset();
    }
}

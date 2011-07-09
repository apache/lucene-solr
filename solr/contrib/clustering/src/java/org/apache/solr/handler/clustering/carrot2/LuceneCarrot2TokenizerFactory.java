package org.apache.solr.handler.clustering.carrot2;

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

import java.io.IOException;
import java.io.Reader;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.carrot2.core.LanguageCode;
import org.carrot2.text.analysis.ExtendedWhitespaceTokenizer;
import org.carrot2.text.analysis.ITokenizer;
import org.carrot2.text.linguistic.ITokenizerFactory;
import org.carrot2.text.util.MutableCharArray;
import org.carrot2.util.ExceptionUtils;
import org.carrot2.util.ReflectionUtils;
import org.slf4j.Logger;

/**
 * An implementation of Carrot2's {@link ITokenizerFactory} based on Lucene's
 * Smart Chinese tokenizer. If Smart Chinese tokenizer is not available in
 * classpath at runtime, the default Carrot2's tokenizer is used. Should the
 * Lucene APIs need to change, the changes can be made in this class.
 */
public class LuceneCarrot2TokenizerFactory implements ITokenizerFactory {
	final static Logger logger = org.slf4j.LoggerFactory
			.getLogger(LuceneCarrot2TokenizerFactory.class);

	@Override
	public ITokenizer getTokenizer(LanguageCode language) {
		switch (language) {
		case CHINESE_SIMPLIFIED:
			return ChineseTokenizerFactory.createTokenizer();

			/*
			 * We use our own analyzer for Arabic. Lucene's version has special
			 * support for Nonspacing-Mark characters (see
			 * http://www.fileformat.info/info/unicode/category/Mn/index.htm), but we
			 * have them included as letters in the parser.
			 */
		case ARABIC:
			// Intentional fall-through.

		default:
			return new ExtendedWhitespaceTokenizer();
		}
	}

	/**
	 * Creates tokenizers that adapt Lucene's Smart Chinese Tokenizer to Carrot2's
	 * {@link ITokenizer}. If Smart Chinese is not available in the classpath, the
	 * factory will fall back to the default white space tokenizer.
	 */
	private static final class ChineseTokenizerFactory {
		static {
			try {
				ReflectionUtils.classForName(
						"org.apache.lucene.analysis.cn.smart.WordTokenFilter", false);
				ReflectionUtils.classForName(
						"org.apache.lucene.analysis.cn.smart.SentenceTokenizer", false);
			} catch (Throwable e) {
				logger
						.warn("Could not instantiate Smart Chinese Analyzer, clustering quality "
								+ "of Chinese content may be degraded. For best quality clusters, "
								+ "make sure Lucene's Smart Chinese Analyzer JAR is in the classpath");
			}
		}

		static ITokenizer createTokenizer() {
			try {
				return new ChineseTokenizer();
			} catch (Throwable e) {
				return new ExtendedWhitespaceTokenizer();
			}
		}

		private final static class ChineseTokenizer implements ITokenizer {
			private final static Pattern numeric = Pattern
					.compile("[\\-+'$]?\\d+([:\\-/,.]?\\d+)*[%$]?");

			private Tokenizer sentenceTokenizer;
			private TokenStream wordTokenFilter;
			private CharTermAttribute term = null;

			private final MutableCharArray tempCharSequence;
			private final Class<?> tokenFilterClass;

			private ChineseTokenizer() throws Exception {
				this.tempCharSequence = new MutableCharArray(new char[0]);

				// As Smart Chinese is not available during compile time,
				// we need to resort to reflection.
				final Class<?> tokenizerClass = ReflectionUtils.classForName(
						"org.apache.lucene.analysis.cn.smart.SentenceTokenizer", false);
				this.sentenceTokenizer = (Tokenizer) tokenizerClass.getConstructor(
						Reader.class).newInstance((Reader) null);
				this.tokenFilterClass = ReflectionUtils.classForName(
						"org.apache.lucene.analysis.cn.smart.WordTokenFilter", false);
			}

			public short nextToken() throws IOException {
				final boolean hasNextToken = wordTokenFilter.incrementToken();
				if (hasNextToken) {
					short flags = 0;
					final char[] image = term.buffer();
					final int length = term.length();
					tempCharSequence.reset(image, 0, length);
					if (length == 1 && image[0] == ',') {
						// ChineseTokenizer seems to convert all punctuation to ','
						// characters
						flags = ITokenizer.TT_PUNCTUATION;
					} else if (numeric.matcher(tempCharSequence).matches()) {
						flags = ITokenizer.TT_NUMERIC;
					} else {
						flags = ITokenizer.TT_TERM;
					}
					return flags;
				}

				return ITokenizer.TT_EOF;
			}

			public void setTermBuffer(MutableCharArray array) {
				array.reset(term.buffer(), 0, term.length());
			}

			public void reset(Reader input) throws IOException {
				try {
					sentenceTokenizer.reset(input);
					wordTokenFilter = (TokenStream) tokenFilterClass.getConstructor(
							TokenStream.class).newInstance(sentenceTokenizer);
          term = wordTokenFilter.addAttribute(CharTermAttribute.class);
				} catch (Exception e) {
					throw ExceptionUtils.wrapAsRuntimeException(e);
				}
			}
		}
	}
}

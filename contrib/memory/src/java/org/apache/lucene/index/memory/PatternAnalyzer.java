package org.apache.lucene.index.memory;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

/**
 * Efficient Lucene analyzer/tokenizer that preferably operates on a String rather than a
 * {@link java.io.Reader}, that can flexibly separate on a regular expression {@link Pattern}
 * (with behaviour identical to {@link String#split(String)}),
 * and that combines the functionality of
 * {@link org.apache.lucene.analysis.LetterTokenizer},
 * {@link org.apache.lucene.analysis.LowerCaseTokenizer},
 * {@link org.apache.lucene.analysis.WhitespaceTokenizer},
 * {@link org.apache.lucene.analysis.StopFilter} into a single efficient
 * multi-purpose class.
 * <p>
 * If you are unsure how exactly a regular expression should look like, consider 
 * prototyping by simply trying various expressions on some test texts via
 * {@link String#split(String)}. Once you are satisfied, give that regex to 
 * PatternAnalyzer. Also see <a target="_blank" 
 * href="http://java.sun.com/docs/books/tutorial/extra/regex/">Java Regular Expression Tutorial</a>.
 * <p>
 * This class can be considerably faster than the "normal" Lucene tokenizers. 
 * It can also serve as a building block in a compound Lucene
 * {@link org.apache.lucene.analysis.TokenFilter} chain. For example as in this 
 * stemming example:
 * <pre>
 * PatternAnalyzer pat = ...
 * TokenStream tokenStream = new SnowballFilter(
 *     pat.tokenStream("content", "James is running round in the woods"), 
 *     "English"));
 * </pre>
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.10 $, $Date: 2005/04/29 08:51:02 $
 */
public class PatternAnalyzer extends Analyzer {
	
	/** <code>"\\W+"</code>; Divides text at non-letters (Character.isLetter(c)) */
	public static final Pattern NON_WORD_PATTERN = Pattern.compile("\\W+");
	
	/** <code>"\\s+"</code>; Divides text at whitespaces (Character.isWhitespace(c) */
	public static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
	
	/**
	 * A lower-casing word analyzer with English stop words (can be shared
	 * freely across threads without harm); global per class loader.
	 */
	public static final PatternAnalyzer DEFAULT_ANALYZER = new PatternAnalyzer(
		NON_WORD_PATTERN, true, StopFilter.makeStopSet(StopAnalyzer.ENGLISH_STOP_WORDS));
		
	private final Pattern pattern;
	private final boolean toLowerCase;
	private final Set stopWords;
	
	/**
	 * Constructs a new instance with the given parameters.
	 * 
	 * @param pattern
	 *            a regular expression delimiting tokens
	 * @param toLowerCase
	 *            if <code>true</code> returns tokens after applying
	 *            String.toLowerCase()
	 * @param stopWords
	 *            if non-null, ignores all tokens that are contained in the
	 *            given stop set (after previously having applied toLowerCase()
	 *            if applicable). For example, created via
	 *            {@link StopFilter#makeStopSet(String[])}and/or
	 *            {@link org.apache.lucene.analysis.WordlistLoader}.
	 */
	public PatternAnalyzer(Pattern pattern, boolean toLowerCase, Set stopWords) {
		if (pattern == null) 
			throw new IllegalArgumentException("pattern must not be null");
		
		if (eqPattern(NON_WORD_PATTERN, pattern)) pattern = NON_WORD_PATTERN;
		else if (eqPattern(WHITESPACE_PATTERN, pattern)) pattern = WHITESPACE_PATTERN;
		
		this.pattern = pattern;
		this.toLowerCase = toLowerCase;
		this.stopWords = stopWords;
	}
	
	/**
	 * Creates a token stream that tokenizes the given string into token terms
	 * (aka words).
	 * 
	 * @param fieldName
	 *            the name of the field to tokenize (currently ignored).
	 * @param text
	 *            the string to tokenize
	 */
	public TokenStream tokenStream(String fieldName, String text) {
		// Ideally the Analyzer superclass should have a method with the same signature, 
		// with a default impl that simply delegates to the StringReader flavour. 
		if (text == null) 
			throw new IllegalArgumentException("text must not be null");
		
		TokenStream stream;
		if (pattern == NON_WORD_PATTERN) { // fast path
			stream = new FastStringTokenizer(text, true, toLowerCase, stopWords);
		}
		else if (pattern == WHITESPACE_PATTERN) { // fast path
			stream = new FastStringTokenizer(text, false, toLowerCase, stopWords);
		}
		else {
			stream = new PatternTokenizer(text, pattern, toLowerCase);
			if (stopWords != null) stream = new StopFilter(stream, stopWords);
		}
		
		return stream;
	}
	
	/**
	 * Creates a token stream that tokenizes all the text in the given Reader;
	 * This implementation forwards to <code>tokenStream(String, String)</code> and is
	 * less efficient than <code>tokenStream(String, String)</code>.
	 */
	public TokenStream tokenStream(String fieldName, Reader reader) {
		try {
			String text = toString(reader);
			return tokenStream(fieldName, text);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**  Indicates whether some other object is "equal to" this one. */
	public boolean equals(Object other) {
		if (this == other) return true;
		if (other instanceof PatternAnalyzer) {
			PatternAnalyzer p2 = (PatternAnalyzer) other;
			return 
				toLowerCase == p2.toLowerCase &&
				eqPattern(pattern, p2.pattern) &&
				eq(stopWords, p2.stopWords);
		}
		return false;
	}
	
	/** Returns a hash code value for the object. */
	public int hashCode() {
		if (this == DEFAULT_ANALYZER) return -1218418418; // fast path
		int h = 1;
		h = 31*h + pattern.pattern().hashCode();
		h = 31*h + pattern.flags();
		h = 31*h + (toLowerCase ? 1231 : 1237);
		h = 31*h + (stopWords != null ? stopWords.hashCode() : 0);
		return h;
	}
	
	/** equality where o1 and/or o2 can be null */
	private static boolean eq(Object o1, Object o2) {
		return (o1 == o2) || (o1 != null ? o1.equals(o2) : false);
	}
	
	/** assumes p1 and p2 are not null */
	private static boolean eqPattern(Pattern p1, Pattern p2) {
		return p1.flags() == p2.flags() && p1.pattern().equals(p2.pattern());
	}
		
	/**
	 * Reads until end-of-stream and returns all read chars, finally closes the stream.
	 * 
	 * @param input the input stream
	 * @throws IOException if an I/O error occurs while reading the stream
	 */
	private static String toString(Reader input) throws IOException {
		try {
			int len = 256;
			char[] buffer = new char[len];
			char[] output = new char[len];
			
			len = 0;
			int n;
			while ((n = input.read(buffer)) >= 0) {
				if (len + n > output.length) { // grow capacity
					char[] tmp = new char[Math.max(output.length << 1, len + n)];
					System.arraycopy(output, 0, tmp, 0, len);
					System.arraycopy(buffer, 0, tmp, len, n);
					buffer = output; // use larger buffer for future larger bulk reads
					output = tmp;
				} else {
					System.arraycopy(buffer, 0, output, len, n);
				}
				len += n;
			}

			return new String(output, 0, output.length);
		} finally {
			if (input != null) input.close();
		}
	}
		
	
	///////////////////////////////////////////////////////////////////////////////
	// Nested classes:
	///////////////////////////////////////////////////////////////////////////////
	/**
	 * The work horse; performance isn't fantastic, but it's not nearly as bad
	 * as one might think - kudos to the Sun regex developers.
	 */
	private static final class PatternTokenizer extends TokenStream {
		
		private final String str;
		private final boolean toLowerCase;
		private Matcher matcher;
		private int pos = 0;
		private static final Locale locale = Locale.getDefault();
		
		public PatternTokenizer(String str, Pattern pattern, boolean toLowerCase) {
			this.str = str;
			this.matcher = pattern.matcher(str);
			this.toLowerCase = toLowerCase;
		}

		public Token next() {
			if (matcher == null) return null;
			
			while (true) { // loop takes care of leading and trailing boundary cases
				int start = pos;
				int end;
				boolean isMatch = matcher.find();
				if (isMatch) {
					end = matcher.start();
					pos = matcher.end();
				} else { 
					end = str.length();
					matcher = null; // we're finished
				}
				
				if (start != end) { // non-empty match (header/trailer)
					String text = str.substring(start, end);
					if (toLowerCase) text = text.toLowerCase(locale);
					return new Token(text, start, end);
				}
				if (!isMatch) return null;
			}
		}
		
	}	
	
	
	///////////////////////////////////////////////////////////////////////////////
	// Nested classes:
	///////////////////////////////////////////////////////////////////////////////
	/**
	 * Special-case class for best perfomance in common cases; this class is
	 * otherwise unnecessary.
	 */
	private static final class FastStringTokenizer extends TokenStream {
		
		private final String str;
		private int pos;
		private final boolean isLetter;
		private final boolean toLowerCase;
		private final Set stopWords;
		private static final Locale locale = Locale.getDefault();
		
		public FastStringTokenizer(String str, boolean isLetter, boolean toLowerCase, Set stopWords) {
			this.str = str;
			this.isLetter = isLetter;
			this.toLowerCase = toLowerCase;
			this.stopWords = stopWords;
		}

		public Token next() {
			// cache loop instance vars (performance)
			String s = str;
			int len = s.length();
			int i = pos;
			boolean letter = isLetter;
			
			int start = 0;
			String text;
			do {
				// find beginning of token
				text = null;
				while (i < len && !isTokenChar(s.charAt(i), letter)) {
					i++;
				}
				
				if (i < len) { // found beginning; now find end of token
					start = i;
					while (i < len && isTokenChar(s.charAt(i), letter)) {
						i++;
					}
					
					text = s.substring(start, i);
					if (toLowerCase) text = text.toLowerCase(locale);
//					if (toLowerCase) {						
////						use next line once JDK 1.5 String.toLowerCase() performance regression is fixed
//						text = s.substring(start, i).toLowerCase(); 
////						char[] chars = new char[i-start];
////						for (int j=start; j < i; j++) chars[j-start] = Character.toLowerCase(s.charAt(j));
////						text = new String(chars);
//					} else {
//						text = s.substring(start, i);
//					}
				}
			} while (text != null && isStopWord(text));
			
			pos = i;
			return text != null ? new Token(text, start, i) : null;
		}
		
		private boolean isTokenChar(char c, boolean isLetter) {
			return isLetter ? Character.isLetter(c) : !Character.isWhitespace(c);
		}
		
		private boolean isStopWord(String text) {
			return stopWords != null && stopWords.contains(text);
		}
		
	}
		
}

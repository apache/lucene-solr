package org.apache.lucene.analysis.de;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Hashtable;

/**
 * Loads a text file and adds every line as an entry to a Hashtable. Every line
 * should contain only one word. If the file is not found or on any error, an
 * empty table is returned.
 *
 * @author    Gerhard Schwarz
 * @version   $Id$
 */
public class WordlistLoader {

	/**
	 * @param path      Path to the wordlist
	 * @param wordfile  Name of the wordlist
	 */
	public static Hashtable getWordtable( String path, String wordfile ) {
		if ( path == null || wordfile == null ) {
			return new Hashtable();
		}
		return getWordtable( new File( path, wordfile ) );
	}
	/**
	 * @param wordfile  Complete path to the wordlist
	 */
	public static Hashtable getWordtable( String wordfile ) {
		if ( wordfile == null ) {
			return new Hashtable();
		}
		return getWordtable( new File( wordfile ) );
	}

	/**
	 * @param wordfile  File containing the wordlist
	 */
	public static Hashtable getWordtable( File wordfile ) {
		if ( wordfile == null ) {
			return new Hashtable();
		}
		Hashtable result = null;
		try {
			LineNumberReader lnr = new LineNumberReader( new FileReader( wordfile ) );
			String word = null;
			String[] stopwords = new String[100];
			int wordcount = 0;
			while ( ( word = lnr.readLine() ) != null ) {
				wordcount++;
				if ( wordcount == stopwords.length ) {
					String[] tmp = new String[stopwords.length + 50];
					System.arraycopy( stopwords, 0, tmp, 0, wordcount );
					stopwords = tmp;
				}
				stopwords[wordcount-1] = word;
			}
			result = makeWordTable( stopwords, wordcount );
		}
		// On error, use an empty table
		catch ( IOException e ) {
			result = new Hashtable();
		}
		return result;
	}

	/**
	 * Builds the wordlist table.
	 *
	 * @param words   Word that where read
	 * @param length  Amount of words that where read into <tt>words</tt>
	 */
	private static Hashtable makeWordTable( String[] words, int length ) {
		Hashtable table = new Hashtable( length );
		for ( int i = 0; i < length; i++ ) {
			table.put( words[i], words[i] );
		}
		return table;
	}
}


package org.apache.lucene.analysis.de;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Hashtable;

/**
 * Loads a textfile and adds every line as an entry to a Hashtable. Every line
 * should contain only one word. If a file is not found or on any error, an
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


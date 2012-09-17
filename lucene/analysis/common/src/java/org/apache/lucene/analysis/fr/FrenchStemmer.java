package org.apache.lucene.analysis.fr;

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

import java.util.Locale;

/**
 * A stemmer for French words. 
 * <p>
 * The algorithm is based on the work of
 * Dr Martin Porter on his snowball project<br>
 * refer to http://snowball.sourceforge.net/french/stemmer.html<br>
 * (French stemming algorithm) for details
 * </p>
 * @deprecated Use {@link org.tartarus.snowball.ext.FrenchStemmer} instead, 
 * which has the same functionality. This filter will be removed in Lucene 4.0
 */
@Deprecated
public class FrenchStemmer {
  private static final Locale locale = new Locale("fr", "FR");


  /**
   * Buffer for the terms while stemming them.
   */
  private StringBuilder sb = new StringBuilder();

  /**
   * A temporary buffer, used to reconstruct R2
   */
   private StringBuilder tb = new StringBuilder();

  /**
   * Region R0 is equal to the whole buffer
   */
  private String R0;

  /**
   * Region RV
   * "If the word begins with two vowels, RV is the region after the third letter,
   * otherwise the region after the first vowel not at the beginning of the word,
   * or the end of the word if these positions cannot be found."
   */
    private String RV;

  /**
   * Region R1
   * "R1 is the region after the first non-vowel following a vowel
   * or is the null region at the end of the word if there is no such non-vowel"
   */
    private String R1;

  /**
   * Region R2
   * "R2 is the region after the first non-vowel in R1 following a vowel
   * or is the null region at the end of the word if there is no such non-vowel"
   */
    private String R2;


  /**
   * Set to true if we need to perform step 2
   */
    private boolean suite;

  /**
   * Set to true if the buffer was modified
   */
    private boolean modified;


    /**
     * Stems the given term to a unique <tt>discriminator</tt>.
     *
     * @param term  java.langString The term that should be stemmed
     * @return java.lang.String  Discriminator for <tt>term</tt>
     */
    protected String stem( String term ) {
    if ( !isStemmable( term ) ) {
      return term;
    }

    // Use lowercase for medium stemming.
    term = term.toLowerCase(locale);

    // Reset the StringBuilder.
    sb.delete( 0, sb.length() );
    sb.insert( 0, term );

    // reset the booleans
    modified = false;
    suite = false;

    sb = treatVowels( sb );

    setStrings();

    step1();

    if (!modified || suite)
    {
      if (RV != null)
      {
        suite = step2a();
        if (!suite)
          step2b();
      }
    }

    if (modified || suite)
      step3();
    else
      step4();

    step5();

    step6();

    return sb.toString();
    }

  /**
   * Sets the search region Strings<br>
   * it needs to be done each time the buffer was modified
   */
  private void setStrings() {
    // set the strings
    R0 = sb.toString();
    RV = retrieveRV( sb );
    R1 = retrieveR( sb );
    if ( R1 != null )
    {
      tb.delete( 0, tb.length() );
      tb.insert( 0, R1 );
      R2 = retrieveR( tb );
    }
    else
      R2 = null;
  }

  /**
   * First step of the Porter Algorithm<br>
   * refer to http://snowball.sourceforge.net/french/stemmer.html for an explanation
   */
  private void step1( ) {
    String[] suffix = { "ances", "iqUes", "ismes", "ables", "istes", "ance", "iqUe", "isme", "able", "iste" };
    deleteFrom( R2, suffix );

    replaceFrom( R2, new String[] { "logies", "logie" }, "log" );
    replaceFrom( R2, new String[] { "usions", "utions", "usion", "ution" }, "u" );
    replaceFrom( R2, new String[] { "ences", "ence" }, "ent" );

    String[] search = { "atrices", "ateurs", "ations", "atrice", "ateur", "ation"};
    deleteButSuffixFromElseReplace( R2, search, "ic",  true, R0, "iqU" );

    deleteButSuffixFromElseReplace( R2, new String[] { "ements", "ement" }, "eus", false, R0, "eux" );
    deleteButSuffixFrom( R2, new String[] { "ements", "ement" }, "ativ", false );
    deleteButSuffixFrom( R2, new String[] { "ements", "ement" }, "iv", false );
    deleteButSuffixFrom( R2, new String[] { "ements", "ement" }, "abl", false );
    deleteButSuffixFrom( R2, new String[] { "ements", "ement" }, "iqU", false );

    deleteFromIfTestVowelBeforeIn( R1, new String[] { "issements", "issement" }, false, R0 );
    deleteFrom( RV, new String[] { "ements", "ement" } );

    deleteButSuffixFromElseReplace( R2, new String[] { "ités", "ité" }, "abil", false, R0, "abl" );
    deleteButSuffixFromElseReplace( R2, new String[] { "ités", "ité" }, "ic", false, R0, "iqU" );
    deleteButSuffixFrom( R2, new String[] { "ités", "ité" }, "iv", true );

    String[] autre = { "ifs", "ives", "if", "ive" };
    deleteButSuffixFromElseReplace( R2, autre, "icat", false, R0, "iqU" );
    deleteButSuffixFromElseReplace( R2, autre, "at", true, R2, "iqU" );

    replaceFrom( R0, new String[] { "eaux" }, "eau" );

    replaceFrom( R1, new String[] { "aux" }, "al" );

    deleteButSuffixFromElseReplace( R2, new String[] { "euses", "euse" }, "", true, R1, "eux" );

    deleteFrom( R2, new String[] { "eux" } );

    // if one of the next steps is performed, we will need to perform step2a
    boolean temp = false;
    temp = replaceFrom( RV, new String[] { "amment" }, "ant" );
    if (temp == true)
      suite = true;
    temp = replaceFrom( RV, new String[] { "emment" }, "ent" );
    if (temp == true)
      suite = true;
    temp = deleteFromIfTestVowelBeforeIn( RV, new String[] { "ments", "ment" }, true, RV );
    if (temp == true)
      suite = true;

  }

  /**
   * Second step (A) of the Porter Algorithm<br>
   * Will be performed if nothing changed from the first step
   * or changed were done in the amment, emment, ments or ment suffixes<br>
   * refer to http://snowball.sourceforge.net/french/stemmer.html for an explanation
   *
   * @return boolean - true if something changed in the StringBuilder
   */
  private boolean step2a() {
    String[] search = { "îmes", "îtes", "iraIent", "irait", "irais", "irai", "iras", "ira",
              "irent", "iriez", "irez", "irions", "irons", "iront",
              "issaIent", "issais", "issantes", "issante", "issants", "issant",
              "issait", "issais", "issions", "issons", "issiez", "issez", "issent",
              "isses", "isse", "ir", "is", "ît", "it", "ies", "ie", "i" };
    return deleteFromIfTestVowelBeforeIn( RV, search, false, RV );
  }

  /**
   * Second step (B) of the Porter Algorithm<br>
   * Will be performed if step 2 A was performed unsuccessfully<br>
   * refer to http://snowball.sourceforge.net/french/stemmer.html for an explanation
   */
  private void step2b() {
    String[] suffix = { "eraIent", "erais", "erait", "erai", "eras", "erions", "eriez",
              "erons", "eront","erez", "èrent", "era", "ées", "iez",
              "ée", "és", "er", "ez", "é" };
    deleteFrom( RV, suffix );

    String[] search = { "assions", "assiez", "assent", "asses", "asse", "aIent",
              "antes", "aIent", "Aient", "ante", "âmes", "âtes", "ants", "ant",
              "ait", "aît", "ais", "Ait", "Aît", "Ais", "ât", "as", "ai", "Ai", "a" };
    deleteButSuffixFrom( RV, search, "e", true );

    deleteFrom( R2, new String[] { "ions" } );
  }

  /**
   * Third step of the Porter Algorithm<br>
   * refer to http://snowball.sourceforge.net/french/stemmer.html for an explanation
   */
  private void step3() {
    if (sb.length()>0)
    {
      char ch = sb.charAt( sb.length()-1 );
      if (ch == 'Y')
      {
        sb.setCharAt( sb.length()-1, 'i' );
        setStrings();
      }
      else if (ch == 'ç')
      {
        sb.setCharAt( sb.length()-1, 'c' );
        setStrings();
      }
    }
  }

  /**
   * Fourth step of the Porter Algorithm<br>
   * refer to http://snowball.sourceforge.net/french/stemmer.html for an explanation
   */
  private void step4() {
    if (sb.length() > 1)
    {
      char ch = sb.charAt( sb.length()-1 );
      if (ch == 's')
      {
        char b = sb.charAt( sb.length()-2 );
        if (b != 'a' && b != 'i' && b != 'o' && b != 'u' && b != 'è' && b != 's')
        {
          sb.delete( sb.length() - 1, sb.length());
          setStrings();
        }
      }
    }
    boolean found = deleteFromIfPrecededIn( R2, new String[] { "ion" }, RV, "s" );
    if (!found)
    found = deleteFromIfPrecededIn( R2, new String[] { "ion" }, RV, "t" );

    replaceFrom( RV, new String[] { "Ière", "ière", "Ier", "ier" }, "i" );
    deleteFrom( RV, new String[] { "e" } );
    deleteFromIfPrecededIn( RV, new String[] { "ë" }, R0, "gu" );
  }

  /**
   * Fifth step of the Porter Algorithm<br>
   * refer to http://snowball.sourceforge.net/french/stemmer.html for an explanation
   */
  private void step5() {
    if (R0 != null)
    {
      if (R0.endsWith("enn") || R0.endsWith("onn") || R0.endsWith("ett") || R0.endsWith("ell") || R0.endsWith("eill"))
      {
        sb.delete( sb.length() - 1, sb.length() );
        setStrings();
      }
    }
  }

  /**
   * Sixth (and last!) step of the Porter Algorithm<br>
   * refer to http://snowball.sourceforge.net/french/stemmer.html for an explanation
   */
  private void step6() {
    if (R0!=null && R0.length()>0)
    {
      boolean seenVowel = false;
      boolean seenConson = false;
      int pos = -1;
      for (int i = R0.length()-1; i > -1; i--)
      {
        char ch = R0.charAt(i);
        if (isVowel(ch))
        {
          if (!seenVowel)
          {
            if (ch == 'é' || ch == 'è')
            {
              pos = i;
              break;
            }
          }
          seenVowel = true;
        }
        else
        {
          if (seenVowel)
            break;
          else
            seenConson = true;
        }
      }
      if (pos > -1 && seenConson && !seenVowel)
        sb.setCharAt(pos, 'e');
    }
  }

  /**
   * Delete a suffix searched in zone "source" if zone "from" contains prefix + search string
   *
   * @param source java.lang.String - the primary source zone for search
   * @param search java.lang.String[] - the strings to search for suppression
   * @param from java.lang.String - the secondary source zone for search
   * @param prefix java.lang.String - the prefix to add to the search string to test
   * @return boolean - true if modified
   */
  private boolean deleteFromIfPrecededIn( String source, String[] search, String from, String prefix ) {
    boolean found = false;
    if (source!=null )
    {
      for (int i = 0; i < search.length; i++) {
        if ( source.endsWith( search[i] ))
        {
          if (from!=null && from.endsWith( prefix + search[i] ))
          {
            sb.delete( sb.length() - search[i].length(), sb.length());
            found = true;
            setStrings();
            break;
          }
        }
      }
    }
    return found;
  }

  /**
   * Delete a suffix searched in zone "source" if the preceding letter is (or isn't) a vowel
   *
   * @param source java.lang.String - the primary source zone for search
   * @param search java.lang.String[] - the strings to search for suppression
   * @param vowel boolean - true if we need a vowel before the search string
   * @param from java.lang.String - the secondary source zone for search (where vowel could be)
   * @return boolean - true if modified
   */
  private boolean deleteFromIfTestVowelBeforeIn( String source, String[] search, boolean vowel, String from ) {
    boolean found = false;
    if (source!=null && from!=null)
    {
      for (int i = 0; i < search.length; i++) {
        if ( source.endsWith( search[i] ))
        {
          if ((search[i].length() + 1) <= from.length())
          {
            boolean test = isVowel(sb.charAt(sb.length()-(search[i].length()+1)));
            if (test == vowel)
            {
              sb.delete( sb.length() - search[i].length(), sb.length());
              modified = true;
              found = true;
              setStrings();
              break;
            }
          }
        }
      }
    }
    return found;
  }

  /**
   * Delete a suffix searched in zone "source" if preceded by the prefix
   *
   * @param source java.lang.String - the primary source zone for search
   * @param search java.lang.String[] - the strings to search for suppression
   * @param prefix java.lang.String - the prefix to add to the search string to test
   * @param without boolean - true if it will be deleted even without prefix found
   */
  private void deleteButSuffixFrom( String source, String[] search, String prefix, boolean without ) {
    if (source!=null)
    {
      for (int i = 0; i < search.length; i++) {
        if ( source.endsWith( prefix + search[i] ))
        {
          sb.delete( sb.length() - (prefix.length() + search[i].length()), sb.length() );
          modified = true;
          setStrings();
          break;
        }
        else if ( without && source.endsWith( search[i] ))
        {
          sb.delete( sb.length() - search[i].length(), sb.length() );
          modified = true;
          setStrings();
          break;
        }
      }
    }
  }

  /**
   * Delete a suffix searched in zone "source" if preceded by prefix<br>
   * or replace it with the replace string if preceded by the prefix in the zone "from"<br>
   * or delete the suffix if specified
   *
   * @param source java.lang.String - the primary source zone for search
   * @param search java.lang.String[] - the strings to search for suppression
   * @param prefix java.lang.String - the prefix to add to the search string to test
   * @param without boolean - true if it will be deleted even without prefix found
   */
  private void deleteButSuffixFromElseReplace( String source, String[] search, String prefix, boolean without, String from, String replace ) {
    if (source!=null)
    {
      for (int i = 0; i < search.length; i++) {
        if ( source.endsWith( prefix + search[i] ))
        {
          sb.delete( sb.length() - (prefix.length() + search[i].length()), sb.length() );
          modified = true;
          setStrings();
          break;
        }
        else if ( from!=null && from.endsWith( prefix + search[i] ))
        {
          sb.replace( sb.length() - (prefix.length() + search[i].length()), sb.length(), replace );
          modified = true;
          setStrings();
          break;
        }
        else if ( without && source.endsWith( search[i] ))
        {
          sb.delete( sb.length() - search[i].length(), sb.length() );
          modified = true;
          setStrings();
          break;
        }
      }
    }
  }

  /**
   * Replace a search string with another within the source zone
   *
   * @param source java.lang.String - the source zone for search
   * @param search java.lang.String[] - the strings to search for replacement
   * @param replace java.lang.String - the replacement string
   */
  private boolean replaceFrom( String source, String[] search, String replace ) {
    boolean found = false;
    if (source!=null)
    {
      for (int i = 0; i < search.length; i++) {
        if ( source.endsWith( search[i] ))
        {
          sb.replace( sb.length() - search[i].length(), sb.length(), replace );
          modified = true;
          found = true;
          setStrings();
          break;
        }
      }
    }
    return found;
  }

  /**
   * Delete a search string within the source zone
   *
   * @param source the source zone for search
   * @param suffix the strings to search for suppression
   */
  private void deleteFrom(String source, String[] suffix ) {
    if (source!=null)
    {
      for (int i = 0; i < suffix.length; i++) {
        if (source.endsWith( suffix[i] ))
        {
          sb.delete( sb.length() - suffix[i].length(), sb.length());
          modified = true;
          setStrings();
          break;
        }
      }
    }
  }

  /**
   * Test if a char is a french vowel, including accentuated ones
   *
   * @param ch the char to test
   * @return boolean - true if the char is a vowel
   */
  private boolean isVowel(char ch) {
    switch (ch)
    {
      case 'a':
      case 'e':
      case 'i':
      case 'o':
      case 'u':
      case 'y':
      case 'â':
      case 'à':
      case 'ë':
      case 'é':
      case 'ê':
      case 'è':
      case 'ï':
      case 'î':
      case 'ô':
      case 'ü':
      case 'ù':
      case 'û':
        return true;
      default:
        return false;
    }
  }

  /**
   * Retrieve the "R zone" (1 or 2 depending on the buffer) and return the corresponding string<br>
   * "R is the region after the first non-vowel following a vowel
   * or is the null region at the end of the word if there is no such non-vowel"<br>
   * @param buffer java.lang.StringBuilder - the in buffer
   * @return java.lang.String - the resulting string
   */
  private String retrieveR( StringBuilder buffer ) {
    int len = buffer.length();
    int pos = -1;
    for (int c = 0; c < len; c++) {
      if (isVowel( buffer.charAt( c )))
      {
        pos = c;
        break;
      }
    }
    if (pos > -1)
    {
      int consonne = -1;
      for (int c = pos; c < len; c++) {
        if (!isVowel(buffer.charAt( c )))
        {
          consonne = c;
          break;
        }
      }
      if (consonne > -1 && (consonne+1) < len)
        return buffer.substring( consonne+1, len );
      else
        return null;
    }
    else
      return null;
  }

  /**
   * Retrieve the "RV zone" from a buffer an return the corresponding string<br>
   * "If the word begins with two vowels, RV is the region after the third letter,
   * otherwise the region after the first vowel not at the beginning of the word,
   * or the end of the word if these positions cannot be found."<br>
   * @param buffer java.lang.StringBuilder - the in buffer
   * @return java.lang.String - the resulting string
   */
  private String retrieveRV( StringBuilder buffer ) {
    int len = buffer.length();
    if ( buffer.length() > 3)
    {
      if ( isVowel(buffer.charAt( 0 )) && isVowel(buffer.charAt( 1 ))) {
        return buffer.substring(3,len);
      }
      else
      {
        int pos = 0;
        for (int c = 1; c < len; c++) {
          if (isVowel( buffer.charAt( c )))
          {
            pos = c;
            break;
          }
        }
        if ( pos+1 < len )
          return buffer.substring( pos+1, len );
        else
          return null;
      }
    }
    else
      return null;
  }



    /**
   * Turns u and i preceded AND followed by a vowel to UpperCase<br>
   * Turns y preceded OR followed by a vowel to UpperCase<br>
   * Turns u preceded by q to UpperCase<br>
     *
     * @param buffer java.util.StringBuilder - the buffer to treat
     * @return java.util.StringBuilder - the treated buffer
     */
    private StringBuilder treatVowels( StringBuilder buffer ) {
    for ( int c = 0; c < buffer.length(); c++ ) {
      char ch = buffer.charAt( c );

      if (c == 0) // first char
      {
        if (buffer.length()>1)
        {
          if (ch == 'y' && isVowel(buffer.charAt( c + 1 )))
            buffer.setCharAt( c, 'Y' );
        }
      }
      else if (c == buffer.length()-1) // last char
      {
        if (ch == 'u' && buffer.charAt( c - 1 ) == 'q')
          buffer.setCharAt( c, 'U' );
        if (ch == 'y' && isVowel(buffer.charAt( c - 1 )))
          buffer.setCharAt( c, 'Y' );
      }
      else // other cases
      {
        if (ch == 'u')
        {
          if (buffer.charAt( c - 1) == 'q')
            buffer.setCharAt( c, 'U' );
          else if (isVowel(buffer.charAt( c - 1 )) && isVowel(buffer.charAt( c + 1 )))
            buffer.setCharAt( c, 'U' );
        }
        if (ch == 'i')
        {
          if (isVowel(buffer.charAt( c - 1 )) && isVowel(buffer.charAt( c + 1 )))
            buffer.setCharAt( c, 'I' );
        }
        if (ch == 'y')
        {
          if (isVowel(buffer.charAt( c - 1 )) || isVowel(buffer.charAt( c + 1 )))
            buffer.setCharAt( c, 'Y' );
        }
      }
    }

    return buffer;
   }

    /**
     * Checks a term if it can be processed correctly.
     *
     * @return boolean - true if, and only if, the given term consists in letters.
     */
    private boolean isStemmable( String term ) {
    boolean upper = false;
    int first = -1;
    for ( int c = 0; c < term.length(); c++ ) {
      // Discard terms that contain non-letter characters.
      if ( !Character.isLetter( term.charAt( c ) ) ) {
        return false;
      }
      // Discard terms that contain multiple uppercase letters.
      if ( Character.isUpperCase( term.charAt( c ) ) ) {
        if ( upper ) {
          return false;
        }
      // First encountered uppercase letter, set flag and save
      // position.
        else {
          first = c;
          upper = true;
        }
      }
    }
    // Discard the term if it contains a single uppercase letter that
    // is not starting the term.
    if ( first > 0 ) {
      return false;
    }
    return true;
  }
}

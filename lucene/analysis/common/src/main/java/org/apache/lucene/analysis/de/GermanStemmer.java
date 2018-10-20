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
package org.apache.lucene.analysis.de;

import java.util.Locale;

// This file is encoded in UTF-8


/**
 * A stemmer for German words. 
 * <p>
 * The algorithm is based on the report
 * "A Fast and Simple Stemming Algorithm for German Words" by J&ouml;rg
 * Caumanns (joerg.caumanns at isst.fhg.de).
 * </p>
 */
public class GermanStemmer
{
    /**
     * Buffer for the terms while stemming them.
     */
    private StringBuilder sb = new StringBuilder();

    /**
     * Amount of characters that are removed with <tt>substitute()</tt> while stemming.
     */
    private int substCount = 0;
    
    private static final Locale locale = new Locale("de", "DE");

    /**
     * Stemms the given term to an unique <tt>discriminator</tt>.
     *
     * @param term  The term that should be stemmed.
     * @return      Discriminator for <tt>term</tt>
     */
    protected String stem( String term )
    {
      // Use lowercase for medium stemming.
      term = term.toLowerCase(locale);
      if ( !isStemmable( term ) )
        return term;
      // Reset the StringBuilder.
      sb.delete( 0, sb.length() );
      sb.insert( 0, term );
      // Stemming starts here...
      substitute( sb );
      strip( sb );
      optimize( sb );
      resubstitute( sb );
      removeParticleDenotion( sb );
      return sb.toString();
    }

    /**
     * Checks if a term could be stemmed.
     *
     * @return  true if, and only if, the given term consists in letters.
     */
    private boolean isStemmable( String term )
    {
      for ( int c = 0; c < term.length(); c++ ) {
        if ( !Character.isLetter( term.charAt( c ) ) )
          return false;
      }
      return true;
    }

    /**
     * suffix stripping (stemming) on the current term. The stripping is reduced
     * to the seven "base" suffixes "e", "s", "n", "t", "em", "er" and * "nd",
     * from which all regular suffixes are build of. The simplification causes
     * some overstemming, and way more irregular stems, but still provides unique.
     * discriminators in the most of those cases.
     * The algorithm is context free, except of the length restrictions.
     */
    private void strip( StringBuilder buffer )
    {
      boolean doMore = true;
      while ( doMore && buffer.length() > 3 ) {
        if ( ( buffer.length() + substCount > 5 ) &&
          buffer.substring( buffer.length() - 2, buffer.length() ).equals( "nd" ) )
        {
          buffer.delete( buffer.length() - 2, buffer.length() );
        }
        else if ( ( buffer.length() + substCount > 4 ) &&
          buffer.substring( buffer.length() - 2, buffer.length() ).equals( "em" ) ) {
            buffer.delete( buffer.length() - 2, buffer.length() );
        }
        else if ( ( buffer.length() + substCount > 4 ) &&
          buffer.substring( buffer.length() - 2, buffer.length() ).equals( "er" ) ) {
            buffer.delete( buffer.length() - 2, buffer.length() );
        }
        else if ( buffer.charAt( buffer.length() - 1 ) == 'e' ) {
          buffer.deleteCharAt( buffer.length() - 1 );
        }
        else if ( buffer.charAt( buffer.length() - 1 ) == 's' ) {
          buffer.deleteCharAt( buffer.length() - 1 );
        }
        else if ( buffer.charAt( buffer.length() - 1 ) == 'n' ) {
          buffer.deleteCharAt( buffer.length() - 1 );
        }
        // "t" occurs only as suffix of verbs.
        else if ( buffer.charAt( buffer.length() - 1 ) == 't' ) {
          buffer.deleteCharAt( buffer.length() - 1 );
        }
        else {
          doMore = false;
        }
      }
    }

    /**
     * Does some optimizations on the term. This optimisations are
     * contextual.
     */
    private void optimize( StringBuilder buffer )
    {
      // Additional step for female plurals of professions and inhabitants.
      if ( buffer.length() > 5 && buffer.substring( buffer.length() - 5, buffer.length() ).equals( "erin*" ) ) {
        buffer.deleteCharAt( buffer.length() -1 );
        strip( buffer );
      }
      // Additional step for irregular plural nouns like "Matrizen -> Matrix".
      // NOTE: this length constraint is probably not a great value, it's just to prevent AIOOBE on empty terms
      if ( buffer.length() > 0 && buffer.charAt( buffer.length() - 1 ) == ( 'z' ) ) {
        buffer.setCharAt( buffer.length() - 1, 'x' );
      }
    }

    /**
     * Removes a particle denotion ("ge") from a term.
     */
    private void removeParticleDenotion( StringBuilder buffer )
    {
      if ( buffer.length() > 4 ) {
        for ( int c = 0; c < buffer.length() - 3; c++ ) {
          if ( buffer.substring( c, c + 4 ).equals( "gege" ) ) {
            buffer.delete( c, c + 2 );
            return;
          }
        }
      }
    }

    /**
     * Do some substitutions for the term to reduce overstemming:
     *
     * - Substitute Umlauts with their corresponding vowel:{@code äöü -> aou},
     *   "ß" is substituted by "ss"
     * - Substitute a second char of a pair of equal characters with
     *   an asterisk: {@code ?? -> ?*}
     * - Substitute some common character combinations with a token:
     *   {@code sch/ch/ei/ie/ig/st -> $/§/%/&/#/!}
     */
    private void substitute( StringBuilder buffer )
    {
      substCount = 0;
      for ( int c = 0; c < buffer.length(); c++ ) {
        // Replace the second char of a pair of the equal characters with an asterisk
        if ( c > 0 && buffer.charAt( c ) == buffer.charAt ( c - 1 )  ) {
          buffer.setCharAt( c, '*' );
        }
        // Substitute Umlauts.
        else if ( buffer.charAt( c ) == 'ä' ) {
          buffer.setCharAt( c, 'a' );
        }
        else if ( buffer.charAt( c ) == 'ö' ) {
          buffer.setCharAt( c, 'o' );
        }
        else if ( buffer.charAt( c ) == 'ü' ) {
          buffer.setCharAt( c, 'u' );
        }
        // Fix bug so that 'ß' at the end of a word is replaced.
        else if ( buffer.charAt( c ) == 'ß' ) {
            buffer.setCharAt( c, 's' );
            buffer.insert( c + 1, 's' );
            substCount++;
        }
        // Take care that at least one character is left left side from the current one
        if ( c < buffer.length() - 1 ) {
          // Masking several common character combinations with an token
          if ( ( c < buffer.length() - 2 ) && buffer.charAt( c ) == 's' &&
            buffer.charAt( c + 1 ) == 'c' && buffer.charAt( c + 2 ) == 'h' )
          {
            buffer.setCharAt( c, '$' );
            buffer.delete( c + 1, c + 3 );
            substCount += 2;
          }
          else if ( buffer.charAt( c ) == 'c' && buffer.charAt( c + 1 ) == 'h' ) {
            buffer.setCharAt( c, '§' );
            buffer.deleteCharAt( c + 1 );
            substCount++;
          }
          else if ( buffer.charAt( c ) == 'e' && buffer.charAt( c + 1 ) == 'i' ) {
            buffer.setCharAt( c, '%' );
            buffer.deleteCharAt( c + 1 );
            substCount++;
          }
          else if ( buffer.charAt( c ) == 'i' && buffer.charAt( c + 1 ) == 'e' ) {
            buffer.setCharAt( c, '&' );
            buffer.deleteCharAt( c + 1 );
            substCount++;
          }
          else if ( buffer.charAt( c ) == 'i' && buffer.charAt( c + 1 ) == 'g' ) {
            buffer.setCharAt( c, '#' );
            buffer.deleteCharAt( c + 1 );
            substCount++;
          }
          else if ( buffer.charAt( c ) == 's' && buffer.charAt( c + 1 ) == 't' ) {
            buffer.setCharAt( c, '!' );
            buffer.deleteCharAt( c + 1 );
            substCount++;
          }
        }
      }
    }

    /**
     * Undoes the changes made by substitute(). That are character pairs and
     * character combinations. Umlauts will remain as their corresponding vowel,
     * as "ß" remains as "ss".
     */
    private void resubstitute( StringBuilder buffer )
    {
      for ( int c = 0; c < buffer.length(); c++ ) {
        if ( buffer.charAt( c ) == '*' ) {
          char x = buffer.charAt( c - 1 );
          buffer.setCharAt( c, x );
        }
        else if ( buffer.charAt( c ) == '$' ) {
          buffer.setCharAt( c, 's' );
          buffer.insert( c + 1, new char[]{'c', 'h'}, 0, 2 );
        }
        else if ( buffer.charAt( c ) == '§' ) {
          buffer.setCharAt( c, 'c' );
          buffer.insert( c + 1, 'h' );
        }
        else if ( buffer.charAt( c ) == '%' ) {
          buffer.setCharAt( c, 'e' );
          buffer.insert( c + 1, 'i' );
        }
        else if ( buffer.charAt( c ) == '&' ) {
          buffer.setCharAt( c, 'i' );
          buffer.insert( c + 1, 'e' );
        }
        else if ( buffer.charAt( c ) == '#' ) {
          buffer.setCharAt( c, 'i' );
          buffer.insert( c + 1, 'g' );
        }
        else if ( buffer.charAt( c ) == '!' ) {
          buffer.setCharAt( c, 's' );
          buffer.insert( c + 1, 't' );
        }
      }
    }
    
}

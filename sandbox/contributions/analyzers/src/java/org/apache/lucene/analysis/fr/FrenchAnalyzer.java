package org.apache.lucene.analysis.fr;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2004 The Apache Software Foundation.  All rights
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import java.io.File;
import java.io.Reader;
import java.util.Hashtable;
import org.apache.lucene.analysis.de.WordlistLoader;

/**
 * Analyzer for french language. Supports an external list of stopwords (words that
 * will not be indexed at all) and an external list of exclusions (word that will
 * not be stemmed, but indexed).
 * A default set of stopwords is used unless an other list is specified, the
 * exclusionlist is empty by default.
 *
 * @author    Patrick Talbot (based on Gerhard Schwarz work for German)
 */
public final class FrenchAnalyzer extends Analyzer {

  /**
   * Extended list of typical french stopwords.
   */
  private String[] FRENCH_STOP_WORDS = {
    "a", "afin", "ai", "ainsi", "aprÃ¨s", "attendu", "au", "aujourd", "auquel", "aussi",
    "autre", "autres", "aux", "auxquelles", "auxquels", "avait", "avant", "avec", "avoir",
    "c", "car", "ce", "ceci", "cela", "celle", "celles", "celui", "cependant", "certain",
    "certaine", "certaines", "certains", "ces", "cet", "cette", "ceux", "chez", "ci",
    "combien", "comme", "comment", "concernant", "contre", "d", "dans", "de", "debout",
    "dedans", "dehors", "delÃ ", "depuis", "derriÃ¨re", "des", "dÃ©sormais", "desquelles",
    "desquels", "dessous", "dessus", "devant", "devers", "devra", "divers", "diverse",
    "diverses", "doit", "donc", "dont", "du", "duquel", "durant", "dÃ¨s", "elle", "elles",
    "en", "entre", "environ", "est", "et", "etc", "etre", "eu", "eux", "exceptÃ©", "hormis",
    "hors", "hÃ©las", "hui", "il", "ils", "j", "je", "jusqu", "jusque", "l", "la", "laquelle",
    "le", "lequel", "les", "lesquelles", "lesquels", "leur", "leurs", "lorsque", "lui", "lÃ ",
    "ma", "mais", "malgrÃ©", "me", "merci", "mes", "mien", "mienne", "miennes", "miens", "moi",
    "moins", "mon", "moyennant", "mÃªme", "mÃªmes", "n", "ne", "ni", "non", "nos", "notre",
    "nous", "nÃ©anmoins", "nÃ´tre", "nÃ´tres", "on", "ont", "ou", "outre", "oÃ¹", "par", "parmi",
    "partant", "pas", "passÃ©", "pendant", "plein", "plus", "plusieurs", "pour", "pourquoi",
    "proche", "prÃ¨s", "puisque", "qu", "quand", "que", "quel", "quelle", "quelles", "quels",
    "qui", "quoi", "quoique", "revoici", "revoilÃ ", "s", "sa", "sans", "sauf", "se", "selon",
    "seront", "ses", "si", "sien", "sienne", "siennes", "siens", "sinon", "soi", "soit",
    "son", "sont", "sous", "suivant", "sur", "ta", "te", "tes", "tien", "tienne", "tiennes",
    "tiens", "toi", "ton", "tous", "tout", "toute", "toutes", "tu", "un", "une", "va", "vers",
    "voici", "voilÃ ", "vos", "votre", "vous", "vu", "vÃ´tre", "vÃ´tres", "y", "Ã ", "Ã§a", "Ã¨s",
    "Ã©tÃ©", "Ãªtre", "Ã´"
  };

  /**
   * Contains the stopwords used with the StopFilter.
   */
  private Hashtable stoptable = new Hashtable();
  /**
   * Contains words that should be indexed but not stemmed.
   */
  private Hashtable excltable = new Hashtable();

  /**
   * Builds an analyzer.
   */
  public FrenchAnalyzer() {
    stoptable = StopFilter.makeStopTable(FRENCH_STOP_WORDS);
  }

  /**
   * Builds an analyzer with the given stop words.
   */
  public FrenchAnalyzer(String[] stopwords) {
    stoptable = StopFilter.makeStopTable(stopwords);
  }

  /**
   * Builds an analyzer with the given stop words.
   */
  public FrenchAnalyzer(Hashtable stopwords) {
    stoptable = stopwords;
  }

  /**
   * Builds an analyzer with the given stop words.
   */
  public FrenchAnalyzer(File stopwords) {
    stoptable = WordlistLoader.getWordtable(stopwords);
  }

  /**
   * Builds an exclusionlist from an array of Strings.
   */
  public void setStemExclusionTable(String[] exclusionlist) {
    excltable = StopFilter.makeStopTable(exclusionlist);
  }

  /**
   * Builds an exclusionlist from a Hashtable.
   */
  public void setStemExclusionTable(Hashtable exclusionlist) {
    excltable = exclusionlist;
  }

  /**
   * Builds an exclusionlist from the words contained in the given file.
   */
  public void setStemExclusionTable(File exclusionlist) {
    excltable = WordlistLoader.getWordtable(exclusionlist);
  }

  /**
   * Creates a TokenStream which tokenizes all the text in the provided Reader.
   *
   * @return  A TokenStream build from a StandardTokenizer filtered with
   * 			StandardFilter, StopFilter, FrenchStemFilter and LowerCaseFilter
   */
  public final TokenStream tokenStream(String fieldName, Reader reader) {

    if (fieldName == null)
      throw new IllegalArgumentException
        ("fieldName must not be null");
    if (reader == null) throw new IllegalArgumentException("reader must not be null");

    TokenStream result = new StandardTokenizer(reader);
    result = new StandardFilter(result);
    result = new StopFilter(result, stoptable);
    result = new FrenchStemFilter(result, excltable);
    // Convert to lowercase after stemming!
    result = new LowerCaseFilter(result);
    return result;
  }
}

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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Set;
import java.util.HashSet;

/**
 * A filter that stems German words. It supports a table of words that should
 * not be stemmed at all. The stemmer used can be changed at runtime after the
 * filter object is created (as long as it is a GermanStemmer).
 *
 * @author    Gerhard Schwarz
 * @version   $Id$
 */
public final class GermanStemFilter extends TokenFilter
{
    /**
     * The actual token in the input stream.
     */
    private Token token = null;
    private GermanStemmer stemmer = null;
    private Set exclusionSet = null;
    
    public GermanStemFilter( TokenStream in )
    {
        super(in);
	stemmer = new GermanStemmer();
    }
    
    /**
     * Builds a GermanStemFilter that uses an exclusiontable.
     * @deprecated Use {@link #GermanStemFilter(org.apache.lucene.analysis.TokenStream, java.util.Set)} instead.
     */
    public GermanStemFilter( TokenStream in, Hashtable exclusiontable )
    {
	this( in );
	exclusionSet = new HashSet(exclusiontable.keySet());

    }

    /**
     * Builds a GermanStemFilter that uses an exclusiontable.
     */
    public GermanStemFilter( TokenStream in, Set exclusionSet )
    {
	this( in );
	this.exclusionSet = exclusionSet;
    }

    /**
     * @return  Returns the next token in the stream, or null at EOS
     */
    public final Token next()
	throws IOException
    {
	if ( ( token = input.next() ) == null ) {
	    return null;
	}
	// Check the exclusiontable
	else if ( exclusionSet != null && exclusionSet.contains( token.termText() ) ) {
	    return token;
	}
	else {
	    String s = stemmer.stem( token.termText() );
	    // If not stemmed, dont waste the time creating a new token
	    if ( !s.equals( token.termText() ) ) {
		return new Token( s, token.startOffset(),
		    token.endOffset(), token.type() );
	    }
	    return token;
	}
    }

    /**
     * Set a alternative/custom GermanStemmer for this filter.
     */
    public void setStemmer( GermanStemmer stemmer )
    {
	if ( stemmer != null ) {
	    this.stemmer = stemmer;
	}
    }

    /**
     * Set an alternative exclusion list for this filter.
     * @deprecated Use {@link #setExclusionSet(java.util.Set)} instead.
     */
    public void setExclusionTable( Hashtable exclusiontable )
    {
	exclusionSet = new HashSet(exclusiontable.keySet());
    }

    /**
     * Set an alternative exclusion list for this filter.
     */
    public void setExclusionSet( Set exclusionSet )
    {
	this.exclusionSet = exclusionSet;
    }
}

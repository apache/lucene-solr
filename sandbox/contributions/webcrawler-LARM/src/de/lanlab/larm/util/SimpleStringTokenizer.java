package de.lanlab.larm.util;

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


/**
 * A simple string tokenizer that regards <b>one</b> character as a delimiter.
 * Compared to Sun's StringTokenizer, it returns an empty token if two
 * subsequent delimiters are found
 *
 * @author    Clemens Marschner
 * @created   24. März 2002
 */
public class SimpleStringTokenizer
{

    String string;

    int currPos;
    int maxPos;
    char delim;


    /**
     * Constructor for the SimpleStringTokenizer object
     *
     * @param string  the string to be tokenized
     * @param delim   the delimiter that splits the string
     */
    public SimpleStringTokenizer(String string, char delim)
    {
        setString(string);
        setDelim(delim);
    }


    /**
     * sets the delimiter. The tokenizer is not reset.
     *
     * @param delim  The new delim value
     */
    public void setDelim(char delim)
    {
        this.delim = delim;
    }


    /**
     * sets the string and reinitializes the tokenizer. Allows for reusing the
     * tokenizer object
     *
     * @param string  string to be tokenized
     */
    public void setString(String string)
    {
        this.string = string;
        reset();

        maxPos = string.length() - 1;
    }


    /**
     * resets the tokenizer. It will act like newly created
     */
    public void reset()
    {
        currPos = 0;
    }


    /**
     * returns true if the end is not reached
     *
     * @return   false if the end is reached.
     */
    public boolean hasMore()
    {
        return currPos <= maxPos;
    }


    /**
     * returns the next token from the stream. returns an empty string if the
     * end is reached
     *
     * @return   Description of the Return Value
     * @see      java.util.StringTokenizer#nextToken
     */
    public String nextToken()
    {
        int nextPos = string.indexOf(delim, currPos);
        if (nextPos == -1)
        {
            nextPos = maxPos + 1;
        }
        String sub;
        if (nextPos > currPos)
        {
            sub = string.substring(currPos, nextPos);
        }
        else
        {
            sub = "";
        }
        currPos = nextPos + 1;
        return sub;
    }
}


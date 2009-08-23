package org.apache.lucene.analysis.ru;

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

import java.io.Reader;
import org.apache.lucene.analysis.CharTokenizer;
import org.apache.lucene.analysis.Tokenizer; // for javadocs
import org.apache.lucene.analysis.LetterTokenizer; // for javadocs
import org.apache.lucene.util.AttributeSource;

/**
 * A RussianLetterTokenizer is a {@link Tokenizer} that extends {@link LetterTokenizer}
 * by additionally looking up letters in a given "russian charset". 
 * <p>
 * The problem with 
 * {@link LetterTokenizer} is that it uses {@link Character#isLetter(char)} method,
 * which doesn't know how to detect letters in encodings like CP1252 and KOI8
 * (well-known problems with 0xD7 and 0xF7 chars)
 * </p>
 *
 * @version $Id$
 */

public class RussianLetterTokenizer extends CharTokenizer
{
    /** 
     * Charset this tokenizer uses.
     * @deprecated Support for non-Unicode encodings will be removed in Lucene 3.0
     */
    private char[] charset;

    /**
     * @deprecated Use {@link #RussianLetterTokenizer(Reader)} instead. 
     */
    public RussianLetterTokenizer(Reader in, char[] charset)
    {
        super(in);
        this.charset = charset;
    }
    
    public RussianLetterTokenizer(Reader in)
    {
    	this(in, RussianCharsets.UnicodeRussian);
    }

    public RussianLetterTokenizer(AttributeSource source, Reader in)
    {
        super(source, in);
        this.charset = RussianCharsets.UnicodeRussian;
    }

    public RussianLetterTokenizer(AttributeFactory factory, Reader in)
    {
        super(factory, in);
        this.charset = RussianCharsets.UnicodeRussian;
    }
    
    /**
     * Collects only characters which satisfy
     * {@link Character#isLetter(char)}.
     */
    protected boolean isTokenChar(char c)
    {
    	/* in the next release, this can be implemented as isLetter(c) or [0-9] */
        if (Character.isLetter(c))
            return true;
        for (int i = 0; i < charset.length; i++)
        {
            if (c == charset[i])
                return true;
        }
        return false;
    }
}

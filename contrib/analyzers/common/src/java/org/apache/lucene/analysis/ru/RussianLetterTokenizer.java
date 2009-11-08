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
 * by also allowing the basic latin digits 0-9. 
 */

public class RussianLetterTokenizer extends CharTokenizer
{    
    public RussianLetterTokenizer(Reader in)
    {
    	super(in);
    }

    public RussianLetterTokenizer(AttributeSource source, Reader in)
    {
        super(source, in);
    }

    public RussianLetterTokenizer(AttributeFactory factory, Reader in)
    {
        super(factory, in);
    }
    
    /**
     * Collects only characters which satisfy
     * {@link Character#isLetter(char)}.
     */
    @Override
    protected boolean isTokenChar(char c)
    {
        if (Character.isLetter(c) || (c >= '0' && c <= '9'))
            return true;
        else
            return false;
    }
}

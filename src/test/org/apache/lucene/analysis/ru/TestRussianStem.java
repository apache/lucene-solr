package org.apache.lucene.analysis.ru;

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

import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.util.ArrayList;

public class TestRussianStem extends TestCase
{
    private ArrayList words = new ArrayList();
    private ArrayList stems = new ArrayList();

    public TestRussianStem(String name)
    {
        super(name);
    }

    /**
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception
    {
        super.setUp();
        //System.out.println(new java.util.Date());
        String str;
        
        File dataDir = new File(System.getProperty("dataDir"));

        // open and read words into an array list
        BufferedReader inWords =
            new BufferedReader(
                new InputStreamReader(
                    new FileInputStream(new File(dataDir, "/org/apache/lucene/analysis/ru/wordsUnicode.txt")),
                    "Unicode"));
        while ((str = inWords.readLine()) != null)
        {
            words.add(str);
        }
        inWords.close();

        // open and read stems into an array list
        BufferedReader inStems =
            new BufferedReader(
                new InputStreamReader(
                    new FileInputStream(new File(dataDir, "/org/apache/lucene/analysis/ru/stemsUnicode.txt")),
                    "Unicode"));
        while ((str = inStems.readLine()) != null)
        {
            stems.add(str);
        }
        inStems.close();
    }

    /**
     * @see TestCase#tearDown()
     */
    protected void tearDown() throws Exception
    {
        super.tearDown();
    }

    public void testStem()
    {
        for (int i = 0; i < words.size(); i++)
        {
            //if ( (i % 100) == 0 ) System.err.println(i);
            String realStem =
                RussianStemmer.stem(
                    (String) words.get(i),
                    RussianCharsets.UnicodeRussian);
            assertEquals("unicode", stems.get(i), realStem);
        }
    }

    private String printChars(String output)
    {
        StringBuffer s = new StringBuffer();
        for (int i = 0; i < output.length(); i++)
            {
            s.append(output.charAt(i));
        }
        return s.toString();
    }
}

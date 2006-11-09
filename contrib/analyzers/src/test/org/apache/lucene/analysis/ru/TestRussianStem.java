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
        
        File dataDir = new File(System.getProperty("dataDir", "./bin"));

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

}

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

import org.apache.lucene.util.LuceneTestCase;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.util.ArrayList;

/**
 * @deprecated Remove this test class (and its datafiles!) in Lucene 4.0
 */
@Deprecated
public class TestRussianStem extends LuceneTestCase
{
    private ArrayList<String> words = new ArrayList<String>();
    private ArrayList<String> stems = new ArrayList<String>();

    public TestRussianStem(String name)
    {
        super(name);
    }

    /**
     * @see TestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        //System.out.println(new java.util.Date());
        String str;
        
        // open and read words into an array list
        BufferedReader inWords =
            new BufferedReader(
                new InputStreamReader(
                    getClass().getResourceAsStream("wordsUTF8.txt"),
                    "UTF-8"));
        while ((str = inWords.readLine()) != null)
        {
            words.add(str);
        }
        inWords.close();

        // open and read stems into an array list
        BufferedReader inStems =
            new BufferedReader(
                new InputStreamReader(
                    getClass().getResourceAsStream("stemsUTF8.txt"),
                    "UTF-8"));
        while ((str = inStems.readLine()) != null)
        {
            stems.add(str);
        }
        inStems.close();
    }

    public void testStem()
    {
        for (int i = 0; i < words.size(); i++)
        {
            //if ( (i % 100) == 0 ) System.err.println(i);
            String realStem =
                RussianStemmer.stemWord(
                    words.get(i));
            assertEquals("unicode", stems.get(i), realStem);
        }
    }

}

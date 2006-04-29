package org.apache.lucene.analysis.cn;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.StringReader;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Token;

/**
 * @author rayt
 */
public class TestChineseTokenizer extends TestCase
{
    public void testOtherLetterOffset() throws IOException
    {
        String s = "a天b";
        ChineseTokenizer tokenizer = new ChineseTokenizer(new StringReader(s));
        Token token;

        int correctStartOffset = 0;
        int correctEndOffset = 1;
        while ((token = tokenizer.next()) != null)
        {
            assertEquals(correctStartOffset, token.startOffset());
            assertEquals(correctEndOffset, token.endOffset());
            correctStartOffset++;
            correctEndOffset++;
        }
    }
}

package org.apache.lucene.analysis.cn;

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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.Version;


/** @deprecated Remove this test when ChineseAnalyzer is removed. */
@Deprecated
public class TestChineseTokenizer extends BaseTokenStreamTestCase
{
    public void testOtherLetterOffset() throws IOException
    {
        String s = "a天b";
        ChineseTokenizer tokenizer = new ChineseTokenizer(new StringReader(s));

        int correctStartOffset = 0;
        int correctEndOffset = 1;
        OffsetAttribute offsetAtt = tokenizer.getAttribute(OffsetAttribute.class);
        while (tokenizer.incrementToken()) {
          assertEquals(correctStartOffset, offsetAtt.startOffset());
          assertEquals(correctEndOffset, offsetAtt.endOffset());
          correctStartOffset++;
          correctEndOffset++;
        }
    }
    
    public void testReusableTokenStream() throws Exception
    {
      Analyzer a = new ChineseAnalyzer();
      assertAnalyzesToReuse(a, "中华人民共和国", 
        new String[] { "中", "华", "人", "民", "共", "和", "国" },
        new int[] { 0, 1, 2, 3, 4, 5, 6 },
        new int[] { 1, 2, 3, 4, 5, 6, 7 });
      assertAnalyzesToReuse(a, "北京市", 
        new String[] { "北", "京", "市" },
        new int[] { 0, 1, 2 },
        new int[] { 1, 2, 3 });
    }
    
    /*
     * Analyzer that just uses ChineseTokenizer, not ChineseFilter.
     * convenience to show the behavior of the tokenizer
     */
    private class JustChineseTokenizerAnalyzer extends Analyzer {
      @Override
      public TokenStreamComponents createComponents(String fieldName, Reader reader) {
        return new TokenStreamComponents(new ChineseTokenizer(reader));
      }   
    }
    
    /*
     * Analyzer that just uses ChineseFilter, not ChineseTokenizer.
     * convenience to show the behavior of the filter.
     */
    private class JustChineseFilterAnalyzer extends Analyzer {
      @Override
      public TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new WhitespaceTokenizer(Version.LUCENE_CURRENT, reader);
        return new TokenStreamComponents(tokenizer, new ChineseFilter(tokenizer));
      }
    }
    
    /*
     * ChineseTokenizer tokenizes numbers as one token, but they are filtered by ChineseFilter
     */
    public void testNumerics() throws Exception
    { 
      Analyzer justTokenizer = new JustChineseTokenizerAnalyzer();
      assertAnalyzesTo(justTokenizer, "中1234", new String[] { "中", "1234" });
          
      // in this case the ChineseAnalyzer (which applies ChineseFilter) will remove the numeric token.
      Analyzer a = new ChineseAnalyzer(); 
      assertAnalyzesTo(a, "中1234", new String[] { "中" });
    }
    
    /*
     * ChineseTokenizer tokenizes english similar to SimpleAnalyzer.
     * it will lowercase terms automatically.
     * 
     * ChineseFilter has an english stopword list, it also removes any single character tokens.
     * the stopword list is case-sensitive.
     */
    public void testEnglish() throws Exception
    {
      Analyzer chinese = new ChineseAnalyzer();
      assertAnalyzesTo(chinese, "This is a Test. b c d",
          new String[] { "test" });
      
      Analyzer justTokenizer = new JustChineseTokenizerAnalyzer();
      assertAnalyzesTo(justTokenizer, "This is a Test. b c d",
          new String[] { "this", "is", "a", "test", "b", "c", "d" });
      
      Analyzer justFilter = new JustChineseFilterAnalyzer();
      assertAnalyzesTo(justFilter, "This is a Test. b c d", 
          new String[] { "This", "Test." });
    }
    
    /** blast some random strings through the analyzer */
    public void testRandomStrings() throws Exception {
      checkRandomData(random(), new ChineseAnalyzer(), 10000*RANDOM_MULTIPLIER);
    }

}

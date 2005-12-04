package org.apache.lucene.analysis.cn;

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
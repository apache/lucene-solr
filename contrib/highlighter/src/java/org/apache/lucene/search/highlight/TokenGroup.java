package org.apache.lucene.search.highlight;
/**
 * Copyright 2002-2004 The Apache Software Foundation
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
import org.apache.lucene.analysis.Token;

/**
 * One, or several overlapping tokens, along with the score(s) and the
 * scope of the original text
 * @author MAHarwood
 */
public class TokenGroup
{
	
	private static final int MAX_NUM_TOKENS_PER_GROUP=50;
	Token [] tokens=new Token[MAX_NUM_TOKENS_PER_GROUP];
	float [] scores=new float[MAX_NUM_TOKENS_PER_GROUP];
	int numTokens=0;
	int startOffset=0;
	int endOffset=0;
	

	void addToken(Token token, float score)
	{
	    if(numTokens < MAX_NUM_TOKENS_PER_GROUP)
        {	    
			if(numTokens==0)
			{
				startOffset=token.startOffset();		
				endOffset=token.endOffset();		
			}
			else
			{
				startOffset=Math.min(startOffset,token.startOffset());		
				endOffset=Math.max(endOffset,token.endOffset());		
			}
			tokens[numTokens]=token;
			scores[numTokens]=score;
			numTokens++;
        }
	}
	
	boolean isDistinct(Token token)
	{
		return token.startOffset()>=endOffset;
	}
	
	
	void clear()
	{
		numTokens=0;
	}
	
	/**
	 * 
	 * @param index a value between 0 and numTokens -1
	 * @return the "n"th token
	 */
	public Token getToken(int index)
	{
		return tokens[index];
	}

	/**
	 * 
	 * @param index a value between 0 and numTokens -1
	 * @return the "n"th score
	 */
	public float getScore(int index)
	{
		return scores[index];
	}

	/**
	 * @return the end position in the original text
	 */
	public int getEndOffset()
	{
		return endOffset;
	}

	/**
	 * @return the number of tokens in this group
	 */
	public int getNumTokens()
	{
		return numTokens;
	}

	/**
	 * @return the start position in the original text
	 */
	public int getStartOffset()
	{
		return startOffset;
	}

	/**
	 * @return all tokens' scores summed up
	 */
	public float getTotalScore()
	{
		float total=0;
		for (int i = 0; i < numTokens; i++)
		{
			total+=scores[i];
		}
		return total;
	}
}

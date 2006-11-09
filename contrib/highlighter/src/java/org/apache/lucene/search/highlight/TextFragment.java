package org.apache.lucene.search.highlight;
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


/**
 * Low-level class used to record information about a section of a document 
 * with a score.
 * @author MAHarwood
 *
 * 
 */
public class TextFragment
{
	StringBuffer markedUpText;
	int fragNum;
	int textStartPos;
	int textEndPos;
	float score;

	public TextFragment(StringBuffer markedUpText,int textStartPos, int fragNum)
	{
		this.markedUpText=markedUpText;
		this.textStartPos = textStartPos;
		this.fragNum = fragNum;
	}
	void setScore(float score)
	{
		this.score=score;
	}
	public float getScore()
	{
		return score;
	}
	/**
	 * @param frag2 Fragment to be merged into this one
	 */
  public void merge(TextFragment frag2)
  {
    textEndPos = frag2.textEndPos;
    score=Math.max(score,frag2.score);
  }
  /**
	 * @param fragment 
	 * @return true if this fragment follows the one passed
	 */
	public boolean follows(TextFragment fragment)
	{
		return textStartPos == fragment.textEndPos;
	}

	/**
	 * @return the fragment sequence number
	 */
	public int getFragNum()
	{
		return fragNum;
	}

	/* Returns the marked-up text for this text fragment 
	 */
	public String toString() {
		return markedUpText.substring(textStartPos, textEndPos);
	}

}

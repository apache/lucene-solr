package org.apache.lucene.beans;

import org.apache.lucene.document.Document;

public class IndividualHit
{
	private float score;
	private String field;
	private int index;
	
	private IndividualHit()
	{
	}
	
	public IndividualHit(int inIndex, String field, float inScore)
	{
		this.index = inIndex;
		this.field = field;
		this.score = inScore;
	}
	
	public int getIndex()
	{
		return this.index;
	}
	
	public String getField()
	{
		return this.field;
	}
	
	public float getScore()
	{
		return this.score;
	}
}
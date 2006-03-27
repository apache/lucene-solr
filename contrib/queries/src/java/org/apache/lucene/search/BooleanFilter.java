package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause.Occur;

/**
 * A container Filter that allows Boolean composition of Filters.
 * Filters are allocated into one of three logical constructs;
 * SHOULD, MUST NOT, MUST
 * The results Filter BitSet is constructed as follows:
 * SHOULD Filters are OR'd together
 * The resulting Filter is NOT'd with the NOT Filters
 * The resulting Filter is AND'd with the MUST Filters
 * @author BPDThebault
 */

public class BooleanFilter extends Filter
{
	//ArrayList of SHOULD filters
	ArrayList shouldFilters = null;
	//ArrayList of NOT filters
	ArrayList notFilters = null;
	//ArrayList of MUST filters
	ArrayList mustFilters = null;

	/**
	 * Returns the a BitSet representing the Boolean composition
	 * of the filters that have been added.
	 */
	
	public BitSet bits(IndexReader reader) throws IOException
	{
		//create a new bitSet
		BitSet returnBits = null;
		
		//SHOULD filters
		if (shouldFilters!=null)
		{
			returnBits = ((Filter)shouldFilters.get(0)).bits(reader);
			if (shouldFilters.size() > 1)
			{
				
				for (int i = 1; i < shouldFilters.size(); i++)
				{
					returnBits.or(((Filter)shouldFilters.get(i)).bits(reader));
				}
			}
		}
		
		//NOT filters
		if (notFilters!=null)
		{
			for (int i = 0; i < notFilters.size(); i++)
			{
				BitSet notBits=((Filter)notFilters.get(i)).bits(reader);
				if(returnBits==null)
				{
					returnBits=notBits;
					returnBits.flip(0,reader.maxDoc());
				}
				else
				{
					returnBits.andNot(notBits);
				}
			}
		}
		
		//MUST filters
		if (mustFilters!=null)
		{
			for (int i = 0; i < mustFilters.size(); i++)
			{
				BitSet mustBits=((Filter)mustFilters.get(i)).bits(reader);
				if(returnBits==null)
				{
					returnBits=mustBits;
				}
				else
				{
					returnBits.and(mustBits);
				}
			}
		}
		if(returnBits==null)
		{
			returnBits=new BitSet(reader.maxDoc());
		}
		return returnBits;
	}
	
	/**
	 * Adds a new FilterClause to the Boolean Filter container
	 * @param filterClause A FilterClause object containing a Filter and an Occur parameter
	 */
	
	public void add(FilterClause filterClause)
	{
		if (filterClause.getOccur().equals(Occur.MUST))
		{
			if(mustFilters==null)
			{
				mustFilters=new ArrayList();
			}
			mustFilters.add(filterClause.getFilter());
		}
		if (filterClause.getOccur().equals(Occur.SHOULD))
		{
			if(shouldFilters==null)
			{
				shouldFilters=new ArrayList();
			}
			shouldFilters.add(filterClause.getFilter());
		}
		if (filterClause.getOccur().equals(Occur.MUST_NOT))
		{
			if(notFilters==null)
			{
				notFilters=new ArrayList();
			}
			notFilters.add(filterClause.getFilter());
		}
	}
}

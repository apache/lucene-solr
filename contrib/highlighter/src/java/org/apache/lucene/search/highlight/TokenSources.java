/*
 * Created on 28-Oct-2004
 */
package org.apache.lucene.search.highlight;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositionVector;
import org.apache.lucene.index.TermVectorOffsetInfo;

/**
 * Hides implementation issues associated with obtaining a TokenStream for use with
 * the higlighter - can obtain from TermFreqVectors with offsets and (optionally) positions or
 * from Analyzer class reparsing the stored content. 
 * @author maharwood
 */
public class TokenSources
{
    /**
     * A convenience method that tries a number of approaches to getting a token stream.
     * The cost of finding there are no termVectors in the index is minimal (1000 invocations still 
     * registers 0 ms). So this "lazy" (flexible?) approach to coding is probably acceptable
     * @param reader
     * @param docId
     * @param field
     * @param analyzer
     * @return null if field not stored correctly 
     * @throws IOException
     */
    public static TokenStream getAnyTokenStream(IndexReader reader,int docId, String field,Analyzer analyzer) throws IOException
    {
		TokenStream ts=null;

		TermFreqVector tfv=(TermFreqVector) reader.getTermFreqVector(docId,field);
		if(tfv!=null)
		{
		    if(tfv instanceof TermPositionVector)
		    {
		        ts=getTokenStream((TermPositionVector) tfv);
		    }
		}
		//No token info stored so fall back to analyzing raw content
		if(ts==null)
		{
		    ts=getTokenStream(reader,docId,field,analyzer);
		}
		return ts;
    }
    
    
    public static TokenStream getTokenStream(TermPositionVector tpv)
    {
        //assumes the worst and makes no assumptions about token position sequences.
         return getTokenStream(tpv,false);   
    }
    /**
     * Low level api.
     * Returns a token stream or null if no offset info available in index.
     * This can be used to feed the highlighter with a pre-parsed token stream 
     * 
     * In my tests the speeds to recreate 1000 token streams using this method are:
     * - with TermVector offset only data stored - 420  milliseconds 
     * - with TermVector offset AND position data stored - 271 milliseconds
     *  (nb timings for TermVector with position data are based on a tokenizer with contiguous
     *  positions - no overlaps or gaps)
     * The cost of not using TermPositionVector to store
     * pre-parsed content and using an analyzer to re-parse the original content: 
     * - reanalyzing the original content - 980 milliseconds
     * 
     * The re-analyze timings will typically vary depending on -
     * 	1) The complexity of the analyzer code (timings above were using a 
     * 	   stemmer/lowercaser/stopword combo)
     *  2) The  number of other fields (Lucene reads ALL fields off the disk 
     *     when accessing just one document field - can cost dear!)
     *  3) Use of compression on field storage - could be faster cos of compression (less disk IO)
     *     or slower (more CPU burn) depending on the content.
     *
     * @param tpv
     * @param tokenPositionsGuaranteedContiguous true if the token position numbers have no overlaps or gaps. If looking
     * to eek out the last drops of performance, set to true. If in doubt, set to false.
     */
    public static TokenStream getTokenStream(TermPositionVector tpv, boolean tokenPositionsGuaranteedContiguous)
    {
        //an object used to iterate across an array of tokens
        class StoredTokenStream extends TokenStream
        {
            Token tokens[];
            int currentToken=0;
            StoredTokenStream(Token tokens[])
            {
                this.tokens=tokens;
            }
            public Token next()
            {
                if(currentToken>=tokens.length)
                {
                    return null;
                }
                return tokens[currentToken++];
            }            
        }        
        //code to reconstruct the original sequence of Tokens
        String[] terms=tpv.getTerms();          
        int[] freq=tpv.getTermFrequencies();
        int totalTokens=0;
        for (int t = 0; t < freq.length; t++)
        {
            totalTokens+=freq[t];
        }
        Token tokensInOriginalOrder[]=new Token[totalTokens];
        ArrayList unsortedTokens = null;
        for (int t = 0; t < freq.length; t++)
        {
            TermVectorOffsetInfo[] offsets=tpv.getOffsets(t);
            if(offsets==null)
            {
                return null;
            }
            
            int[] pos=null;
            if(tokenPositionsGuaranteedContiguous)
            {
                //try get the token position info to speed up assembly of tokens into sorted sequence
                pos=tpv.getTermPositions(t);
            }
            if(pos==null)
            {	
                //tokens NOT stored with positions or not guaranteed contiguous - must add to list and sort later
                if(unsortedTokens==null)
                {
                    unsortedTokens=new ArrayList();
                }
                for (int tp = 0; tp < offsets.length; tp++)
                {
                    unsortedTokens.add(new Token(terms[t],
                        offsets[tp].getStartOffset(),
                        offsets[tp].getEndOffset()));
                }
            }
            else
            {
                //We have positions stored and a guarantee that the token position information is contiguous
                
                // This may be fast BUT wont work if Tokenizers used which create >1 token in same position or
                // creates jumps in position numbers - this code would fail under those circumstances
                
                //tokens stored with positions - can use this to index straight into sorted array
                for (int tp = 0; tp < pos.length; tp++)
                {
                    tokensInOriginalOrder[pos[tp]]=new Token(terms[t],
                            offsets[tp].getStartOffset(),
                            offsets[tp].getEndOffset());
                }                
            }
        }
        //If the field has been stored without position data we must perform a sort        
        if(unsortedTokens!=null)
        {
            tokensInOriginalOrder=(Token[]) unsortedTokens.toArray(new Token[unsortedTokens.size()]);
            Arrays.sort(tokensInOriginalOrder, new Comparator(){
                public int compare(Object o1, Object o2)
                {
                    Token t1=(Token) o1;
                    Token t2=(Token) o2;
                    if(t1.startOffset()>t2.startOffset())
                        return 1;
                    if(t1.startOffset()<t2.startOffset())
                        return -1;
                    return 0;
                }});
        }
        return new StoredTokenStream(tokensInOriginalOrder);
    }

    public static TokenStream getTokenStream(IndexReader reader,int docId, String field) throws IOException
    {
		TermFreqVector tfv=(TermFreqVector) reader.getTermFreqVector(docId,field);
		if(tfv==null)
		{
		    throw new IllegalArgumentException(field+" in doc #"+docId
		            	+"does not have any term position data stored");
		}
	    if(tfv instanceof TermPositionVector)
	    {
			TermPositionVector tpv=(TermPositionVector) reader.getTermFreqVector(docId,field);
	        return getTokenStream(tpv);	        
	    }
	    throw new IllegalArgumentException(field+" in doc #"+docId
            	+"does not have any term position data stored");
    }

    //convenience method
    public static TokenStream getTokenStream(IndexReader reader,int docId, String field,Analyzer analyzer) throws IOException
    {
		Document doc=reader.document(docId);
		String contents=doc.get(field);
		if(contents==null)
		{
		    throw new IllegalArgumentException("Field "+field +" in document #"+docId+ " is not stored and cannot be analyzed");
		}
        return analyzer.tokenStream(field,new StringReader(contents));
    }
    
    

}

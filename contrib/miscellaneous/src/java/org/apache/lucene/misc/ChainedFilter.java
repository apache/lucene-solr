package org.apache.lucene.misc;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001-2003 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Filter;

import java.io.IOException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.OpenBitSetDISI;
import org.apache.lucene.util.SortedVIntList;

/**
 * <p>
 * Allows multiple {@link Filter}s to be chained.
 * Logical operations such as <b>NOT</b> and <b>XOR</b>
 * are applied between filters. One operation can be used
 * for all filters, or a specific operation can be declared
 * for each filter.
 * </p>
 * <p>
 * Order in which filters are called depends on
 * the position of the filter in the chain. It's probably
 * more efficient to place the most restrictive filters
 * /least computationally-intensive filters first.
 * </p>
 *
 */
public class ChainedFilter extends Filter
{
    public static final int OR = 0;
    public static final int AND = 1;
    public static final int ANDNOT = 2;
    public static final int XOR = 3;
    /**
     * Logical operation when none is declared. Defaults to
     * OR.
     */
    public static int DEFAULT = OR;

    /** The filter chain */
    private Filter[] chain = null;

    private int[] logicArray;

    private int logic = -1;

    /**
     * Ctor.
     * @param chain The chain of filters
     */
    public ChainedFilter(Filter[] chain)
    {
        this.chain = chain;
    }

    /**
     * Ctor.
     * @param chain The chain of filters
     * @param logicArray Logical operations to apply between filters
     */
    public ChainedFilter(Filter[] chain, int[] logicArray)
    {
        this.chain = chain;
        this.logicArray = logicArray;
    }

    /**
     * Ctor.
     * @param chain The chain of filters
     * @param logic Logicial operation to apply to ALL filters
     */
    public ChainedFilter(Filter[] chain, int logic)
    {
        this.chain = chain;
        this.logic = logic;
    }

    /**
     * {@link Filter#getDocIdSet}.
     */
    public DocIdSet getDocIdSet(IndexReader reader) throws IOException
    {
        int[] index = new int[1]; // use array as reference to modifiable int; 
        index[0] = 0;             // an object attribute would not be thread safe.
        if (logic != -1)
            return getDocIdSet(reader, logic, index);
        else if (logicArray != null)
            return getDocIdSet(reader, logicArray, index);
        else
            return getDocIdSet(reader, DEFAULT, index);
    }

    private DocIdSetIterator getDISI(Filter filter, IndexReader reader)
    throws IOException
    {
        return filter.getDocIdSet(reader).iterator();
    }

    private OpenBitSetDISI initialResult(IndexReader reader, int logic, int[] index)
    throws IOException
    {
        OpenBitSetDISI result;
        /**
         * First AND operation takes place against a completely false
         * bitset and will always return zero results.
         */
        if (logic == AND)
        {
            result = new OpenBitSetDISI(getDISI(chain[index[0]], reader), reader.maxDoc());
            ++index[0];
        }
        else if (logic == ANDNOT)
        {
            result = new OpenBitSetDISI(getDISI(chain[index[0]], reader), reader.maxDoc());
            result.flip(0,reader.maxDoc()); // NOTE: may set bits for deleted docs.
            ++index[0];
        }
        else
        {
            result = new OpenBitSetDISI(reader.maxDoc());
        }
        return result;
    }
    
    /** Provide a SortedVIntList when it is definitely smaller than an OpenBitSet */
    protected DocIdSet finalResult(OpenBitSetDISI result, int maxDocs) {
        return (result.cardinality() < (maxDocs / 9))
              ? (DocIdSet) new SortedVIntList(result)
              : (DocIdSet) result;
    }
        

    /**
     * Delegates to each filter in the chain.
     * @param reader IndexReader
     * @param logic Logical operation
     * @return DocIdSet
     */
    private DocIdSet getDocIdSet(IndexReader reader, int logic, int[] index)
    throws IOException
    {
        OpenBitSetDISI result = initialResult(reader, logic, index);
        for (; index[0] < chain.length; index[0]++)
        {
            doChain(result, logic, chain[index[0]].getDocIdSet(reader));
        }
        return finalResult(result, reader.maxDoc());
    }

    /**
     * Delegates to each filter in the chain.
     * @param reader IndexReader
     * @param logic Logical operation
     * @return DocIdSet
     */
    private DocIdSet getDocIdSet(IndexReader reader, int[] logic, int[] index)
    throws IOException
    {
        if (logic.length != chain.length)
            throw new IllegalArgumentException("Invalid number of elements in logic array");

        OpenBitSetDISI result = initialResult(reader, logic[0], index);
        for (; index[0] < chain.length; index[0]++)
        {
            doChain(result, logic[index[0]], chain[index[0]].getDocIdSet(reader));
        }
        return finalResult(result, reader.maxDoc());
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("ChainedFilter: [");
        for (int i = 0; i < chain.length; i++)
        {
            sb.append(chain[i]);
            sb.append(' ');
        }
        sb.append(']');
        return sb.toString();
    }

    private void doChain(OpenBitSetDISI result, int logic, DocIdSet dis)
    throws IOException
    {
      
      if (dis instanceof OpenBitSet) {
        // optimized case for OpenBitSets
        switch (logic)
        {
            case OR:
                result.or((OpenBitSet) dis);
                break;
            case AND:
                result.and((OpenBitSet) dis);
                break;
            case ANDNOT:
                result.andNot((OpenBitSet) dis);
                break;
            case XOR:
                result.xor((OpenBitSet) dis);
                break;
            default:
                doChain(result, DEFAULT, dis);
                break;
        }
      } else {
        DocIdSetIterator disi = dis.iterator();      
        switch (logic)
        {
            case OR:
                result.inPlaceOr(disi);
                break;
            case AND:
                result.inPlaceAnd(disi);
                break;
            case ANDNOT:
                result.inPlaceNot(disi);
                break;
            case XOR:
                result.inPlaceXor(disi);
                break;
            default:
                doChain(result, DEFAULT, dis);
                break;
        }
      }
    }

}

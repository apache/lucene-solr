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
import java.util.BitSet;

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
 * @author <a href="mailto:kelvint@apache.org">Kelvin Tan</a>
 */
public class ChainedFilter extends Filter
{
    /**
     * {@link BitSet#or}.
     */
    public static final int OR = 0;

    /**
     * {@link BitSet#and}.
     */
    public static final int AND = 1;

    /**
     * {@link BitSet#andNot}.
     */
    public static final int ANDNOT = 2;

    /**
     * {@link BitSet#xor}.
     */
    public static final int XOR = 3;

    /**
     * Logical operation when none is declared. Defaults to
     * {@link BitSet#or}.
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
     * {@link Filter#bits}.
     */
    public BitSet bits(IndexReader reader) throws IOException
    {
        if (logic != -1)
            return bits(reader, logic);
        else if (logicArray != null)
            return bits(reader, logicArray);
        else
            return bits(reader, DEFAULT);
    }

    /**
     * Delegates to each filter in the chain.
     * @param reader IndexReader
     * @param logic Logical operation
     * @return BitSet
     */
    private BitSet bits(IndexReader reader, int logic) throws IOException
    {
        BitSet result;
        int i = 0;

        /**
         * First AND operation takes place against a completely false
         * bitset and will always return zero results. Thanks to
         * Daniel Armbrust for pointing this out and suggesting workaround.
         */
        if (logic == AND)
        {
            result = (BitSet) chain[i].bits(reader).clone();
            ++i;
        }
        else if (logic == ANDNOT)
        {
            result = (BitSet) chain[i].bits(reader).clone();
            result.flip(0,reader.maxDoc());
            ++i;
        }
        else
        {
            result = new BitSet(reader.maxDoc());
        }

        for (; i < chain.length; i++)
        {
            doChain(result, reader, logic, chain[i]);
        }
        return result;
    }

    /**
     * Delegates to each filter in the chain.
     * @param reader IndexReader
     * @param logic Logical operation
     * @return BitSet
     */
    private BitSet bits(IndexReader reader, int[] logic) throws IOException
    {
        if (logic.length != chain.length)
            throw new IllegalArgumentException("Invalid number of elements in logic array");
        BitSet result;
        int i = 0;

        /**
         * First AND operation takes place against a completely false
         * bitset and will always return zero results. Thanks to
         * Daniel Armbrust for pointing this out and suggesting workaround.
         */
        if (logic[0] == AND)
        {
            result = (BitSet) chain[i].bits(reader).clone();
            ++i;
        }
        else if (logic[0] == ANDNOT)
        {
            result = (BitSet) chain[i].bits(reader).clone();
            result.flip(0,reader.maxDoc());
            ++i;
        }
        else
        {
            result = new BitSet(reader.maxDoc());
        }

        for (; i < chain.length; i++)
        {
            doChain(result, reader, logic[i], chain[i]);
        }
        return result;
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

    private void doChain(BitSet result, IndexReader reader,
                         int logic, Filter filter) throws IOException
    {
        switch (logic)
        {
            case OR:
                result.or(filter.bits(reader));
                break;
            case AND:
                result.and(filter.bits(reader));
                break;
            case ANDNOT:
                result.andNot(filter.bits(reader));
                break;
            case XOR:
                result.xor(filter.bits(reader));
                break;
            default:
                doChain(result, reader, DEFAULT, filter);
                break;
        }
    }
}

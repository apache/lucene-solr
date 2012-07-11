package org.apache.lucene.queries;

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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.OpenBitSetDISI;

import java.io.IOException;

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
 */
public class ChainedFilter extends Filter {

  public static final int OR = 0;
  public static final int AND = 1;
  public static final int ANDNOT = 2;
  public static final int XOR = 3;
  /**
   * Logical operation when none is declared. Defaults to OR.
   */
  public static final int DEFAULT = OR;

  /**
   * The filter chain
   */
  private Filter[] chain = null;

  private int[] logicArray;

  private int logic = -1;

  /**
   * Ctor.
   *
   * @param chain The chain of filters
   */
  public ChainedFilter(Filter[] chain) {
    this.chain = chain;
  }

  /**
   * Ctor.
   *
   * @param chain The chain of filters
   * @param logicArray Logical operations to apply between filters
   */
  public ChainedFilter(Filter[] chain, int[] logicArray) {
    this.chain = chain;
    this.logicArray = logicArray;
  }

  /**
   * Ctor.
   *
   * @param chain The chain of filters
   * @param logic Logical operation to apply to ALL filters
   */
  public ChainedFilter(Filter[] chain, int logic) {
    this.chain = chain;
    this.logic = logic;
  }

  /**
   * {@link Filter#getDocIdSet}.
   */
  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    int[] index = new int[1]; // use array as reference to modifiable int;
    index[0] = 0;             // an object attribute would not be thread safe.
    if (logic != -1) {
      return BitsFilteredDocIdSet.wrap(getDocIdSet(context, logic, index), acceptDocs);
    } else if (logicArray != null) {
      return BitsFilteredDocIdSet.wrap(getDocIdSet(context, logicArray, index), acceptDocs);
    }
    
    return BitsFilteredDocIdSet.wrap(getDocIdSet(context, DEFAULT, index), acceptDocs);
  }

  private DocIdSetIterator getDISI(Filter filter, AtomicReaderContext context)
      throws IOException {
    // we dont pass acceptDocs, we will filter at the end using an additional filter
    DocIdSet docIdSet = filter.getDocIdSet(context, null);
    if (docIdSet == null) {
      return DocIdSet.EMPTY_DOCIDSET.iterator();
    } else {
      DocIdSetIterator iter = docIdSet.iterator();
      if (iter == null) {
        return DocIdSet.EMPTY_DOCIDSET.iterator();
      } else {
        return iter;
      }
    }
  }

  private OpenBitSetDISI initialResult(AtomicReaderContext context, int logic, int[] index)
      throws IOException {
    AtomicReader reader = context.reader();
    OpenBitSetDISI result;
    /**
     * First AND operation takes place against a completely false
     * bitset and will always return zero results.
     */
    if (logic == AND) {
      result = new OpenBitSetDISI(getDISI(chain[index[0]], context), reader.maxDoc());
      ++index[0];
    } else if (logic == ANDNOT) {
      result = new OpenBitSetDISI(getDISI(chain[index[0]], context), reader.maxDoc());
      result.flip(0, reader.maxDoc()); // NOTE: may set bits for deleted docs.
      ++index[0];
    } else {
      result = new OpenBitSetDISI(reader.maxDoc());
    }
    return result;
  }

  /**
   * Delegates to each filter in the chain.
   *
   * @param context AtomicReaderContext
   * @param logic Logical operation
   * @return DocIdSet
   */
  private DocIdSet getDocIdSet(AtomicReaderContext context, int logic, int[] index)
      throws IOException {
    OpenBitSetDISI result = initialResult(context, logic, index);
    for (; index[0] < chain.length; index[0]++) {
      // we dont pass acceptDocs, we will filter at the end using an additional filter
      doChain(result, logic, chain[index[0]].getDocIdSet(context, null));
    }
    return result;
  }

  /**
   * Delegates to each filter in the chain.
   *
   * @param context AtomicReaderContext
   * @param logic Logical operation
   * @return DocIdSet
   */
  private DocIdSet getDocIdSet(AtomicReaderContext context, int[] logic, int[] index)
      throws IOException {
    if (logic.length != chain.length) {
      throw new IllegalArgumentException("Invalid number of elements in logic array");
    }

    OpenBitSetDISI result = initialResult(context, logic[0], index);
    for (; index[0] < chain.length; index[0]++) {
      // we dont pass acceptDocs, we will filter at the end using an additional filter
      doChain(result, logic[index[0]], chain[index[0]].getDocIdSet(context, null));
    }
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ChainedFilter: [");
    for (Filter aChain : chain) {
      sb.append(aChain);
      sb.append(' ');
    }
    sb.append(']');
    return sb.toString();
  }

  private void doChain(OpenBitSetDISI result, int logic, DocIdSet dis)
      throws IOException {

    if (dis instanceof OpenBitSet) {
      // optimized case for OpenBitSets
      switch (logic) {
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
      DocIdSetIterator disi;
      if (dis == null) {
        disi = DocIdSet.EMPTY_DOCIDSET.iterator();
      } else {
        disi = dis.iterator();
        if (disi == null) {
          disi = DocIdSet.EMPTY_DOCIDSET.iterator();
        }
      }

      switch (logic) {
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

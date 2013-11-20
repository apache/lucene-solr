package org.apache.lucene.facet.simple;

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
import java.util.List;

import org.apache.lucene.facet.simple.SimpleFacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

// nocommit jdoc that this assumes/requires the default encoding
public class TaxonomyFacetSumFloatAssociations extends TaxonomyFacets {
  private final float[] values;

  public TaxonomyFacetSumFloatAssociations(TaxonomyReader taxoReader, FacetsConfig config, SimpleFacetsCollector fc) throws IOException {
    this(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, taxoReader, config, fc);
  }

  public TaxonomyFacetSumFloatAssociations(String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config, SimpleFacetsCollector fc) throws IOException {
    super(indexFieldName, taxoReader, config);
    values = new float[taxoReader.getSize()];
    sumValues(fc.getMatchingDocs());
  }

  private final void sumValues(List<MatchingDocs> matchingDocs) throws IOException {
    //System.out.println("count matchingDocs=" + matchingDocs + " facetsField=" + facetsFieldName);
    for(MatchingDocs hits : matchingDocs) {
      BinaryDocValues dv = hits.context.reader().getBinaryDocValues(indexFieldName);
      if (dv == null) { // this reader does not have DocValues for the requested category list
        continue;
      }
      FixedBitSet bits = hits.bits;
    
      final int length = hits.bits.length();
      int doc = 0;
      BytesRef scratch = new BytesRef();
      //System.out.println("count seg=" + hits.context.reader());
      while (doc < length && (doc = bits.nextSetBit(doc)) != -1) {
        //System.out.println("  doc=" + doc);
        // nocommit use OrdinalsReader?  but, add a
        // BytesRef getAssociation()?
        dv.get(doc, scratch);
        byte[] bytes = scratch.bytes;
        int end = scratch.offset + scratch.length;
        int offset = scratch.offset;
        while (offset < end) {
          int ord = ((bytes[offset]&0xFF) << 24) |
            ((bytes[offset+1]&0xFF) << 16) |
            ((bytes[offset+2]&0xFF) << 8) |
            (bytes[offset+3]&0xFF);
          offset += 4;
          int value = ((bytes[offset]&0xFF) << 24) |
            ((bytes[offset+1]&0xFF) << 16) |
            ((bytes[offset+2]&0xFF) << 8) |
            (bytes[offset+3]&0xFF);
          offset += 4;
          values[ord] += Float.intBitsToFloat(value);
        }
        ++doc;
      }
    }
  }

  /** Return the count for a specific path.  Returns -1 if
   *  this path doesn't exist, else the count. */
  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    verifyDim(dim);
    int ord = taxoReader.getOrdinal(FacetLabel.create(dim, path));
    if (ord < 0) {
      return -1;
    }
    return values[ord];
  }

  @Override
  public SimpleFacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    FacetsConfig.DimConfig dimConfig = verifyDim(dim);
    FacetLabel cp = FacetLabel.create(dim, path);
    int dimOrd = taxoReader.getOrdinal(cp);
    if (dimOrd == -1) {
      //System.out.println("no ord for path=" + path);
      return null;
    }

    TopOrdAndFloatQueue q = new TopOrdAndFloatQueue(Math.min(taxoReader.getSize(), topN));
    float bottomValue = 0;

    int ord = children[dimOrd];
    float sumValue = 0;

    TopOrdAndFloatQueue.OrdAndValue reuse = null;
    while(ord != TaxonomyReader.INVALID_ORDINAL) {
      if (values[ord] > 0) {
        sumValue += values[ord];
        if (values[ord] > bottomValue) {
          if (reuse == null) {
            reuse = new TopOrdAndFloatQueue.OrdAndValue();
          }
          reuse.ord = ord;
          reuse.value = values[ord];
          reuse = q.insertWithOverflow(reuse);
          if (q.size() == topN) {
            bottomValue = q.top().value;
          }
        }
      }

      ord = siblings[ord];
    }

    if (sumValue == 0) {
      //System.out.println("totCount=0 for path=" + path);
      return null;
    }

    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    for(int i=labelValues.length-1;i>=0;i--) {
      TopOrdAndFloatQueue.OrdAndValue ordAndValue = q.pop();
      FacetLabel child = taxoReader.getPath(ordAndValue.ord);
      labelValues[i] = new LabelAndValue(child.components[path.length], ordAndValue.value);
    }

    return new SimpleFacetResult(cp, sumValue, labelValues);
  }
}

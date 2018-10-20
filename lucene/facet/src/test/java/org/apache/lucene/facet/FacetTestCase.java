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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.facet.taxonomy.CachedOrdinalsReader;
import org.apache.lucene.facet.taxonomy.DocValuesOrdinalsReader;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.OrdinalsReader;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public abstract class FacetTestCase extends LuceneTestCase {
  
  public Facets getTaxonomyFacetCounts(TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector c) throws IOException {
    return getTaxonomyFacetCounts(taxoReader, config, c, FacetsConfig.DEFAULT_INDEX_FIELD_NAME);
  }

  public Facets getTaxonomyFacetCounts(TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector c, String indexFieldName) throws IOException {
    Facets facets;
    if (random().nextBoolean()) {
      facets = new FastTaxonomyFacetCounts(indexFieldName, taxoReader, config, c);
    } else {
      OrdinalsReader ordsReader = new DocValuesOrdinalsReader(indexFieldName);
      if (random().nextBoolean()) {
        ordsReader = new CachedOrdinalsReader(ordsReader);
      }
      facets = new TaxonomyFacetCounts(ordsReader, taxoReader, config, c);
    }

    return facets;
  }

  protected String[] getRandomTokens(int count) {
    String[] tokens = new String[count];
    for(int i=0;i<tokens.length;i++) {
      tokens[i] = TestUtil.randomRealisticUnicodeString(random(), 1, 10);
      //tokens[i] = _TestUtil.randomSimpleString(random(), 1, 10);
    }
    return tokens;
  }

  protected String pickToken(String[] tokens) {
    for(int i=0;i<tokens.length;i++) {
      if (random().nextBoolean()) {
        return tokens[i];
      }
    }

    // Move long tail onto first token:
    return tokens[0];
  }

  protected static class TestDoc {
    public String content;
    public String[] dims;
    public float value;
  }

  protected List<TestDoc> getRandomDocs(String[] tokens, int count, int numDims) {
    List<TestDoc> docs = new ArrayList<>();
    for(int i=0;i<count;i++) {
      TestDoc doc = new TestDoc();
      docs.add(doc);
      doc.content = pickToken(tokens);
      doc.dims = new String[numDims];
      for(int j=0;j<numDims;j++) {
        doc.dims[j] = pickToken(tokens);
        if (random().nextInt(10) < 3) {
          break;
        }
      }
      if (VERBOSE) {
        System.out.println("  doc " + i + ": content=" + doc.content);
        for(int j=0;j<numDims;j++) {
          if (doc.dims[j] != null) {
            System.out.println("    dim[" + j + "]=" + doc.dims[j]);
          }
        }
      }
    }

    return docs;
  }
  
  protected void sortTies(List<FacetResult> results) {
    for(FacetResult result : results) {
      sortTies(result.labelValues);
    }
  }

  protected void sortTies(LabelAndValue[] labelValues) {
    double lastValue = -1;
    int numInRow = 0;
    int i = 0;
    while(i <= labelValues.length) {
      if (i < labelValues.length && labelValues[i].value.doubleValue() == lastValue) {
        numInRow++;
      } else {
        if (numInRow > 1) {
          Arrays.sort(labelValues, i-numInRow, i,
                      new Comparator<LabelAndValue>() {
                        @Override
                        public int compare(LabelAndValue a, LabelAndValue b) {
                          assert a.value.doubleValue() == b.value.doubleValue();
                          return new BytesRef(a.label).compareTo(new BytesRef(b.label));
                        }
                      });
        }
        numInRow = 1;
        if (i < labelValues.length) {
          lastValue = labelValues[i].value.doubleValue();
        }
      }
      i++;
    }
  }

  protected void sortLabelValues(List<LabelAndValue> labelValues) {
    Collections.sort(labelValues,
                     new Comparator<LabelAndValue>() {
                       @Override
                       public int compare(LabelAndValue a, LabelAndValue b) {
                         if (a.value.doubleValue() > b.value.doubleValue()) {
                           return -1;
                         } else if (a.value.doubleValue() < b.value.doubleValue()) {
                           return 1;
                         } else {
                           return new BytesRef(a.label).compareTo(new BytesRef(b.label));
                         }
                       }
                     });
  }

  protected void sortFacetResults(List<FacetResult> results) {
      Collections.sort(results,
                       new Comparator<FacetResult>() {
                         @Override
                         public int compare(FacetResult a, FacetResult b) {
                           if (a.value.doubleValue() > b.value.doubleValue()) {
                             return -1;
                           } else if (b.value.doubleValue() > a.value.doubleValue()) {
                             return 1;
                           } else {
                             return 0;
                           }
                         }
                       });
  }

  protected void assertFloatValuesEquals(List<FacetResult> a, List<FacetResult> b) {
    assertEquals(a.size(), b.size());
    float lastValue = Float.POSITIVE_INFINITY;
    Map<String,FacetResult> aByDim = new HashMap<>();
    for(int i=0;i<a.size();i++) {
      assertTrue(a.get(i).value.floatValue() <= lastValue);
      lastValue = a.get(i).value.floatValue();
      aByDim.put(a.get(i).dim, a.get(i));
    }
    lastValue = Float.POSITIVE_INFINITY;
    Map<String,FacetResult> bByDim = new HashMap<>();
    for(int i=0;i<b.size();i++) {
      bByDim.put(b.get(i).dim, b.get(i));
      assertTrue(b.get(i).value.floatValue() <= lastValue);
      lastValue = b.get(i).value.floatValue();
    }
    for(String dim : aByDim.keySet()) {
      assertFloatValuesEquals(aByDim.get(dim), bByDim.get(dim));
    }
  }

  protected void assertFloatValuesEquals(FacetResult a, FacetResult b) {
    assertEquals(a.dim, b.dim);
    assertTrue(Arrays.equals(a.path, b.path));
    assertEquals(a.childCount, b.childCount);
    assertEquals(a.value.floatValue(), b.value.floatValue(), a.value.floatValue()/1e5);
    assertEquals(a.labelValues.length, b.labelValues.length);
    for(int i=0;i<a.labelValues.length;i++) {
      assertEquals(a.labelValues[i].label, b.labelValues[i].label);
      assertEquals(a.labelValues[i].value.floatValue(), b.labelValues[i].value.floatValue(), a.labelValues[i].value.floatValue()/1e5);
    }
  }
}

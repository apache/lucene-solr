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
package org.apache.solr.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.PointField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds documents whose specified field has any of the specified values. It's like
 * {@link TermQParserPlugin} but multi-valued, and supports a variety of internal algorithms.
 * <br>Parameters:
 * <br><code>f</code>: The field name (mandatory)
 * <br><code>separator</code>: the separator delimiting the values in the query string, defaulting to a comma.
 * If it's a " " then it splits on any consecutive whitespace.
 * <br><code>method</code>: Any of termsFilter (default), booleanQuery, automaton, docValuesTermsFilter.
 * <p>
 * Note that if no values are specified then the query matches no documents.
 */
public class TermsQParserPlugin extends QParserPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String NAME = "terms";

  /** The separator to use in the underlying suggester */
  public static final String SEPARATOR = "separator";

  /** Choose the internal algorithm */
  private static final String METHOD = "method";

  private static enum Method {
    termsFilter {
      @Override
      Query makeFilter(String fname, BytesRef[] bytesRefs) {
        return new TermInSetQuery(fname, bytesRefs);// constant scores
      }
    },
    booleanQuery {
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (BytesRef byteRef : byteRefs) {
          bq.add(new TermQuery(new Term(fname, byteRef)), BooleanClause.Occur.SHOULD);
        }
        return new ConstantScoreQuery(bq.build());
      }
    },
    automaton {
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        ArrayUtil.timSort(byteRefs); // same sort algo as TermInSetQuery's choice
        Automaton union = Automata.makeStringUnion(Arrays.asList(byteRefs)); // input must be sorted
        return new AutomatonQuery(new Term(fname), union);//constant scores
      }
    },
    docValuesTermsFilter {//on 4x this is FieldCacheTermsFilter but we use the 5x name any way
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        // TODO Further tune this heuristic number
        return (byteRefs.length > 700) ? docValuesTermsFilterTopLevel.makeFilter(fname, byteRefs) : docValuesTermsFilterPerSegment.makeFilter(fname, byteRefs);
      }
    },
    docValuesTermsFilterTopLevel {
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        return disableCacheByDefault(new TopLevelDocValuesTermsQuery(fname, byteRefs));
      }
    },
    docValuesTermsFilterPerSegment {
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        return disableCacheByDefault(new DocValuesTermsQuery(fname, byteRefs));
      }
    };

    private static Query disableCacheByDefault(Query q) {
      final WrappedQuery wrappedQuery = new WrappedQuery(q);
      wrappedQuery.setCache(false);
      return wrappedQuery;
    }

    abstract Query makeFilter(String fname, BytesRef[] byteRefs);
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        String fname = localParams.get(QueryParsing.F);
        if (fname == null || fname.isEmpty()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing field to query");
        }
        FieldType ft = req.getSchema().getFieldType(fname);
        String separator = localParams.get(SEPARATOR, ",");
        String qstr = localParams.get(QueryParsing.V);//never null
        Method method = Method.valueOf(localParams.get(METHOD, Method.termsFilter.name()));
        //TODO pick the default method based on various heuristics from benchmarks
        //TODO pick the default using FieldType.getSetQuery

        //if space then split on all whitespace & trim, otherwise strictly interpret
        final boolean sepIsSpace = separator.equals(" ");
        if (sepIsSpace)
          qstr = qstr.trim();
        if (qstr.length() == 0)
          return new MatchNoDocsQuery();
        final String[] splitVals = sepIsSpace ? qstr.split("\\s+") : qstr.split(Pattern.quote(separator), -1);
        assert splitVals.length > 0;
        
        if (ft.isPointField()) {
          if (localParams.get(METHOD) != null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                String.format(Locale.ROOT, "Method '%s' not supported in TermsQParser when using PointFields", localParams.get(METHOD)));
          }
          return ((PointField)ft).getSetQuery(this, req.getSchema().getField(fname), Arrays.asList(splitVals));
        }

        BytesRef[] bytesRefs = new BytesRef[splitVals.length];
        BytesRefBuilder term = new BytesRefBuilder();
        for (int i = 0; i < splitVals.length; i++) {
          String stringVal = splitVals[i];
          //logic same as TermQParserPlugin
          if (ft != null) {
            ft.readableToIndexed(stringVal, term);
          } else {
            term.copyChars(stringVal);
          }
          bytesRefs[i] = term.toBytesRef();
        }

        return method.makeFilter(fname, bytesRefs);
      }
    };
  }

  private static class TopLevelDocValuesTermsQuery extends DocValuesTermsQuery {
    private final String fieldName;
    private SortedSetDocValues topLevelDocValues;
    private LongBitSet topLevelTermOrdinals;
    private boolean matchesAtLeastOneTerm = false;


    public TopLevelDocValuesTermsQuery(String field, BytesRef... terms) {
      super(field, terms);
      this.fieldName = field;
    }

    public Weight createWeight(IndexSearcher searcher, final ScoreMode scoreMode, float boost) throws IOException {
      if (! (searcher instanceof SolrIndexSearcher)) {
        log.debug("Falling back to DocValuesTermsQuery because searcher [{}] is not the required SolrIndexSearcher", searcher);
        return super.createWeight(searcher, scoreMode, boost);
      }

      topLevelDocValues = DocValues.getSortedSet(((SolrIndexSearcher)searcher).getSlowAtomicReader(), fieldName);
      topLevelTermOrdinals = new LongBitSet(topLevelDocValues.getValueCount());
      PrefixCodedTerms.TermIterator iterator = getTerms().iterator();

      long lastTermOrdFound = 0;
      for(BytesRef term = iterator.next(); term != null; term = iterator.next()) {
        long currentTermOrd = lookupTerm(topLevelDocValues, term, lastTermOrdFound);
        if (currentTermOrd >= 0L) {
          matchesAtLeastOneTerm = true;
          topLevelTermOrdinals.set(currentTermOrd);
          lastTermOrdFound = currentTermOrd;
        }
      }

      return new ConstantScoreWeight(this, boost) {
        public Scorer scorer(LeafReaderContext context) throws IOException {
          if (! matchesAtLeastOneTerm) {
            return null;
          }

          SortedSetDocValues segmentDocValues = DocValues.getSortedSet(context.reader(), fieldName);
          if (segmentDocValues == null) {
            return null;
          }

          final int docBase = context.docBase;
          return new ConstantScoreScorer(this, this.score(), scoreMode, new TwoPhaseIterator(segmentDocValues) {
            public boolean matches() throws IOException {
              topLevelDocValues.advanceExact(docBase + approximation.docID());
              for(long ord = topLevelDocValues.nextOrd(); ord != -1L; ord = topLevelDocValues.nextOrd()) {
                if (topLevelTermOrdinals.get(ord)) {
                  return true;
                }
              }

              return false;
            }

            public float matchCost() {
              return 10.0F;
            }
          });

        }

        public boolean isCacheable(LeafReaderContext ctx) {
          return DocValues.isCacheable(ctx, new String[]{fieldName});
        }
      };
    }

    /*
     * Same binary-search based implementation as SortedSetDocValues.lookupTerm(BytesRef), but with an
     * optimization to narrow the search space where possible by providing a startOrd instead of begining each search
     * at 0.
     */
    private long lookupTerm(SortedSetDocValues docValues, BytesRef key, long startOrd) throws IOException {
      long low = startOrd;
      long high = docValues.getValueCount()-1;

      while (low <= high) {
        long mid = (low + high) >>> 1;
        final BytesRef term = docValues.lookupOrd(mid);
        int cmp = term.compareTo(key);

        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          return mid; // key found
        }
      }

      return -(low + 1);  // key not found.
    }
  }
}

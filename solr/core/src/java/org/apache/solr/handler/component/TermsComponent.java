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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.solr.client.solrj.response.TermsResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.TermsParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.HttpShardHandlerFactory.WhitelistHostChecker;
import org.apache.solr.request.SimpleFacets.CountPair;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.PointMerger;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.BoundedTreeSet;

/**
 * Return TermEnum information, useful for things like auto suggest.
 *
 * <pre class="prettyprint">
 * &lt;searchComponent name="termsComponent" class="solr.TermsComponent"/&gt;
 *
 * &lt;requestHandler name="/terms" class="solr.SearchHandler"&gt;
 *   &lt;lst name="defaults"&gt;
 *     &lt;bool name="terms"&gt;true&lt;/bool&gt;
 *   &lt;/lst&gt;
 *   &lt;arr name="components"&gt;
 *     &lt;str&gt;termsComponent&lt;/str&gt;
 *   &lt;/arr&gt;
 * &lt;/requestHandler&gt;</pre>
 *
 * @see org.apache.solr.common.params.TermsParams
 *      See Lucene's TermEnum class
 *
 */
public class TermsComponent extends SearchComponent {
  public static final int UNLIMITED_MAX_COUNT = -1;
  public static final String COMPONENT_NAME = "terms";

  // This needs to be created here too, because Solr doesn't call init(...) on default components. Bug?
  private WhitelistHostChecker whitelistHostChecker = new WhitelistHostChecker(
      null, 
      !HttpShardHandlerFactory.doGetDisableShardsWhitelist());

  @Override
  public void init( NamedList args )
  {
    super.init(args);
    whitelistHostChecker = new WhitelistHostChecker(
        (String) args.get(HttpShardHandlerFactory.INIT_SHARDS_WHITELIST), 
        !HttpShardHandlerFactory.doGetDisableShardsWhitelist());
  }
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();

    //the terms parameter is also used by json facet API. So we will get errors if we try to parse as boolean
    if (params.get(TermsParams.TERMS, "false").equals("true")) {
      rb.doTerms = true;
    } else {
      return;
    }

    // TODO: temporary... this should go in a different component.
    String shards = params.get(ShardParams.SHARDS);
    if (shards != null) {
      rb.isDistrib = true;
      if (params.get(ShardParams.SHARDS_QT) == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No shards.qt parameter specified");
      }
      List<String> lst = StrUtils.splitSmart(shards, ",", true);
      checkShardsWhitelist(rb, lst);
      rb.shards = lst.toArray(new String[lst.size()]);
    }
  }

  protected void checkShardsWhitelist(final ResponseBuilder rb, final List<String> lst) {
    final List<String> urls = new LinkedList<String>();
    for (final String ele : lst) {
      urls.addAll(StrUtils.splitSmart(ele, '|'));
    }
    
    if (whitelistHostChecker.isWhitelistHostCheckingEnabled() && rb.req.getCore().getCoreContainer().getZkController() == null && !whitelistHostChecker.hasExplicitWhitelist()) {
      throw new SolrException(ErrorCode.FORBIDDEN, "TermsComponent "+HttpShardHandlerFactory.INIT_SHARDS_WHITELIST
          +" not configured but required when using the '"+ShardParams.SHARDS+"' parameter with the TermsComponent."
          +HttpShardHandlerFactory.SET_SOLR_DISABLE_SHARDS_WHITELIST_CLUE);
    } else {
      ClusterState cs = null;
      if (rb.req.getCore().getCoreContainer().getZkController() != null) {
        cs = rb.req.getCore().getCoreContainer().getZkController().getClusterState();
      }
      whitelistHostChecker.checkWhitelist(cs, urls.toString(), urls);
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (!params.get(TermsParams.TERMS, "false").equals("true")) {
      return;
    }

    String[] fields = params.getParams(TermsParams.TERMS_FIELD);

    NamedList<Object> termsResult = new SimpleOrderedMap<>();
    rb.rsp.add("terms", termsResult);

    if (fields == null || fields.length==0) return;

    boolean termStats = params.getBool(TermsParams.TERMS_STATS, false);

    if (termStats) {
      NamedList<Number> stats = new SimpleOrderedMap<>();
      rb.rsp.add("indexstats", stats);
      collectStats(rb.req.getSearcher(), stats);
    }

    String termList = params.get(TermsParams.TERMS_LIST);
    if (termList != null) {
      boolean includeTotalTermFreq = params.getBool(TermsParams.TERMS_TTF, false);
      fetchTerms(rb.req.getSearcher(), fields, termList, includeTotalTermFreq, termsResult);
      return;
    }

    int _limit = params.getInt(TermsParams.TERMS_LIMIT, 10);
    final int limit = _limit < 0 ? Integer.MAX_VALUE : _limit;

    String lowerStr = params.get(TermsParams.TERMS_LOWER);
    String upperStr = params.get(TermsParams.TERMS_UPPER);
    boolean upperIncl = params.getBool(TermsParams.TERMS_UPPER_INCLUSIVE, false);
    boolean lowerIncl = params.getBool(TermsParams.TERMS_LOWER_INCLUSIVE, true);
    boolean sort = !TermsParams.TERMS_SORT_INDEX.equals(
        params.get(TermsParams.TERMS_SORT, TermsParams.TERMS_SORT_COUNT));
    int freqmin = params.getInt(TermsParams.TERMS_MINCOUNT, 1);
    int _freqmax = params.getInt(TermsParams.TERMS_MAXCOUNT, UNLIMITED_MAX_COUNT);
    final int freqmax = _freqmax < 0 ? Integer.MAX_VALUE : _freqmax;

    String prefix = params.get(TermsParams.TERMS_PREFIX_STR);
    String regexp = params.get(TermsParams.TERMS_REGEXP_STR);
    Pattern pattern = regexp != null ? Pattern.compile(regexp, resolveRegexpFlags(params)) : null;

    boolean raw = params.getBool(TermsParams.TERMS_RAW, false);


    final LeafReader indexReader = rb.req.getSearcher().getSlowAtomicReader();

    for (String field : fields) {
      NamedList<Integer> fieldTerms = new NamedList<>();

      Terms terms = indexReader.terms(field);
      if (terms == null) {
        // field does not exist in terms index.  Check points.
        SchemaField sf = rb.req.getSchema().getFieldOrNull(field);
        if (sf != null && sf.getType().isPointField()) {
          if (lowerStr!=null || upperStr!=null || prefix!=null || regexp!=null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                String.format(Locale.ROOT, "The terms component does not support Points-based fields with sorting or with parameters %s,%s,%s,%s ", TermsParams.TERMS_LOWER, TermsParams.TERMS_UPPER, TermsParams.TERMS_PREFIX_STR, TermsParams.TERMS_REGEXP_STR));
          }

          if (sort) {
            PointMerger.ValueIterator valueIterator = new PointMerger.ValueIterator(sf, rb.req.getSearcher().getRawReader().leaves());
            MutableValue mv = valueIterator.getMutableValue();
            BoundedTreeSet<CountPair<MutableValue, Integer>> queue = (sort ? new BoundedTreeSet<>(limit) : null);

            for (; ; ) {
              long count = valueIterator.getNextCount();
              if (count < 0) break;
              if (count < freqmin || count > freqmax) continue;
              if (queue.size() < limit || queue.last().val < count || (queue.last().val == count && queue.last().key.compareTo(mv) < 0)) {
                queue.add(new CountPair<>(mv.duplicate(), (int) count));
              }
            }

            for (CountPair<MutableValue, Integer> item : queue) {
              fieldTerms.add(item.key.toString(), item.val);
            }
            termsResult.add(field, fieldTerms);
            continue;
          }

          if (!sort) {
            /***
            // streaming solution that is deferred until writing the response
            // TODO: we can't use the streaming solution until XML writer supports PushWriter!
            termsResult.add(field, (MapWriter) ew -> {
              PointMerger.ValueIterator valueIterator = new PointMerger.ValueIterator(sf, rb.req.getSearcher().getRawReader().leaves());
              MutableValue mv = valueIterator.getMutableValue();
              int num = 0;
              for(;;) {
                long count = valueIterator.getNextCount();
                if (count < 0) break;
                if (count < freqmin || count > freqmax) continue;
                if (++num > limit) break;
                ew.put(mv.toString(), (int)count); // match the numeric type of terms
              }
            });
             ***/

            PointMerger.ValueIterator valueIterator = new PointMerger.ValueIterator(sf, rb.req.getSearcher().getRawReader().leaves());
            MutableValue mv = valueIterator.getMutableValue();
            int num = 0;
            for(;;) {
              long count = valueIterator.getNextCount();
              if (count < 0) break;
              if (count < freqmin || count > freqmax) continue;
              if (++num > limit) break;
              fieldTerms.add(mv.toString(), (int)count); // match the numeric type of terms
            }

            termsResult.add(field, fieldTerms);
            continue;
          }
        }

        termsResult.add(field, fieldTerms);  // add empty
        continue;
      }
      termsResult.add(field, fieldTerms);

      FieldType ft = raw ? null : rb.req.getSchema().getFieldTypeNoEx(field);
      if (ft==null) ft = new StrField();

      // prefix must currently be text
      BytesRef prefixBytes = prefix==null ? null : new BytesRef(prefix);

      BytesRef upperBytes = null;
      if (upperStr != null) {
        BytesRefBuilder b = new BytesRefBuilder();
        ft.readableToIndexed(upperStr, b);
        upperBytes = b.get();
      }

      BytesRef lowerBytes;
      if (lowerStr == null) {
        // If no lower bound was specified, use the prefix
        lowerBytes = prefixBytes;
      } else {
        lowerBytes = new BytesRef();
        if (raw) {
          // TODO: how to handle binary? perhaps we don't for "raw"... or if the field exists
          // perhaps we detect if the FieldType is non-character and expect hex if so?
          lowerBytes = new BytesRef(lowerStr);
        } else {
          BytesRefBuilder b = new BytesRefBuilder();
          ft.readableToIndexed(lowerStr, b);
          lowerBytes = b.get();
        }
      }


     TermsEnum termsEnum = terms.iterator();
     BytesRef term = null;

      if (lowerBytes != null) {
        if (termsEnum.seekCeil(lowerBytes) == TermsEnum.SeekStatus.END) {
          termsEnum = null;
        } else {
          term = termsEnum.term();
          //Only advance the enum if we are excluding the lower bound and the lower Term actually matches
          if (lowerIncl == false && term.equals(lowerBytes)) {
            term = termsEnum.next();
          }
        }
      } else {
        // position termsEnum on first term
        term = termsEnum.next();
      }

      int i = 0;
      BoundedTreeSet<CountPair<BytesRef, Integer>> queue = (sort ? new BoundedTreeSet<CountPair<BytesRef, Integer>>(limit) : null);
      CharsRefBuilder external = new CharsRefBuilder();
      while (term != null && (i<limit || sort)) {
        boolean externalized = false; // did we fill in "external" yet for this term?

        // stop if the prefix doesn't match
        if (prefixBytes != null && !StringHelper.startsWith(term, prefixBytes)) break;

        if (pattern != null) {
          // indexed text or external text?
          // TODO: support "raw" mode?
          ft.indexedToReadable(term, external);
          externalized = true;
          if (!pattern.matcher(external.get()).matches()) {
            term = termsEnum.next();
            continue;
          }
        }

        if (upperBytes != null) {
          int upperCmp = term.compareTo(upperBytes);
          // if we are past the upper term, or equal to it (when don't include upper) then stop.
          if (upperCmp>0 || (upperCmp==0 && !upperIncl)) break;
        }

        // This is a good term in the range.  Check if mincount/maxcount conditions are satisfied.
        int docFreq = termsEnum.docFreq();
        if (docFreq >= freqmin && docFreq <= freqmax) {
          // add the term to the list
          if (sort) {
            queue.add(new CountPair<>(BytesRef.deepCopyOf(term), docFreq));
          } else {

            // TODO: handle raw somehow
            if (!externalized) {
              ft.indexedToReadable(term, external);
            }
            fieldTerms.add(external.toString(), docFreq);
            i++;
          }
        }

        term = termsEnum.next();
      }

      if (sort) {
        for (CountPair<BytesRef, Integer> item : queue) {
          if (i >= limit) break;
          ft.indexedToReadable(item.key, external);
          fieldTerms.add(external.toString(), item.val);
          i++;
        }
      }
    }
  }

  int resolveRegexpFlags(SolrParams params) {
      String[] flagParams = params.getParams(TermsParams.TERMS_REGEXP_FLAG);
      if (flagParams == null) {
          return 0;
      }
      int flags = 0;
      for (String flagParam : flagParams) {
          try {
            flags |= TermsParams.TermsRegexpFlag.valueOf(flagParam.toUpperCase(Locale.ROOT)).getValue();
          } catch (IllegalArgumentException iae) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown terms regex flag '" + flagParam + "'");
          }
      }
      return flags;
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    if (!rb.doTerms) {
      return ResponseBuilder.STAGE_DONE;
    }

    if (rb.stage == ResponseBuilder.STAGE_EXECUTE_QUERY) {
      TermsHelper th = rb._termsHelper;
      if (th == null) {
        th = rb._termsHelper = new TermsHelper();
        th.init(rb.req.getParams());
      }
      ShardRequest sreq = createShardQuery(rb.req.getParams());
      rb.addRequest(this, sreq);
    }

    if (rb.stage < ResponseBuilder.STAGE_EXECUTE_QUERY) {
      return ResponseBuilder.STAGE_EXECUTE_QUERY;
    } else {
      return ResponseBuilder.STAGE_DONE;
    }
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (!rb.doTerms || (sreq.purpose & ShardRequest.PURPOSE_GET_TERMS) == 0) {
      return;
    }
    TermsHelper th = rb._termsHelper;
    if (th != null) {
      for (ShardResponse srsp : sreq.responses) {
        @SuppressWarnings("unchecked")
        NamedList<NamedList<Object>> terms = (NamedList<NamedList<Object>>) srsp.getSolrResponse().getResponse().get("terms");
        th.parse(terms);


        NamedList<Number> stats = (NamedList<Number>)srsp.getSolrResponse().getResponse().get("indexstats");
        if(stats != null) {
          th.numDocs += stats.get("numDocs").longValue();
          th.stats = true;
        }
      }
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (!rb.doTerms || rb.stage != ResponseBuilder.STAGE_EXECUTE_QUERY) {
      return;
    }

    TermsHelper ti = rb._termsHelper;
    NamedList terms = ti.buildResponse();

    rb.rsp.add("terms", terms);
    if(ti.stats) {
      NamedList<Number> stats = new SimpleOrderedMap();
      stats.add("numDocs", Long.valueOf(ti.numDocs));
      rb.rsp.add("indexstats", stats);
    }
    rb._termsHelper = null;
  }

  private static ShardRequest createShardQuery(SolrParams params) {
    ShardRequest sreq = new ShardRequest();
    sreq.purpose = ShardRequest.PURPOSE_GET_TERMS;

    // base shard request on original parameters
    sreq.params = new ModifiableSolrParams(params);

    // remove any limits for shards, we want them to return all possible
    // responses
    // we want this so we can calculate the correct counts
    // dont sort by count to avoid that unnecessary overhead on the shards
    sreq.params.remove(TermsParams.TERMS_MAXCOUNT);
    sreq.params.remove(TermsParams.TERMS_MINCOUNT);
    sreq.params.set(TermsParams.TERMS_LIMIT, -1);
    sreq.params.set(TermsParams.TERMS_SORT, TermsParams.TERMS_SORT_INDEX);

    return sreq;
  }

  public static class TermsHelper {
    // map to store returned terms
    private HashMap<String, HashMap<String, TermsResponse.Term>> fieldmap;
    private SolrParams params;
    public long numDocs = 0;
    public boolean stats;

    public TermsHelper() {
      fieldmap = new HashMap<>(5);
    }

    public void init(SolrParams params) {
      this.params = params;
      String[] fields = params.getParams(TermsParams.TERMS_FIELD);
      if (fields != null) {
        for (String field : fields) {
          // TODO: not sure 128 is the best starting size
          // It use it because that is what is used for facets
          fieldmap.put(field, new HashMap<String, TermsResponse.Term>(128));
        }
      }
    }

    public void parse(NamedList<NamedList<Object>> terms) {
      // exit if there is no terms
      if (terms == null) {
        return;
      }

      TermsResponse termsResponse = new TermsResponse(terms);

      // loop though each field and add each term+freq to map
      for (String key : fieldmap.keySet()) {
        HashMap<String, TermsResponse.Term> termmap = fieldmap.get(key);
        List<TermsResponse.Term> termlist = termsResponse.getTerms(key);

        // skip this field if there are no terms
        if (termlist == null) {
          continue;
        }

        // loop though each term
        for (TermsResponse.Term tc : termlist) {
          String term = tc.getTerm();
          if (termmap.containsKey(term)) {
            TermsResponse.Term oldtc = termmap.get(term);
            oldtc.addFrequency(tc.getFrequency());
            oldtc.addTotalTermFreq(tc.getTotalTermFreq());
            termmap.put(term, oldtc);
          } else {
            termmap.put(term, tc);
          }
        }
      }
    }

    public NamedList<Object> buildResponse() {
      NamedList<Object> response = new SimpleOrderedMap<>();

      // determine if we are going index or count sort
      boolean sort = !TermsParams.TERMS_SORT_INDEX.equals(params.get(TermsParams.TERMS_SORT,
                                                                     TermsParams.TERMS_SORT_COUNT));
      if(params.get(TermsParams.TERMS_LIST) != null) {
        //Always use lexical sort when TERM_LIST is provided
        sort = false;
      }

      // init minimum frequency
      long freqmin = 1;
      String s = params.get(TermsParams.TERMS_MINCOUNT);
      if (s != null)  freqmin = Long.parseLong(s);

      // init maximum frequency, default to max int
      long freqmax = -1;
      s = params.get(TermsParams.TERMS_MAXCOUNT);
      if (s != null)  freqmax = Long.parseLong(s);
      if (freqmax < 0) {
        freqmax = Long.MAX_VALUE;
      }

      // init limit, default to max int
      long limit = 10;
      s = params.get(TermsParams.TERMS_LIMIT);
      if (s != null)  limit = Long.parseLong(s);
      if (limit < 0) {
        limit = Long.MAX_VALUE;
      }

      // loop though each field we want terms from
      for (String key : fieldmap.keySet()) {
        NamedList<Object> fieldterms = new SimpleOrderedMap<>();
        TermsResponse.Term[] data = null;
        if (sort) {
          data = getCountSorted(fieldmap.get(key));
        } else {
          data = getLexSorted(fieldmap.get(key));
        }

        boolean includeTotalTermFreq = params.getBool(TermsParams.TERMS_TTF, false);
        // loop though each term until we hit limit
        int cnt = 0;
        for (TermsResponse.Term tc : data) {
          if (tc.getFrequency() >= freqmin && tc.getFrequency() <= freqmax) {
            if (includeTotalTermFreq) {
              NamedList<Number> termStats = new SimpleOrderedMap<>();
              termStats.add("df", tc.getFrequency());
              termStats.add("ttf", tc.getTotalTermFreq());
              fieldterms.add(tc.getTerm(), termStats);
            } else {
              fieldterms.add(tc.getTerm(), num(tc.getFrequency()));
            }
            cnt++;
          }

          if (cnt >= limit) {
            break;
          }
        }

        response.add(key, fieldterms);
      }

      return response;
    }

    // use <int> tags for smaller facet counts (better back compatibility)
    private static Number num(long val) {
      if (val < Integer.MAX_VALUE) return (int) val;
      else return val;
    }

    // based on code from facets
    public TermsResponse.Term[] getLexSorted(HashMap<String, TermsResponse.Term> data) {
      TermsResponse.Term[] arr = data.values().toArray(new TermsResponse.Term[data.size()]);

      Arrays.sort(arr, (o1, o2) -> o1.getTerm().compareTo(o2.getTerm()));

      return arr;
    }

    // based on code from facets
    public TermsResponse.Term[] getCountSorted(HashMap<String, TermsResponse.Term> data) {
      TermsResponse.Term[] arr = data.values().toArray(new TermsResponse.Term[data.size()]);

      Arrays.sort(arr, (o1, o2) -> {
        long freq1 = o1.getFrequency();
        long freq2 = o2.getFrequency();

        if (freq2 < freq1) {
          return -1;
        } else if (freq1 < freq2) {
          return 1;
        } else {
          return o1.getTerm().compareTo(o2.getTerm());
        }
      });

      return arr;
    }
  }

  private static void fetchTerms(SolrIndexSearcher indexSearcher, String[] fields, String termList,
      boolean includeTotalTermFreq, NamedList<Object> result) throws IOException {
    List<String> splitTermList = StrUtils.splitSmart(termList, ",", true);
    // Sort the terms once
    String[] splitTerms = splitTermList.toArray(new String[splitTermList.size()]);
    // Not a great idea to trim here, but it was in the original implementation
    for (int i = 0; i < splitTerms.length; i++) {
      splitTerms[i] = splitTerms[i].trim();
    }
    Arrays.sort(splitTerms);

    IndexReaderContext topReaderContext = indexSearcher.getTopReaderContext();
    for (String field : fields) {
      SchemaField sf = indexSearcher.getSchema().getField(field);
      FieldType fieldType = sf.getType();

      if (fieldType.isPointField()) {
        NamedList<Object> termsMap = new SimpleOrderedMap<>();
        for (String term : splitTerms) {
          Query q = fieldType.getFieldQuery(null, sf, term);
          int count = indexSearcher.getDocSet(q).size();
          termsMap.add(term, count);
        }
        result.add(field, termsMap);
        continue;
      }

      // Since splitTerms is already sorted, this array will also be sorted. NOTE: this may not be true, it depends on readableToIndexed.
      Term[] terms = new Term[splitTerms.length];
      for (int i = 0; i < splitTerms.length; i++) {
        terms[i] = new Term(field, fieldType.readableToIndexed(splitTerms[i]));
      }

      TermContext[] termContexts = new TermContext[terms.length];
      collectTermContext(topReaderContext, termContexts, terms);

      NamedList<Object> termsMap = new SimpleOrderedMap<>();
      for (int i = 0; i < terms.length; i++) {
        if (termContexts[i] != null) {
          String outTerm = fieldType.indexedToReadable(terms[i].bytes().utf8ToString());
          int docFreq = termContexts[i].docFreq();
          if (!includeTotalTermFreq) {
            termsMap.add(outTerm, docFreq);
          } else {
            long totalTermFreq = termContexts[i].totalTermFreq();
            NamedList<Long> termStats = new SimpleOrderedMap<>();
            termStats.add("df", (long) docFreq);
            termStats.add("ttf", totalTermFreq);
            termsMap.add(outTerm, termStats);
          }
        }
      }

      result.add(field, termsMap);
    }
  }

  private static void collectTermContext(IndexReaderContext topReaderContext, TermContext[] contextArray,
      Term[] queryTerms) throws IOException {
    TermsEnum termsEnum = null;
    for (LeafReaderContext context : topReaderContext.leaves()) {
      for (int i = 0; i < queryTerms.length; i++) {
        Term term = queryTerms[i];
        final Terms terms = context.reader().terms(term.field());
        if (terms == null) {
          // field does not exist
          continue;
        }
        termsEnum = terms.iterator();
        assert termsEnum != null;

        if (termsEnum == TermsEnum.EMPTY) continue;

        TermContext termContext = contextArray[i];
        if (termsEnum.seekExact(term.bytes())) {
          if (termContext == null) {
            termContext = new TermContext(topReaderContext);
            contextArray[i] = termContext;
          }
          termContext.accumulateStatistics(termsEnum.docFreq(), termsEnum.totalTermFreq());
        }
      }
    }
  }

  private static void collectStats(SolrIndexSearcher searcher, NamedList<Number> stats) {
    int numDocs = searcher.getTopReaderContext().reader().numDocs();
    stats.add("numDocs", Long.valueOf(numDocs));
  }

  @Override
  public String getDescription() {
    return "A Component for working with Term Enumerators";
  }

  @Override
  public Category getCategory() {
    return Category.QUERY;
  }

}

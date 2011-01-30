/**
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
import java.net.URL;
import java.util.*;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SimpleFacets;
import org.apache.lucene.util.OpenBitSet;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.schema.FieldType;
import org.apache.lucene.queryParser.ParseException;

/**
 * TODO!
 *
 * @version $Id$
 * @since solr 1.3
 */
public class  FacetComponent extends SearchComponent
{
  public static final String COMPONENT_NAME = "facet";

  @Override
  public void prepare(ResponseBuilder rb) throws IOException
  {
    if (rb.req.getParams().getBool(FacetParams.FACET,false)) {
      rb.setNeedDocSet( true );
      rb.doFacets = true;
    }
  }

  /**
   * Actually run the query
   * @param rb
   */
  @Override
  public void process(ResponseBuilder rb) throws IOException
  {
    if (rb.doFacets) {
      SolrParams params = rb.req.getParams();
      SimpleFacets f = new SimpleFacets(rb.req,
              rb.getResults().docSet,
              params,
              rb );

      // TODO ???? add this directly to the response, or to the builder?
      rb.rsp.add( "facet_counts", f.getFacetCounts() );
    }
  }

  private static final String commandPrefix = "{!" + CommonParams.TERMS + "=$";

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    if (!rb.doFacets) {
      return ResponseBuilder.STAGE_DONE;
    }

    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      // overlap facet refinement requests (those shards that we need a count for
      // particular facet values from), where possible, with
      // the requests to get fields (because we know that is the
      // only other required phase).
      // We do this in distributedProcess so we can look at all of the
      // requests in the outgoing queue at once.



      for (int shardNum=0; shardNum<rb.shards.length; shardNum++) {
        List<String> refinements = null;

        for (DistribFieldFacet dff : rb._facetInfo.facets.values()) {
          if (!dff.needRefinements) continue;
          List<String> refList = dff._toRefine[shardNum];
          if (refList == null || refList.size()==0) continue;

          String key = dff.getKey();  // reuse the same key that was used for the main facet
          String termsKey = key + "__terms";
          String termsVal = StrUtils.join(refList, ',');

          String facetCommand;
          // add terms into the original facet.field command
          // do it via parameter reference to avoid another layer of encoding.

          String termsKeyEncoded = QueryParsing.encodeLocalParamVal(termsKey);
          if (dff.localParams != null) {
            facetCommand = commandPrefix+termsKeyEncoded + " " + dff.facetStr.substring(2);
          } else {
            facetCommand = commandPrefix+termsKeyEncoded+'}'+dff.field;
          }

          if (refinements == null) {
            refinements = new ArrayList<String>();
          }

          refinements.add(facetCommand);
          refinements.add(termsKey);
          refinements.add(termsVal);
        }

        if (refinements == null) continue;


        String shard = rb.shards[shardNum];
        ShardRequest refine = null;
        boolean newRequest = false;

        // try to find a request that is already going out to that shard.
        // If nshards becomes to great, we way want to move to hashing for better
        // scalability.
        for (ShardRequest sreq : rb.outgoing) {
          if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS)!=0
                  && sreq.shards != null
                  && sreq.shards.length==1
                  && sreq.shards[0].equals(shard))
          {
            refine = sreq;
            break;
          }
        }

        if (refine == null) {
          // we didn't find any other suitable requests going out to that shard, so
          // create one ourselves.
          newRequest = true;
          refine = new ShardRequest();
          refine.shards = new String[]{rb.shards[shardNum]};
          refine.params = new ModifiableSolrParams(rb.req.getParams());
          // don't request any documents
          refine.params.remove(CommonParams.START);
          refine.params.set(CommonParams.ROWS,"0");
        }

        refine.purpose |= ShardRequest.PURPOSE_REFINE_FACETS;
        refine.params.set(FacetParams.FACET, "true");
        refine.params.remove(FacetParams.FACET_FIELD);
        refine.params.remove(FacetParams.FACET_QUERY);

        for (int i=0; i<refinements.size();) {
          String facetCommand=refinements.get(i++);
          String termsKey=refinements.get(i++);
          String termsVal=refinements.get(i++);

          refine.params.add(FacetParams.FACET_FIELD, facetCommand);
          refine.params.set(termsKey, termsVal);
        }

        if (newRequest) {
          rb.addRequest(this, refine);
        }
      }
    }

    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    if (!rb.doFacets) return;

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
        sreq.purpose |= ShardRequest.PURPOSE_GET_FACETS;

        FacetInfo fi = rb._facetInfo;
        if (fi == null) {
          rb._facetInfo = fi = new FacetInfo();
          fi.parse(rb.req.getParams(), rb);
          // should already be true...
          // sreq.params.set(FacetParams.FACET, "true");
        }

        sreq.params.remove(FacetParams.FACET_MINCOUNT);
        sreq.params.remove(FacetParams.FACET_OFFSET);
        sreq.params.remove(FacetParams.FACET_LIMIT);

        for (DistribFieldFacet dff : fi.facets.values()) {
          String paramStart = "f." + dff.field + '.';
          sreq.params.remove(paramStart + FacetParams.FACET_MINCOUNT);
          sreq.params.remove(paramStart + FacetParams.FACET_OFFSET);

          if(dff.sort.equals(FacetParams.FACET_SORT_COUNT) && dff.limit > 0) {
            // set the initial limit higher to increase accuracy
            dff.initialLimit = dff.offset + dff.limit;
            dff.initialLimit = (int)(dff.initialLimit * 1.5) + 10;
          } else {
            dff.initialLimit = dff.limit;
          }

          // Currently this is for testing only and allows overriding of the
          // facet.limit set to the shards
          dff.initialLimit = rb.req.getParams().getInt("facet.shard.limit", dff.initialLimit);

          sreq.params.set(paramStart + FacetParams.FACET_LIMIT,  dff.initialLimit);
      }
    } else {
      // turn off faceting on other requests
      sreq.params.set(FacetParams.FACET, "false");
      // we could optionally remove faceting params
    }
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (!rb.doFacets) return;

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FACETS)!=0) {
      countFacets(rb, sreq);
    } else if ((sreq.purpose & ShardRequest.PURPOSE_REFINE_FACETS)!=0) {
      refineFacets(rb, sreq);
    }
  }




  private void countFacets(ResponseBuilder rb, ShardRequest sreq) {
    FacetInfo fi = rb._facetInfo;

    for (ShardResponse srsp: sreq.responses) {
      int shardNum = rb.getShardNum(srsp.getShard());
      NamedList facet_counts = (NamedList)srsp.getSolrResponse().getResponse().get("facet_counts");

      fi.addExceptions((List)facet_counts.get("exception"));

      // handle facet queries
      NamedList facet_queries = (NamedList)facet_counts.get("facet_queries");
      if (facet_queries != null) {
        for (int i=0; i<facet_queries.size(); i++) {
          String returnedKey = facet_queries.getName(i);
          long count = ((Number)facet_queries.getVal(i)).longValue();
          QueryFacet qf = fi.queryFacets.get(returnedKey);
          qf.count += count;
        }
      }

      // step through each facet.field, adding results from this shard
      NamedList facet_fields = (NamedList)facet_counts.get("facet_fields");
    
      if (facet_fields != null) {
        for (DistribFieldFacet dff : fi.facets.values()) {
          dff.add(shardNum, (NamedList)facet_fields.get(dff.getKey()), dff.initialLimit);
        }
      }
    }


    //
    // This code currently assumes that there will be only a single
    // request ((with responses from all shards) sent out to get facets...
    // otherwise we would need to wait until all facet responses were received.
    //

    for (DistribFieldFacet dff : fi.facets.values()) {
      if (dff.limit <= 0) continue; // no need to check these facets for refinement
      if (dff.minCount <= 1 && dff.sort.equals(FacetParams.FACET_SORT_INDEX)) continue;

      dff._toRefine = new List[rb.shards.length];
      ShardFacetCount[] counts = dff.getCountSorted();
      int ntop = Math.min(counts.length, dff.offset + dff.limit);
      long smallestCount = counts.length == 0 ? 0 : counts[ntop-1].count;

      for (int i=0; i<counts.length; i++) {
        ShardFacetCount sfc = counts[i];
        boolean needRefinement = false;

        if (i<ntop) {
          // automatically flag the top values for refinement
          needRefinement = true;
        } else {
          // calculate the maximum value that this term may have
          // and if it is >= smallestCount, then flag for refinement
          long maxCount = sfc.count;
          for (int shardNum=0; shardNum<rb.shards.length; shardNum++) {
            OpenBitSet obs = dff.counted[shardNum];
            if (!obs.get(sfc.termNum)) {
              // if missing from this shard, add the max it could be
              maxCount += dff.maxPossible(sfc,shardNum);
            }
          }
          if (maxCount >= smallestCount) {
            // TODO: on a tie, we could check the term values
            needRefinement = true;
          }
        }

        if (needRefinement) {
          // add a query for each shard missing the term that needs refinement
          for (int shardNum=0; shardNum<rb.shards.length; shardNum++) {
            OpenBitSet obs = dff.counted[shardNum];
            if (!obs.get(sfc.termNum) && dff.maxPossible(sfc,shardNum)>0) {
              dff.needRefinements = true;
              List<String> lst = dff._toRefine[shardNum];
              if (lst == null) {
                lst = dff._toRefine[shardNum] = new ArrayList<String>();
              }
              lst.add(sfc.name);
            }
          }
        }
      }
    }
  }


  private void refineFacets(ResponseBuilder rb, ShardRequest sreq) {
    FacetInfo fi = rb._facetInfo;

    for (ShardResponse srsp: sreq.responses) {
      // int shardNum = rb.getShardNum(srsp.shard);
      NamedList facet_counts = (NamedList)srsp.getSolrResponse().getResponse().get("facet_counts");
      NamedList facet_fields = (NamedList)facet_counts.get("facet_fields");

      fi.addExceptions((List)facet_counts.get("exception"));

      if (facet_fields == null) continue; // this can happen when there's an exception      

      for (int i=0; i<facet_fields.size(); i++) {
        String key = facet_fields.getName(i);
        DistribFieldFacet dff = fi.facets.get(key);
        if (dff == null) continue;

        NamedList shardCounts = (NamedList)facet_fields.getVal(i);

        for (int j=0; j<shardCounts.size(); j++) {
          String name = shardCounts.getName(j);
          long count = ((Number)shardCounts.getVal(j)).longValue();
          ShardFacetCount sfc = dff.counts.get(name);
          sfc.count += count;
        }
      }
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (!rb.doFacets || rb.stage != ResponseBuilder.STAGE_GET_FIELDS) return;
    // wait until STAGE_GET_FIELDS
    // so that "result" is already stored in the response (for aesthetics)


    FacetInfo fi = rb._facetInfo;

    NamedList facet_counts = new SimpleOrderedMap();

    if (fi.exceptionList != null) {
      facet_counts.add("exception",fi.exceptionList);
    }

    NamedList facet_queries = new SimpleOrderedMap();
    facet_counts.add("facet_queries",facet_queries);
    for (QueryFacet qf : fi.queryFacets.values()) {
      facet_queries.add(qf.getKey(), num(qf.count));
    }

    NamedList facet_fields = new SimpleOrderedMap();
    facet_counts.add("facet_fields", facet_fields);

    for (DistribFieldFacet dff : fi.facets.values()) {
      NamedList fieldCounts = new NamedList(); // order is more important for facets
      facet_fields.add(dff.getKey(), fieldCounts);

      ShardFacetCount[] counts;
      boolean countSorted = dff.sort.equals(FacetParams.FACET_SORT_COUNT);
      if (countSorted) {
        counts = dff.countSorted;
        if (counts == null || dff.needRefinements) {
          counts = dff.getCountSorted();
        }
      } else if (dff.sort.equals(FacetParams.FACET_SORT_INDEX)) {
          counts = dff.getLexSorted();
      } else { // TODO: log error or throw exception?
          counts = dff.getLexSorted();
      }

      int end = dff.limit < 0 ? counts.length : Math.min(dff.offset + dff.limit, counts.length);
      for (int i=dff.offset; i<end; i++) {
        if (counts[i].count < dff.minCount) {
          if (countSorted) break;  // if sorted by count, we can break out of loop early
          else continue;
        }
        fieldCounts.add(counts[i].name, num(counts[i].count));
      }

      if (dff.missing) {
        fieldCounts.add(null, num(dff.missingCount));
      }
    }

    // TODO: facet dates & numbers
    facet_counts.add("facet_dates", new SimpleOrderedMap());
    facet_counts.add("facet_ranges", new SimpleOrderedMap());

    rb.rsp.add("facet_counts", facet_counts);

    rb._facetInfo = null;  // could be big, so release asap
  }


  // use <int> tags for smaller facet counts (better back compatibility)
  private Number num(long val) {
   if (val < Integer.MAX_VALUE) return (int)val;
   else return val;
  }
  private Number num(Long val) {
    if (val.longValue() < Integer.MAX_VALUE) return val.intValue();
    else return val;
  }


  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "Handle Faceting";
  }

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

  @Override
  public URL[] getDocs() {
    return null;
  }

  /**
   * <b>This API is experimental and subject to change</b>
   */
  public static class FacetInfo {
    public LinkedHashMap<String,QueryFacet> queryFacets;
    public LinkedHashMap<String,DistribFieldFacet> facets;
    public List exceptionList;

    void parse(SolrParams params, ResponseBuilder rb) {
      queryFacets = new LinkedHashMap<String,QueryFacet>();
      facets = new LinkedHashMap<String,DistribFieldFacet>();

      String[] facetQs = params.getParams(FacetParams.FACET_QUERY);
      if (facetQs != null) {
        for (String query : facetQs) {
          QueryFacet queryFacet = new QueryFacet(rb, query);
          queryFacets.put(queryFacet.getKey(), queryFacet);
        }
      }

      String[] facetFs = params.getParams(FacetParams.FACET_FIELD);
      if (facetFs != null) {

        for (String field : facetFs) {
          DistribFieldFacet ff = new DistribFieldFacet(rb, field);
          facets.put(ff.getKey(), ff);
        }
      }
    }
        
    public void addExceptions(List exceptions) {
      if (exceptions == null) return;
      if (exceptionList == null) exceptionList = new ArrayList();
      exceptionList.addAll(exceptions);
    }
  }

  /**
   * <b>This API is experimental and subject to change</b>
   */
  public static class FacetBase {
    String facetType;  // facet.field, facet.query, etc (make enum?)
    String facetStr;   // original parameter value of facetStr
    String facetOn;    // the field or query, absent localParams if appropriate
    private String key; // label in the response for the result... "foo" for {!key=foo}myfield
    SolrParams localParams;  // any local params for the facet

    public FacetBase(ResponseBuilder rb, String facetType, String facetStr) {
      this.facetType = facetType;
      this.facetStr = facetStr;
      try {
        this.localParams = QueryParsing.getLocalParams(facetStr, rb.req.getParams());
      } catch (ParseException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
      this.facetOn = facetStr;
      this.key = facetStr;

      if (localParams != null) {
        // remove local params unless it's a query
        if (!facetType.equals(FacetParams.FACET_QUERY)) {
          facetOn = localParams.get(CommonParams.VALUE);
          key = facetOn;
        }

        key = localParams.get(CommonParams.OUTPUT_KEY, key);
      }
    }

    /** returns the key in the response that this facet will be under */
    public String getKey() { return key; }
    public String getType() { return facetType; }
  }

  /**
   * <b>This API is experimental and subject to change</b>
   */
  public static class QueryFacet extends FacetBase {
    public long count;

    public QueryFacet(ResponseBuilder rb, String facetStr) {
      super(rb, FacetParams.FACET_QUERY, facetStr);
    }
  }

  /**
   * <b>This API is experimental and subject to change</b>
   */
  public static class FieldFacet extends FacetBase {
    public String field;     // the field to facet on... "myfield" for {!key=foo}myfield
    public FieldType ftype;
    public int offset;
    public int limit;
    public int minCount;
    public String sort;
    public boolean missing;
    public String prefix;
    public long missingCount;

    public FieldFacet(ResponseBuilder rb, String facetStr) {
      super(rb, FacetParams.FACET_FIELD, facetStr);
      fillParams(rb, rb.req.getParams(), facetOn);
    }

    private void fillParams(ResponseBuilder rb, SolrParams params, String field) {
      this.field = field;
      this.ftype = rb.req.getSchema().getFieldTypeNoEx(this.field);
      this.offset = params.getFieldInt(field, FacetParams.FACET_OFFSET, 0);
      this.limit = params.getFieldInt(field, FacetParams.FACET_LIMIT, 100);
      Integer mincount = params.getFieldInt(field, FacetParams.FACET_MINCOUNT);
      if (mincount==null) {
        Boolean zeros = params.getFieldBool(field, FacetParams.FACET_ZEROS);
        // mincount = (zeros!=null && zeros) ? 0 : 1;
        mincount = (zeros!=null && !zeros) ? 1 : 0;
        // current default is to include zeros.
      }
      this.minCount = mincount;
      this.missing = params.getFieldBool(field, FacetParams.FACET_MISSING, false);
      // default to sorting by count if there is a limit.
      this.sort = params.getFieldParam(field, FacetParams.FACET_SORT, limit>0 ? FacetParams.FACET_SORT_COUNT : FacetParams.FACET_SORT_INDEX);
      if (this.sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
        this.sort = FacetParams.FACET_SORT_COUNT;
      } else if (this.sort.equals(FacetParams.FACET_SORT_INDEX_LEGACY)) {
        this.sort = FacetParams.FACET_SORT_INDEX;
      }
      this.prefix = params.getFieldParam(field,FacetParams.FACET_PREFIX);
    }
  }

  /**
   * <b>This API is experimental and subject to change</b>
   */
  public static class DistribFieldFacet extends FieldFacet {
    public List<String>[] _toRefine; // a List<String> of refinements needed, one for each shard.

    // SchemaField sf;    // currently unneeded

    // the max possible count for a term appearing on no list
    public long missingMaxPossible;
    // the max possible count for a missing term for each shard (indexed by shardNum)
    public long[] missingMax;
    public OpenBitSet[] counted; // a bitset for each shard, keeping track of which terms seen
    public HashMap<String,ShardFacetCount> counts = new HashMap<String,ShardFacetCount>(128);
    public int termNum;

    public int initialLimit;  // how many terms requested in first phase
    public boolean needRefinements;
    public ShardFacetCount[] countSorted;

    DistribFieldFacet(ResponseBuilder rb, String facetStr) {
      super(rb, facetStr);
      // sf = rb.req.getSchema().getField(field);
      missingMax = new long[rb.shards.length];
      counted = new OpenBitSet[rb.shards.length];
    }

    void add(int shardNum, NamedList shardCounts, int numRequested) {
      // shardCounts could be null if there was an exception
      int sz = shardCounts == null ? 0 : shardCounts.size();
      int numReceived = sz;

      OpenBitSet terms = new OpenBitSet(termNum+sz);

      long last = 0;
      for (int i=0; i<sz; i++) {
        String name = shardCounts.getName(i);
        long count = ((Number)shardCounts.getVal(i)).longValue();
        if (name == null) {
          missingCount += count;
          numReceived--;
        } else {
          ShardFacetCount sfc = counts.get(name);
          if (sfc == null) {
            sfc = new ShardFacetCount();
            sfc.name = name;
            sfc.indexed = ftype == null ? sfc.name : ftype.toInternal(sfc.name);
            sfc.termNum = termNum++;
            counts.put(name, sfc);
          }
          sfc.count += count;
          terms.fastSet(sfc.termNum);
          last = count;
        }
      }

      // the largest possible missing term is 0 if we received less
      // than the number requested (provided mincount==0 like it should be for
      // a shard request)
      if (numRequested<0 || numRequested != 0 && numReceived < numRequested) {
        last = 0;
      }

      missingMaxPossible += last;
      missingMax[shardNum] = last;
      counted[shardNum] = terms;
    }

    public ShardFacetCount[] getLexSorted() {
      ShardFacetCount[] arr = counts.values().toArray(new ShardFacetCount[counts.size()]);
      Arrays.sort(arr, new Comparator<ShardFacetCount>() {
        public int compare(ShardFacetCount o1, ShardFacetCount o2) {
          return o1.indexed.compareTo(o2.indexed);
        }
      });
      countSorted = arr;
      return arr;
    }

    public ShardFacetCount[] getCountSorted() {
      ShardFacetCount[] arr = counts.values().toArray(new ShardFacetCount[counts.size()]);
      Arrays.sort(arr, new Comparator<ShardFacetCount>() {
        public int compare(ShardFacetCount o1, ShardFacetCount o2) {
          if (o2.count < o1.count) return -1;
          else if (o1.count < o2.count) return 1;
          return o1.indexed.compareTo(o2.indexed);
        }
      });
      countSorted = arr;
      return arr;
    }

    // returns the max possible value this ShardFacetCount could have for this shard
    // (assumes the shard did not report a count for this value)
    long maxPossible(ShardFacetCount sfc, int shardNum) {
      return missingMax[shardNum];
      // TODO: could store the last term in the shard to tell if this term
      // comes before or after it.  If it comes before, we could subtract 1
    }
  }

  /**
   * <b>This API is experimental and subject to change</b>
   */
  public static class ShardFacetCount {
    public String name;
    public String indexed;  // the indexed form of the name... used for comparisons.
    public long count;
    public int termNum;  // term number starting at 0 (used in bit arrays)

    @Override
    public String toString() {
      return "{term="+name+",termNum="+termNum+",count="+count+"}";
    }
  }
}

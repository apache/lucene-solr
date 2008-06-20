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
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.util.OpenBitSet;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QueryParsing;
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
              params );

      // TODO ???? add this directly to the response, or to the builder?
      rb.rsp.add( "facet_counts", f.getFacetCounts() );
    }
  }


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
        List<String> fqueries = rb._facetInfo._toRefine[shardNum];
        if (fqueries == null || fqueries.size()==0) continue;

        String shard = rb.shards[shardNum];

        ShardRequest refine = null;
        boolean newRequest = false;

        // try to find a request that is already going out to that shard.
        // If nshards becomes to great, we way want to move to hashing for better
        // scalability.
        for (ShardRequest sreq : rb.outgoing) {
          if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS)!=0
                  && sreq.shards != null & sreq.shards.length==1
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
        refine.params.set(FacetParams.FACET,"true");
        refine.params.remove(FacetParams.FACET_FIELD);
        // TODO: perhaps create a more compact facet.terms method?
        refine.params.set(FacetParams.FACET_QUERY, fqueries.toArray(new String[fqueries.size()]));

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

        for (DistribFieldFacet dff : fi.topFacets.values()) {
          String paramStart = "f." + dff.field + '.';
          sreq.params.remove(paramStart + FacetParams.FACET_MINCOUNT);
          sreq.params.remove(paramStart + FacetParams.FACET_OFFSET);

          if(dff.limit > 0) {          
            // set the initial limit higher in increase accuracy
            dff.initialLimit = dff.offset + dff.limit;
            dff.initialLimit = (int)(dff.initialLimit * 1.5) + 10;
          } else {
            dff.initialLimit = dff.limit;
          }

          // TEST: Uncomment the following line when testing to supress over-requesting facets and
          // thus cause more facet refinement queries.
          // if (dff.limit > 0) dff.initialLimit = dff.offset + dff.limit;

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
      int shardNum = rb.getShardNum(srsp.shard);
      NamedList facet_counts = (NamedList)srsp.rsp.getResponse().get("facet_counts");

      // handle facet queries
      NamedList facet_queries = (NamedList)facet_counts.get("facet_queries");
      if (facet_queries != null) {
        for (int i=0; i<facet_queries.size(); i++) {
          String facet_q = (String)facet_queries.getName(i);
          long count = ((Number)facet_queries.getVal(i)).longValue();
          Long prevCount = fi.queryFacets.get(facet_q);
          if (prevCount != null) count += prevCount;
          fi.queryFacets.put(facet_q, count);
        }
      }

      // step through each facet.field, adding results from this shard
      NamedList facet_fields = (NamedList)facet_counts.get("facet_fields");      
      for (DistribFieldFacet dff : fi.topFacets.values()) {
        dff.add(shardNum, (NamedList)facet_fields.get(dff.field), dff.initialLimit);
      }
    }


    //
    // This code currently assumes that there will be only a single
    // request ((with responses from all shards) sent out to get facets...
    // otherwise we would need to wait until all facet responses were received.
    //

    // list of queries to send each shard
    List<String>[] toRefine = new List[rb.shards.length];
    fi._toRefine = toRefine;
    for (int i=0; i<toRefine.length; i++) {
      toRefine[i] = new ArrayList<String>();
    }


    for (DistribFieldFacet dff : fi.topFacets.values()) {
      if (dff.limit <= 0) continue; // no need to check these facets for refinement
      ShardFacetCount[] counts = dff.getSorted();
      int ntop = Math.min(counts.length, dff.offset + dff.limit);
      long smallestCount = counts.length == 0 ? 0 : counts[ntop-1].count;

      for (int i=0; i<counts.length; i++) {
        ShardFacetCount sfc = counts[i];
        String query = null;
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
              if (query==null) query = dff.makeQuery(sfc);
              toRefine[shardNum].add(query);
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
      NamedList facet_counts = (NamedList)srsp.rsp.getResponse().get("facet_counts");
      NamedList facet_queries = (NamedList)facet_counts.get("facet_queries");

      // These are single term queries used to fill in missing counts
      // for facet.field queries
      for (int i=0; i<facet_queries.size(); i++) {
        try {
          
          String facet_q = (String)facet_queries.getName(i);
          long count = ((Number)facet_queries.getVal(i)).longValue();

          // expect {!field f=field}value style params
          SolrParams qparams = QueryParsing.getLocalParams(facet_q,null);
          if (qparams == null) continue;  // not a refinement
          String field = qparams.get(QueryParsing.F);
          String val = qparams.get(QueryParsing.V);

          // Find the right field.facet for this field
          DistribFieldFacet dff = fi.topFacets.get(field);
          if (dff == null) continue;  // maybe this wasn't for facet count refinement

          // Find the right constraint count for this value
          ShardFacetCount sfc = dff.counts.get(val);

          if (sfc == null) {
            continue;
            // Just continue, since other components might have added
            // this facet.query for other purposes.  But if there are charset
            // issues then the values coming back may not match the values sent.
          }

// TODO REMOVE
// System.out.println("Got " + facet_q + " , refining count: " + sfc + " += " + count);

          sfc.count += count;

        } catch (ParseException e) {
          // shouldn't happen, so fail for now rather than covering it up
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
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
    NamedList facet_queries = new SimpleOrderedMap();
    facet_counts.add("facet_queries",facet_queries);
    for (Map.Entry<String,Long> entry : fi.queryFacets.entrySet()) {
      facet_queries.add(entry.getKey(), num(entry.getValue()));
    }

    NamedList facet_fields = new SimpleOrderedMap();
    facet_counts.add("facet_fields", facet_fields);

    for (DistribFieldFacet dff : fi.topFacets.values()) {
      NamedList fieldCounts = new NamedList(); // order is more important for facets
      facet_fields.add(dff.field, fieldCounts);

      ShardFacetCount[] counts = dff.countSorted;
      if (counts == null || dff.needRefinements) {
        counts = dff.getSorted();
      }

      int end = Math.min(dff.offset + dff.limit, counts.length);
      for (int i=dff.offset; i<end; i++) {
        if (counts[i].count < dff.minCount) break;
        fieldCounts.add(counts[i].name, num(counts[i].count));
      }
      
      if (dff.missing) {
        fieldCounts.add(null, num(dff.missingCount));
      }
    }

    // TODO: list facets (sorted by natural order)
    // TODO: facet dates
    facet_counts.add("facet_dates", new SimpleOrderedMap());

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
}



class FacetInfo {
  List<String>[] _toRefine;

  void parse(SolrParams params, ResponseBuilder rb) {
    queryFacets = new LinkedHashMap<String,Long>();
    topFacets = new LinkedHashMap<String,DistribFieldFacet>();
    listFacets = new LinkedHashMap<String,DistribFieldFacet>();

    String[] facetQs = params.getParams(FacetParams.FACET_QUERY);
    if (facetQs != null) {
      for (String query : facetQs) {
        queryFacets.put(query,0L);
      }
    }

    String[] facetFs = params.getParams(FacetParams.FACET_FIELD);
    if (facetFs != null) {
      for (String field : facetFs) {
        DistribFieldFacet ff = new DistribFieldFacet(rb, field);
        ff.fillParams(params, field);
        if (ff.sort) {
          topFacets.put(field, ff);
        } else {
          listFacets.put(field, ff);
        }
      }
    }
  }

  LinkedHashMap<String,Long> queryFacets;
  LinkedHashMap<String,DistribFieldFacet> topFacets;   // field facets that order by constraint count (sort=true)
  LinkedHashMap<String,DistribFieldFacet> listFacets;  // field facets that list values in term order
}


class FieldFacet {
  String field;
  int offset;
  int limit;
  int minCount;
  boolean sort;
  boolean missing;
  String prefix;
  long missingCount;

  void fillParams(SolrParams params, String field) {
    this.field = field;
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
    // default to sorting if there is a limit.
    this.sort = params.getFieldBool(field, FacetParams.FACET_SORT, limit>0);
    this.prefix = params.getFieldParam(field,FacetParams.FACET_PREFIX);
  }
}

class DistribFieldFacet extends FieldFacet {
  SchemaField sf;

  // the max possible count for a term appearing on no list
  long missingMaxPossible;
  // the max possible count for a missing term for each shard (indexed by shardNum)
  long[] missingMax;
  OpenBitSet[] counted; // a bitset for each shard, keeping track of which terms seen
  HashMap<String,ShardFacetCount> counts = new HashMap<String,ShardFacetCount>(128);
  int termNum;
  String queryPrefix;

  int initialLimit;  // how many terms requested in first phase
  boolean needRefinements;  
  ShardFacetCount[] countSorted;

  DistribFieldFacet(ResponseBuilder rb, String field) {
    sf = rb.req.getSchema().getField(field);
    missingMax = new long[rb.shards.length];
    counted = new OpenBitSet[rb.shards.length];
    queryPrefix = "{!field f=" + field + '}';
  }

  void add(int shardNum, NamedList shardCounts, int numRequested) {
    int sz = shardCounts.size();
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
    if (numRequested !=0 && numReceived < numRequested) {
      last = 0;
    }

    missingMaxPossible += last;
    missingMax[shardNum] = last;
    counted[shardNum] = terms;
  }


  ShardFacetCount[] getSorted() {
    ShardFacetCount[] arr = counts.values().toArray(new ShardFacetCount[counts.size()]);
    Arrays.sort(arr, new Comparator<ShardFacetCount>() {
      public int compare(ShardFacetCount o1, ShardFacetCount o2) {
        if (o2.count < o1.count) return -1;
        else if (o1.count < o2.count) return 1;
        // TODO: handle tiebreaks for types other than strings
        return o1.name.compareTo(o2.name);
      }
    });
    countSorted = arr;
    return arr;
  }

  String makeQuery(ShardFacetCount sfc) {
    return queryPrefix + sfc.name;    
  }

  // returns the max possible value this ShardFacetCount could have for this shard
  // (assumes the shard did not report a count for this value)
  long maxPossible(ShardFacetCount sfc, int shardNum) {
    return missingMax[shardNum];
    // TODO: could store the last term in the shard to tell if this term
    // comes before or after it.  If it comes before, we could subtract 1
  }

}


class ShardFacetCount {
  String name;
  long count;
  int termNum;  // term number starting at 0 (used in bit arrays)

  public String toString() {
    return "{term="+name+",termNum="+termNum+",count="+count+"}";
  }
}

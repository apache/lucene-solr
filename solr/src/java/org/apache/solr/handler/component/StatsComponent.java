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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.noggit.CharArr;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.request.UnInvertedField;

/**
 * Stats component calculates simple statistics on numeric field values
 * 
 * @version $Id$
 * @since solr 1.4
 */
public class StatsComponent extends SearchComponent {

  public static final String COMPONENT_NAME = "stats";
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (rb.req.getParams().getBool(StatsParams.STATS,false)) {
      rb.setNeedDocSet( true );
      rb.doStats = true;
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (rb.doStats) {
      SolrParams params = rb.req.getParams();
      SimpleStats s = new SimpleStats(rb.req,
              rb.getResults().docSet,
              params );

      // TODO ???? add this directly to the response, or to the builder?
      rb.rsp.add( "stats", s.getStatsCounts() );
    }
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    if (!rb.doStats) return;

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
        sreq.purpose |= ShardRequest.PURPOSE_GET_STATS;

        StatsInfo si = rb._statsInfo;
        if (si == null) {
          rb._statsInfo = si = new StatsInfo();
          si.parse(rb.req.getParams(), rb);
          // should already be true...
          // sreq.params.set(StatsParams.STATS, "true");
        }
    } else {
      // turn off stats on other requests
      sreq.params.set(StatsParams.STATS, "false");
      // we could optionally remove stats params
    }
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (!rb.doStats || (sreq.purpose & ShardRequest.PURPOSE_GET_STATS) == 0) return;

    StatsInfo si = rb._statsInfo;

    for (ShardResponse srsp : sreq.responses) {
      NamedList stats = (NamedList) srsp.getSolrResponse().getResponse().get("stats");

      NamedList stats_fields = (NamedList) stats.get("stats_fields");
      if (stats_fields != null) {
        for (int i = 0; i < stats_fields.size(); i++) {
          String field = stats_fields.getName(i);
          StatsValues stv = si.statsFields.get(field);
          NamedList shardStv = (NamedList) stats_fields.get(field);
          stv.accumulate(shardStv);
        }
      }
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (!rb.doStats || rb.stage != ResponseBuilder.STAGE_GET_FIELDS) return;
    // wait until STAGE_GET_FIELDS
    // so that "result" is already stored in the response (for aesthetics)

    StatsInfo si = rb._statsInfo;

    NamedList<NamedList<Object>> stats = new SimpleOrderedMap<NamedList<Object>>();
    NamedList<Object> stats_fields = new SimpleOrderedMap<Object>();
    stats.add("stats_fields", stats_fields);
    for (String field : si.statsFields.keySet()) {
      NamedList stv = si.statsFields.get(field).getStatsValues();
      if ((Long) stv.get("count") != 0) {
        stats_fields.add(field, stv);
      } else {
        stats_fields.add(field, null);
      }
    }

    rb.rsp.add("stats", stats);

    rb._statsInfo = null;
  }


  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "Calculate Statistics";
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

}

class StatsInfo {
  Map<String, StatsValues> statsFields;

  void parse(SolrParams params, ResponseBuilder rb) {
    statsFields = new HashMap<String, StatsValues>();

    String[] statsFs = params.getParams(StatsParams.STATS_FIELD);
    if (statsFs != null) {
      for (String field : statsFs) {
        statsFields.put(field,new StatsValues());
      }
    }
  }
}


class SimpleStats {

  /** The main set of documents */
  protected DocSet docs;
  /** Configuration params behavior should be driven by */
  protected SolrParams params;
  /** Searcher to use for all calculations */
  protected SolrIndexSearcher searcher;
  protected SolrQueryRequest req;

  public SimpleStats(SolrQueryRequest req,
                      DocSet docs,
                      SolrParams params) {
    this.req = req;
    this.searcher = req.getSearcher();
    this.docs = docs;
    this.params = params;
  }

  public NamedList<Object> getStatsCounts() throws IOException {
    NamedList<Object> res = new SimpleOrderedMap<Object>();
    res.add("stats_fields", getStatsFields());
    return res;
  }

  public NamedList<Object> getStatsFields() throws IOException {
    NamedList<Object> res = new SimpleOrderedMap<Object>();
    String[] statsFs = params.getParams(StatsParams.STATS_FIELD);
    boolean isShard = params.getBool(ShardParams.IS_SHARD, false);
    if (null != statsFs) {
      for (String f : statsFs) {
        String[] facets = params.getFieldParams(f, StatsParams.STATS_FACET);
        if (facets == null) {
          facets = new String[0]; // make sure it is something...
        }
        SchemaField sf = searcher.getSchema().getField(f);
        FieldType ft = sf.getType();
        NamedList<?> stv;

        // Currently, only UnInvertedField can deal with multi-part trie fields
        String prefix = TrieField.getMainValuePrefix(ft);

        if (sf.multiValued() || ft.multiValuedFieldCache() || prefix!=null) {
          //use UnInvertedField for multivalued fields
          UnInvertedField uif = UnInvertedField.getUnInvertedField(f, searcher);
          stv = uif.getStats(searcher, docs, facets).getStatsValues();
        } else {
          stv = getFieldCacheStats(f, facets);
        }
        if (isShard == true || (Long) stv.get("count") > 0) {
          res.add(f, stv);
        } else {
          res.add(f, null);
        }
      }
    }
    return res;
  }
  
  public NamedList<?> getFieldCacheStats(String fieldName, String[] facet ) {
    FieldType ft = searcher.getSchema().getFieldType(fieldName);

    FieldCache.DocTermsIndex si = null;
    try {
      si = FieldCache.DEFAULT.getTermsIndex(searcher.getIndexReader(), fieldName);
    } 
    catch (IOException e) {
      throw new RuntimeException( "failed to open field cache for: "+fieldName, e );
    }
    FieldFacetStats all = new FieldFacetStats( "all", si, ft, 0 );
    StatsValues allstats = new StatsValues();
    if ( all.nTerms <= 0 || docs.size() <= 0 ) return allstats.getStatsValues();

    // don't worry about faceting if the no documents match...
    int i=0;
    final FieldFacetStats[] finfo = new FieldFacetStats[facet.length];
    for( String f : facet ) {
      ft = searcher.getSchema().getFieldType(f);
      try {
        si = FieldCache.DEFAULT.getTermsIndex(searcher.getIndexReader(), f);
      } 
      catch (IOException e) {
        throw new RuntimeException( "failed to open field cache for: "+f, e );
      }
      finfo[i++] = new FieldFacetStats( f, si, ft, 0 );
    }
    final CharsRef spare = new CharsRef();
    final BytesRef tempBR = new BytesRef();
    DocIterator iter = docs.iterator();
    while (iter.hasNext()) {
      int docID = iter.nextDoc();
      BytesRef raw = all.getTermText(docID, tempBR);
      Double v = null;
      if( raw != null ) {
        v = Double.parseDouble(all.ft.indexedToReadable(raw, spare).toString());
        allstats.accumulate(v);
      }
      else {
        allstats.missing++;
      }
      
      // now check the facets
      for( FieldFacetStats f : finfo ) {
        f.facet(docID, v);
      }
    }
    
    if( finfo.length > 0 ) {
      allstats.facets = new HashMap<String, Map<String,StatsValues>>();
      for( FieldFacetStats f : finfo ) {
        allstats.facets.put( f.name, f.facetStatsValues );
      }
    }
    return allstats.getStatsValues();
  }


}

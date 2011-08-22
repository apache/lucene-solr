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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.ReaderUtil;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.*;
import org.apache.solr.util.SolrPluginUtils;

/**
 * TODO!
 * 
 *
 * @since solr 1.3
 */
public class QueryComponent extends SearchComponent
{
  public static final String COMPONENT_NAME = "query";
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException
  {

    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();
    if (!params.getBool(COMPONENT_NAME, true)) {
      return;
    }
    SolrQueryResponse rsp = rb.rsp;

    // Set field flags    
    ReturnFields returnFields = new ReturnFields( req );
    rsp.setReturnFields( returnFields );
    int flags = 0;
    if (returnFields.wantsScore()) {
      flags |= SolrIndexSearcher.GET_SCORES;
    }
    rb.setFieldFlags( flags );

    String defType = params.get(QueryParsing.DEFTYPE,QParserPlugin.DEFAULT_QTYPE);

    // get it from the response builder to give a different component a chance
    // to set it.
    String queryString = rb.getQueryString();
    if (queryString == null) {
      // this is the normal way it's set.
      queryString = params.get( CommonParams.Q );
      rb.setQueryString(queryString);
    }

    try {
      QParser parser = QParser.getParser(rb.getQueryString(), defType, req);
      Query q = parser.getQuery();
      if (q == null) {
        // normalize a null query to a query that matches nothing
        q = new BooleanQuery();        
      }
      rb.setQuery( q );
      rb.setSortSpec( parser.getSort(true) );
      rb.setQparser(parser);

      String[] fqs = req.getParams().getParams(CommonParams.FQ);
      if (fqs!=null && fqs.length!=0) {
        List<Query> filters = rb.getFilters();
        if (filters==null) {
          filters = new ArrayList<Query>(fqs.length);
        }
        for (String fq : fqs) {
          if (fq != null && fq.trim().length()!=0) {
            QParser fqp = QParser.getParser(fq, null, req);
            filters.add(fqp.getQuery());
          }
        }
        // only set the filters if they are not empty otherwise
        // fq=&someotherParam= will trigger all docs filter for every request 
        // if filter cache is disabled
        if (!filters.isEmpty()) {
          rb.setFilters( filters );
        }
      }
    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

  }


  // TODO: this could go in a different component, or in SearchHandler
  // check if this is a distributed request and set info on the response builder
  void checkDistributed(ResponseBuilder rb) {
    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();

    rb.isDistrib = params.getBool("distrib",false);
    String shards = params.get(ShardParams.SHARDS);

    // for back compat, a shards param with URLs like localhost:8983/solr will mean that this
    // search is distributed.
    boolean hasShardURL = shards != null && shards.indexOf('/') > 0;
    rb.isDistrib = hasShardURL | rb.isDistrib;

    if (rb.isDistrib) {
      // since the cost of grabbing cloud state is still up in the air, we grab it only
      // if we need it.
      CloudState cloudState = null;
      Map<String,Slice> slices = null;
      CoreDescriptor coreDescriptor = req.getCore().getCoreDescriptor();
      CloudDescriptor cloudDescriptor = coreDescriptor.getCloudDescriptor();
      ZkController zkController = coreDescriptor.getCoreContainer().getZkController();


      if (shards != null) {
        List<String> lst = StrUtils.splitSmart(shards, ",", true);
        rb.shards = lst.toArray(new String[lst.size()]);
        rb.slices = new String[rb.shards.length];

        if (zkController != null) {
          // figure out which shards are slices
          for (int i=0; i<rb.shards.length; i++) {
            if (rb.shards[i].indexOf('/') < 0) {
              // this is a logical shard
              rb.slices[i] = rb.shards[i];
              rb.shards[i] = null;
            }
          }
        }
      } else if (zkController != null) {
        // we weren't provided with a list of slices to query, so find the list that will cover the complete index

        cloudState =  zkController.getCloudState();

        // TODO: check "collection" for which collection(s) to search.. but for now, just default
        // to the collection for this core.
        // This can be more efficient... we only record the name, even though we have the
        // shard info we need in the next step of mapping slice->shards
        slices = cloudState.getSlices(cloudDescriptor.getCollectionName());
        rb.slices = slices.keySet().toArray(new String[slices.size()]);
        rb.shards = new String[rb.slices.length];

        /***
         rb.slices = new String[slices.size()];
         for (int i=0; i<rb.slices.length; i++) {
         rb.slices[i] = slices.get(i).getName();
         }
         ***/
      }

      //
      // Map slices to shards
      //
      if (zkController != null) {
        for (int i=0; i<rb.shards.length; i++) {
          if (rb.shards[i] == null) {
            if (cloudState == null) {
              cloudState =  zkController.getCloudState();
              slices = cloudState.getSlices(cloudDescriptor.getCollectionName());
            }
            String sliceName = rb.slices[i];

            Slice slice = slices.get(sliceName);

            if (slice==null) {
              // Treat this the same as "all servers down" for a slice, and let things continue
              // if partial results are acceptable
              rb.shards[i] = "";
              continue;
              // throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no such shard: " + sliceName);
            }

            Map<String, ZkNodeProps> sliceShards = slice.getShards();

            // For now, recreate the | delimited list of equivalent servers
            Set<String> liveNodes = cloudState.getLiveNodes();
            StringBuilder sliceShardsStr = new StringBuilder();
            boolean first = true;
            for (ZkNodeProps nodeProps : sliceShards.values()) {
              if (!liveNodes.contains(nodeProps.get(ZkStateReader.NODE_NAME)))
                continue;
              if (first) {
                first = false;
              } else {
                sliceShardsStr.append('|');
              }
              String url = nodeProps.get("url");
              if (url.startsWith("http://"))
                url = url.substring(7);
              sliceShardsStr.append(url);
            }

            rb.shards[i] = sliceShardsStr.toString();
          }
        }
      }
    }
    String shards_rows = params.get(ShardParams.SHARDS_ROWS);
    if(shards_rows != null) {
      rb.shards_rows = Integer.parseInt(shards_rows);
    }
    String shards_start = params.get(ShardParams.SHARDS_START);
    if(shards_start != null) {
      rb.shards_start = Integer.parseInt(shards_start);
    }
  }

  /**
   * Actually run the query
   */
  @Override
  public void process(ResponseBuilder rb) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;
    SolrParams params = req.getParams();
    if (!params.getBool(COMPONENT_NAME, true)) {
      return;
    }
    SolrIndexSearcher searcher = req.getSearcher();

    if (rb.getQueryCommand().getOffset() < 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'start' parameter cannot be negative");
    }

    // -1 as flag if not set.
    long timeAllowed = (long)params.getInt( CommonParams.TIME_ALLOWED, -1 );

    // Optional: This could also be implemented by the top-level searcher sending
    // a filter that lists the ids... that would be transparent to
    // the request handler, but would be more expensive (and would preserve score
    // too if desired).
    String ids = params.get(ShardParams.IDS);
    if (ids != null) {
      SchemaField idField = req.getSchema().getUniqueKeyField();
      List<String> idArr = StrUtils.splitSmart(ids, ",", true);
      int[] luceneIds = new int[idArr.size()];
      int docs = 0;
      for (int i=0; i<idArr.size(); i++) {
        int id = req.getSearcher().getFirstMatch(
                new Term(idField.getName(), idField.getType().toInternal(idArr.get(i))));
        if (id >= 0)
          luceneIds[docs++] = id;
      }

      DocListAndSet res = new DocListAndSet();
      res.docList = new DocSlice(0, docs, luceneIds, null, docs, 0);
      if (rb.isNeedDocSet()) {
        // TODO: create a cache for this!
        List<Query> queries = new ArrayList<Query>();
        queries.add(rb.getQuery());
        List<Query> filters = rb.getFilters();
        if (filters != null) queries.addAll(filters);
        res.docSet = searcher.getDocSet(queries);
      }
      rb.setResults(res);
      
      ResultContext ctx = new ResultContext();
      ctx.docs = rb.getResults().docList;
      ctx.query = null; // anything?
      rsp.add("response", ctx);
      return;
    }

    SolrIndexSearcher.QueryCommand cmd = rb.getQueryCommand();
    cmd.setTimeAllowed(timeAllowed);
    SolrIndexSearcher.QueryResult result = new SolrIndexSearcher.QueryResult();

    //
    // grouping / field collapsing
    //
    boolean doGroup = params.getBool(GroupParams.GROUP, false);
    if (doGroup) {
      try {
        int maxDocsPercentageToCache = params.getInt(GroupParams.GROUP_CACHE_PERCENTAGE, 0);
        boolean cacheSecondPassSearch = maxDocsPercentageToCache >= 1 && maxDocsPercentageToCache <= 100;
        String[] fields = params.getParams(GroupParams.GROUP_FIELD);
        String[] funcs = params.getParams(GroupParams.GROUP_FUNC);
        String[] queries = params.getParams(GroupParams.GROUP_QUERY);
        String groupSortStr = params.get(GroupParams.GROUP_SORT);
        boolean main = params.getBool(GroupParams.GROUP_MAIN, false);
        boolean truncateGroups = params.getBool(GroupParams.GROUP_TRUNCATE, false);

        String formatStr = params.get(GroupParams.GROUP_FORMAT, Grouping.Format.grouped.name());
        Grouping.Format defaultFormat;
        try {
          defaultFormat = Grouping.Format.valueOf(formatStr);
        } catch (IllegalArgumentException e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format("Illegal %s parameter", GroupParams.GROUP_FORMAT));
        }

        boolean includeTotalGroupCount = params.getBool(GroupParams.GROUP_TOTAL_COUNT, false);
        Grouping.TotalCount defaultTotalCount = includeTotalGroupCount ? Grouping.TotalCount.grouped : Grouping.TotalCount.ungrouped;
        Sort sort = searcher.weightSort(cmd.getSort());
        // groupSort defaults to sort
        Sort groupSort = groupSortStr == null ?  sort : searcher.weightSort(QueryParsing.parseSort(groupSortStr, req));

        int limitDefault = cmd.getLen(); // this is normally from "rows"
        int groupOffsetDefault = params.getInt(GroupParams.GROUP_OFFSET, 0);
        int docsPerGroupDefault = params.getInt(GroupParams.GROUP_LIMIT, 1);

        Grouping grouping = new Grouping(searcher, result, cmd, cacheSecondPassSearch, maxDocsPercentageToCache, main);
        grouping.setSort(sort)
            .setGroupSort(groupSort)
            .setDefaultFormat(defaultFormat)
            .setLimitDefault(limitDefault)
            .setDefaultTotalCount(defaultTotalCount)
            .setDocsPerGroupDefault(docsPerGroupDefault)
            .setGroupOffsetDefault(groupOffsetDefault)
            .setGetGroupedDocSet(truncateGroups);

        if (fields != null) {
          for (String field : fields) {
            grouping.addFieldCommand(field, rb.req);
          }
        }

        if (funcs != null) {
          for (String groupByStr : funcs) {
            grouping.addFunctionCommand(groupByStr, rb.req);
          }
        }

        if (queries != null) {
          for (String groupByStr : queries) {
            grouping.addQueryCommand(groupByStr, rb.req);
          }
        }

        if (rb.doHighlights || rb.isDebug()) {
          // we need a single list of the returned docs
          cmd.setFlags(SolrIndexSearcher.GET_DOCLIST);
        }

        grouping.execute();
        if (grouping.isSignalCacheWarning()) {
          rsp.add(
              "cacheWarning",
              String.format("Cache limit of %d percent relative to maxdoc has exceeded. Please increase cache size or disable caching.", maxDocsPercentageToCache)
          );
        }
        rb.setResult(result);
        rsp.add("grouped", result.groupedResults);

        if (grouping.mainResult != null) {
          ResultContext ctx = new ResultContext();
          ctx.docs = grouping.mainResult;
          ctx.query = null; // TODO? add the query?
          rsp.add("response", ctx);
          rsp.getToLog().add("hits", grouping.mainResult.matches());
        } else if (!grouping.getCommands().isEmpty()) { // Can never be empty since grouping.execute() checks for this.
          rsp.getToLog().add("hits", grouping.getCommands().get(0).getMatches());
        }
        return;
      } catch (ParseException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }

    // normal search result
    searcher.search(result,cmd);
    rb.setResult( result );


    ResultContext ctx = new ResultContext();
    ctx.docs = rb.getResults().docList;
    ctx.query = rb.getQuery();
    rsp.add("response", ctx);
    rsp.getToLog().add("hits", rb.getResults().docList.matches());

    doFieldSortValues(rb, searcher);
    doPrefetch(rb);
  }

  protected void doFieldSortValues(ResponseBuilder rb, SolrIndexSearcher searcher) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;
    final CharsRef spare = new CharsRef();
    // The query cache doesn't currently store sort field values, and SolrIndexSearcher doesn't
    // currently have an option to return sort field values.  Because of this, we
    // take the documents given and re-derive the sort values.
    boolean fsv = req.getParams().getBool(ResponseBuilder.FIELD_SORT_VALUES,false);
    if(fsv){
      Sort sort = searcher.weightSort(rb.getSortSpec().getSort());
      SortField[] sortFields = sort==null ? new SortField[]{SortField.FIELD_SCORE} : sort.getSort();
      NamedList sortVals = new NamedList(); // order is important for the sort fields
      Field field = new StringField("dummy", ""); // a dummy Field
      ReaderContext topReaderContext = searcher.getTopReaderContext();
      AtomicReaderContext[] leaves = ReaderUtil.leaves(topReaderContext);
      AtomicReaderContext currentLeaf = null;
      if (leaves.length==1) {
        // if there is a single segment, use that subReader and avoid looking up each time
        currentLeaf = leaves[0];
        leaves=null;
      }

      for (SortField sortField: sortFields) {
        SortField.Type type = sortField.getType();
        if (type==SortField.Type.SCORE || type==SortField.Type.DOC) continue;

        FieldComparator comparator = null;
        FieldComparator comparators[] = (leaves==null) ? null : new FieldComparator[leaves.length];

        String fieldname = sortField.getField();
        FieldType ft = fieldname==null ? null : req.getSchema().getFieldTypeNoEx(fieldname);

        DocList docList = rb.getResults().docList;
        ArrayList<Object> vals = new ArrayList<Object>(docList.size());
        DocIterator it = rb.getResults().docList.iterator();

        int idx = 0;

        while(it.hasNext()) {
          int doc = it.nextDoc();
          if (leaves != null) {
            idx = ReaderUtil.subIndex(doc, leaves);
            currentLeaf = leaves[idx];
            comparator = comparators[idx];
          }

          if (comparator == null) {
            comparator = sortField.getComparator(1,0);
            comparator = comparator.setNextReader(currentLeaf);
            if (comparators != null)
              comparators[idx] = comparator;
          }

          doc -= currentLeaf.docBase;  // adjust for what segment this is in
          comparator.copy(0, doc);
          Object val = comparator.value(0);

          // Sortable float, double, int, long types all just use a string
          // comparator. For these, we need to put the type into a readable
          // format.  One reason for this is that XML can't represent all
          // string values (or even all unicode code points).
          // indexedToReadable() should be a no-op and should
          // thus be harmless anyway (for all current ways anyway)
          if (val instanceof String) {
            field.setValue((String)val);
            val = ft.toObject(field);
          }

          // Must do the same conversion when sorting by a
          // String field in Lucene, which returns the terms
          // data as BytesRef:
          if (val instanceof BytesRef) {
            field.setValue(((BytesRef)val).utf8ToChars(spare).toString());
            val = ft.toObject(field);
          }

          vals.add(val);
        }

        sortVals.add(fieldname, vals);
      }

      rsp.add("sort_values", sortVals);
    }
  }

  protected void doPrefetch(ResponseBuilder rb) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;
    //pre-fetch returned documents
    if (!req.getParams().getBool(ShardParams.IS_SHARD,false) && rb.getResults().docList != null && rb.getResults().docList.size()<=50) {
      SolrPluginUtils.optimizePreFetchDocs(rb, rb.getResults().docList, rb.getQuery(), req, rsp);
    }
  }

  @Override  
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    if (rb.stage < ResponseBuilder.STAGE_PARSE_QUERY)
      return ResponseBuilder.STAGE_PARSE_QUERY;
    if (rb.stage == ResponseBuilder.STAGE_PARSE_QUERY) {
      createDistributedIdf(rb);
      return ResponseBuilder.STAGE_EXECUTE_QUERY;
    }
    if (rb.stage < ResponseBuilder.STAGE_EXECUTE_QUERY) return ResponseBuilder.STAGE_EXECUTE_QUERY;
    if (rb.stage == ResponseBuilder.STAGE_EXECUTE_QUERY) {
      createMainQuery(rb);
      return ResponseBuilder.STAGE_GET_FIELDS;
    }
    if (rb.stage < ResponseBuilder.STAGE_GET_FIELDS) return ResponseBuilder.STAGE_GET_FIELDS;
    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      createRetrieveDocs(rb);
      return ResponseBuilder.STAGE_DONE;
    }
    return ResponseBuilder.STAGE_DONE;
  }


  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
      mergeIds(rb, sreq);
    }

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      returnFields(rb, sreq);
      return;
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      // We may not have been able to retrieve all the docs due to an
      // index change.  Remove any null documents.
      for (Iterator<SolrDocument> iter = rb._responseDocs.iterator(); iter.hasNext();) {
        if (iter.next() == null) {
          iter.remove();
          rb._responseDocs.setNumFound(rb._responseDocs.getNumFound()-1);
        }        
      }

      rb.rsp.add("response", rb._responseDocs);
    }
  }


  private void createDistributedIdf(ResponseBuilder rb) {
    // TODO
  }

  private void createMainQuery(ResponseBuilder rb) {
    ShardRequest sreq = new ShardRequest();
    sreq.purpose = ShardRequest.PURPOSE_GET_TOP_IDS;

    sreq.params = new ModifiableSolrParams(rb.req.getParams());
    // TODO: base on current params or original params?

    // don't pass through any shards param
    sreq.params.remove(ShardParams.SHARDS);

    // set the start (offset) to 0 for each shard request so we can properly merge
    // results from the start.
    if(rb.shards_start > -1) {
      // if the client set shards.start set this explicitly
      sreq.params.set(CommonParams.START,rb.shards_start);
    } else {
      sreq.params.set(CommonParams.START, "0");
    }
    // TODO: should we even use the SortSpec?  That's obtained from the QParser, and
    // perhaps we shouldn't attempt to parse the query at this level?
    // Alternate Idea: instead of specifying all these things at the upper level,
    // we could just specify that this is a shard request.
    if(rb.shards_rows > -1) {
      // if the client set shards.rows set this explicity
      sreq.params.set(CommonParams.ROWS,rb.shards_rows);
    } else {
      sreq.params.set(CommonParams.ROWS, rb.getSortSpec().getOffset() + rb.getSortSpec().getCount());
    }

    // in this first phase, request only the unique key field
    // and any fields needed for merging.
    sreq.params.set(ResponseBuilder.FIELD_SORT_VALUES,"true");

    if ( (rb.getFieldFlags() & SolrIndexSearcher.GET_SCORES)!=0 || rb.getSortSpec().includesScore()) {
      sreq.params.set(CommonParams.FL, rb.req.getSchema().getUniqueKeyField().getName() + ",score");
    } else {
      sreq.params.set(CommonParams.FL, rb.req.getSchema().getUniqueKeyField().getName());      
    }

    rb.addRequest(this, sreq);
  }





  private void mergeIds(ResponseBuilder rb, ShardRequest sreq) {
      SortSpec ss = rb.getSortSpec();
      Sort sort = ss.getSort();

      SortField[] sortFields = null;
      if(sort != null) sortFields = sort.getSort();
      else {
        sortFields = new SortField[]{SortField.FIELD_SCORE};
      }
 
      SchemaField uniqueKeyField = rb.req.getSchema().getUniqueKeyField();


      // id to shard mapping, to eliminate any accidental dups
      HashMap<Object,String> uniqueDoc = new HashMap<Object,String>();    

      // Merge the docs via a priority queue so we don't have to sort *all* of the
      // documents... we only need to order the top (rows+start)
      ShardFieldSortedHitQueue queue;
      queue = new ShardFieldSortedHitQueue(sortFields, ss.getOffset() + ss.getCount());

      long numFound = 0;
      Float maxScore=null;
      for (ShardResponse srsp : sreq.responses) {
        SolrDocumentList docs = (SolrDocumentList)srsp.getSolrResponse().getResponse().get("response");

        // calculate global maxScore and numDocsFound
        if (docs.getMaxScore() != null) {
          maxScore = maxScore==null ? docs.getMaxScore() : Math.max(maxScore, docs.getMaxScore());
        }
        numFound += docs.getNumFound();

        NamedList sortFieldValues = (NamedList)(srsp.getSolrResponse().getResponse().get("sort_values"));

        // go through every doc in this response, construct a ShardDoc, and
        // put it in the priority queue so it can be ordered.
        for (int i=0; i<docs.size(); i++) {
          SolrDocument doc = docs.get(i);
          Object id = doc.getFieldValue(uniqueKeyField.getName());

          String prevShard = uniqueDoc.put(id, srsp.getShard());
          if (prevShard != null) {
            // duplicate detected
            numFound--;

            // For now, just always use the first encountered since we can't currently
            // remove the previous one added to the priority queue.  If we switched
            // to the Java5 PriorityQueue, this would be easier.
            continue;
            // make which duplicate is used deterministic based on shard
            // if (prevShard.compareTo(srsp.shard) >= 0) {
            //  TODO: remove previous from priority queue
            //  continue;
            // }
          }

          ShardDoc shardDoc = new ShardDoc();
          shardDoc.id = id;
          shardDoc.shard = srsp.getShard();
          shardDoc.orderInShard = i;
          Object scoreObj = doc.getFieldValue("score");
          if (scoreObj != null) {
            if (scoreObj instanceof String) {
              shardDoc.score = Float.parseFloat((String)scoreObj);
            } else {
              shardDoc.score = (Float)scoreObj;
            }
          }

          shardDoc.sortFieldValues = sortFieldValues;

          queue.insertWithOverflow(shardDoc);
        } // end for-each-doc-in-response
      } // end for-each-response


      // The queue now has 0 -> queuesize docs, where queuesize <= start + rows
      // So we want to pop the last documents off the queue to get
      // the docs offset -> queuesize
      int resultSize = queue.size() - ss.getOffset();
      resultSize = Math.max(0, resultSize);  // there may not be any docs in range

      Map<Object,ShardDoc> resultIds = new HashMap<Object,ShardDoc>();
      for (int i=resultSize-1; i>=0; i--) {
        ShardDoc shardDoc = (ShardDoc)queue.pop();
        shardDoc.positionInResponse = i;
        // Need the toString() for correlation with other lists that must
        // be strings (like keys in highlighting, explain, etc)
        resultIds.put(shardDoc.id.toString(), shardDoc);
      }


      SolrDocumentList responseDocs = new SolrDocumentList();
      if (maxScore!=null) responseDocs.setMaxScore(maxScore);
      responseDocs.setNumFound(numFound);
      responseDocs.setStart(ss.getOffset());
      // size appropriately
      for (int i=0; i<resultSize; i++) responseDocs.add(null);

      // save these results in a private area so we can access them
      // again when retrieving stored fields.
      // TODO: use ResponseBuilder (w/ comments) or the request context?
      rb.resultIds = resultIds;
      rb._responseDocs = responseDocs;
  }

  private void createRetrieveDocs(ResponseBuilder rb) {

    // TODO: in a system with nTiers > 2, we could be passed "ids" here
    // unless those requests always go to the final destination shard

    // for each shard, collect the documents for that shard.
    HashMap<String, Collection<ShardDoc>> shardMap = new HashMap<String,Collection<ShardDoc>>();
    for (ShardDoc sdoc : rb.resultIds.values()) {
      Collection<ShardDoc> shardDocs = shardMap.get(sdoc.shard);
      if (shardDocs == null) {
        shardDocs = new ArrayList<ShardDoc>();
        shardMap.put(sdoc.shard, shardDocs);
      }
      shardDocs.add(sdoc);
    }

    SchemaField uniqueField = rb.req.getSchema().getUniqueKeyField();

    // Now create a request for each shard to retrieve the stored fields
    for (Collection<ShardDoc> shardDocs : shardMap.values()) {
      ShardRequest sreq = new ShardRequest();
      sreq.purpose = ShardRequest.PURPOSE_GET_FIELDS;

      sreq.shards = new String[] {shardDocs.iterator().next().shard};

      sreq.params = new ModifiableSolrParams();

      // add original params
      sreq.params.add( rb.req.getParams());

      // no need for a sort, we already have order
      sreq.params.remove(CommonParams.SORT);

      // we already have the field sort values
      sreq.params.remove(ResponseBuilder.FIELD_SORT_VALUES);

      // make sure that the id is returned for correlation.
      String fl = sreq.params.get(CommonParams.FL);
      if (fl != null) {
         fl = fl.trim();
        // currently, "score" is synonymous with "*,score" so
        // don't add "id" if the fl is empty or "score" or it would change the meaning.
         if (fl.length()!=0 && !"score".equals(fl) && !"*".equals(fl)) {
           sreq.params.set(CommonParams.FL, fl+','+uniqueField.getName());
         }
      }      

      ArrayList<String> ids = new ArrayList<String>(shardDocs.size());
      for (ShardDoc shardDoc : shardDocs) {
        // TODO: depending on the type, we may need more tha a simple toString()?
        ids.add(shardDoc.id.toString());
      }
      sreq.params.add(ShardParams.IDS, StrUtils.join(ids, ','));

      rb.addRequest(this, sreq);
    }

  }


  private void returnFields(ResponseBuilder rb, ShardRequest sreq) {
    // Keep in mind that this could also be a shard in a multi-tiered system.
    // TODO: if a multi-tiered system, it seems like some requests
    // could/should bypass middlemen (like retrieving stored fields)
    // TODO: merge fsv to if requested

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      boolean returnScores = (rb.getFieldFlags() & SolrIndexSearcher.GET_SCORES) != 0;

      assert(sreq.responses.size() == 1);
      ShardResponse srsp = sreq.responses.get(0);
      SolrDocumentList docs = (SolrDocumentList)srsp.getSolrResponse().getResponse().get("response");

      String keyFieldName = rb.req.getSchema().getUniqueKeyField().getName();

      for (SolrDocument doc : docs) {
        Object id = doc.getFieldValue(keyFieldName);
        ShardDoc sdoc = rb.resultIds.get(id.toString());
        if (sdoc != null) {
          if (returnScores && sdoc.score != null) {
              doc.setField("score", sdoc.score);
          }
          rb._responseDocs.set(sdoc.positionInResponse, doc);
        }
      }      
    }
  }

  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "query";
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

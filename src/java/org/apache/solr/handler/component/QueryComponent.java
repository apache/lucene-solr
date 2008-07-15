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

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.queryParser.ParseException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.*;
import org.apache.solr.util.SolrPluginUtils;

import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.util.*;
import java.text.Collator;

/**
 * TODO!
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class QueryComponent extends SearchComponent
{
  public static final String COMPONENT_NAME = "query";
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrQueryResponse rsp = rb.rsp;
    SolrParams params = req.getParams();

    // Set field flags
    String fl = params.get(CommonParams.FL);
    int fieldFlags = 0;
    if (fl != null) {
      fieldFlags |= SolrPluginUtils.setReturnFields(fl, rsp);
    }
    rb.setFieldFlags( fieldFlags );

    String defType = params.get(QueryParsing.DEFTYPE);
    defType = defType==null ? OldLuceneQParserPlugin.NAME : defType;

    if (rb.getQueryString() == null) {
      rb.setQueryString( params.get( CommonParams.Q ) );
    }

    try {
      QParser parser = QParser.getParser(rb.getQueryString(), defType, req);
      rb.setQuery( parser.getQuery() );
      rb.setSortSpec( parser.getSort(true) );
      rb.setQparser(parser);

      String[] fqs = req.getParams().getParams(CommonParams.FQ);
      if (fqs!=null && fqs.length!=0) {
        List<Query> filters = rb.getFilters();
        if (filters==null) {
          filters = new ArrayList<Query>();
          rb.setFilters( filters );
        }
        for (String fq : fqs) {
          if (fq != null && fq.trim().length()!=0) {
            QParser fqp = QParser.getParser(fq, null, req);
            filters.add(fqp.getQuery());
          }
        }
      }
    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    // TODO: temporary... this should go in a different component.
    String shards = params.get(ShardParams.SHARDS);
    if (shards != null) {
      List<String> lst = StrUtils.splitSmart(shards, ",", true);
      rb.shards = lst.toArray(new String[lst.size()]);
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
    SolrIndexSearcher searcher = req.getSearcher();
    SolrParams params = req.getParams();

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
        List<Query> queries = new ArrayList<Query>();
        queries.add(rb.getQuery());
        List<Query> filters = rb.getFilters();
        if (filters != null) queries.addAll(filters);
        res.docSet = searcher.getDocSet(queries);
      }
      rb.setResults(res);
      rsp.add("response",rb.getResults().docList);
      return;
    }

    SolrIndexSearcher.QueryCommand cmd = rb.getQueryCommand();
    cmd.setTimeAllowed(timeAllowed);
    SolrIndexSearcher.QueryResult result = new SolrIndexSearcher.QueryResult();
    searcher.search(result,cmd);
    rb.setResult( result );

    rsp.add("response",rb.getResults().docList);
    rsp.getToLog().add("hits", rb.getResults().docList.size());

    boolean fsv = req.getParams().getBool(ResponseBuilder.FIELD_SORT_VALUES,false);
    if(fsv){
      Sort sort = rb.getSortSpec().getSort();
      SortField[] sortFields = sort==null ? new SortField[]{SortField.FIELD_SCORE} : sort.getSort();
      ScoreDoc sd = new ScoreDoc(0,1.0f); // won't work for comparators that look at the score
      NamedList sortVals = new NamedList(); // order is important for the sort fields
      StringFieldable field = new StringFieldable();

      for (SortField sortField: sortFields) {
        int type = sortField.getType();
        if (type==SortField.SCORE || type==SortField.DOC) continue;

        ScoreDocComparator comparator = null;
        IndexReader reader = searcher.getReader();
        String fieldname = sortField.getField();
        FieldType ft = fieldname==null ? null : req.getSchema().getFieldTypeNoEx(fieldname);


        switch (type) {
          case SortField.INT:
            comparator = comparatorInt (reader, fieldname);
            break;
          case SortField.FLOAT:
            comparator = comparatorFloat (reader, fieldname);
            break;
          case SortField.LONG:
            comparator = comparatorLong(reader, fieldname);
            break;
          case SortField.DOUBLE:
            comparator = comparatorDouble(reader, fieldname);
            break;
          case SortField.STRING:
            if (sortField.getLocale() != null) comparator = comparatorStringLocale (reader, fieldname, sortField.getLocale());
            else comparator = comparatorString (reader, fieldname);
            break;
          case SortField.CUSTOM:
            comparator = sortField.getFactory().newComparator (reader, fieldname);
            break;
          default:
            throw new RuntimeException ("unknown field type: "+type);
        }

        DocList docList = rb.getResults().docList;
        ArrayList<Object> vals = new ArrayList<Object>(docList.size());
        DocIterator it = rb.getResults().docList.iterator();
        while(it.hasNext()) {
          sd.doc = it.nextDoc();
          Object val = comparator.sortValue(sd);
          // Sortable float, double, int, long types all just use a string
          // comparator. For these, we need to put the type into a readable
          // format.  One reason for this is that XML can't represent all
          // string values (or even all unicode code points).
          // indexedToReadable() should be a no-op and should
          // thus be harmless anyway (for all current ways anyway)
          if (val instanceof String) {
            field.val = (String)val;
            val = ft.toObject(field);
          }
          vals.add(val);
        }

        sortVals.add(fieldname, vals);
      }

      rsp.add("sort_values", sortVals);
    }

    //pre-fetch returned documents
    if (!req.getParams().getBool(ShardParams.IS_SHARD,false) && rb.getResults().docList != null && rb.getResults().docList.size()<=50) {
      // TODO: this may depend on the highlighter component (or other components?)
      SolrPluginUtils.optimizePreFetchDocs(rb.getResults().docList, rb.getQuery(), req, rsp);
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
      return;
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
    sreq.params.set(CommonParams.START, "0");

    // TODO: should we even use the SortSpec?  That's obtained from the QParser, and
    // perhaps we shouldn't attempt to parse the query at this level?
    // Alternate Idea: instead of specifying all these things at the upper level,
    // we could just specify that this is a shard request.
    sreq.params.set(CommonParams.ROWS, rb.getSortSpec().getOffset() + rb.getSortSpec().getCount());


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
      ShardFieldSortedHitQueue queue = new ShardFieldSortedHitQueue(sortFields, ss.getOffset() + ss.getCount());

      long numFound = 0;
      Float maxScore=null;
      for (ShardResponse srsp : sreq.responses) {
        SolrDocumentList docs = (SolrDocumentList)srsp.rsp.getResponse().get("response");

        // calculate global maxScore and numDocsFound
        if (docs.getMaxScore() != null) {
          maxScore = maxScore==null ? docs.getMaxScore() : Math.max(maxScore, docs.getMaxScore());
        }
        numFound += docs.getNumFound();

        NamedList sortFieldValues = (NamedList)(srsp.rsp.getResponse().get("sort_values"));

        // go through every doc in this response, construct a ShardDoc, and
        // put it in the priority queue so it can be ordered.
        for (int i=0; i<docs.size(); i++) {
          SolrDocument doc = docs.get(i);
          Object id = doc.getFieldValue(uniqueKeyField.getName());

          String prevShard = uniqueDoc.put(id, srsp.shard);
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
          shardDoc.shard = srsp.shard;
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

          queue.insert(shardDoc);
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

      // make sure that the id is returned for correlation
      String fl = sreq.params.get(CommonParams.FL);
      if (fl != null) {
       sreq.params.set(CommonParams.FL, fl+','+uniqueField.getName());
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
      SolrDocumentList docs = (SolrDocumentList)srsp.rsp.getResponse().get("response");

      String keyFieldName = rb.req.getSchema().getUniqueKeyField().getName();

      for (SolrDocument doc : docs) {
        Object id = doc.getFieldValue(keyFieldName);
        ShardDoc sdoc = rb.resultIds.get(id.toString());
        if (returnScores && sdoc.score != null) {
          doc.setField("score", sdoc.score);
        }
        rb._responseDocs.set(sdoc.positionInResponse, doc);
      }      
    }
  }



  /////////////////////////////////////////////
  ///  Comparators copied from Lucene
  /////////////////////////////////////////////

  /**
   * Returns a comparator for sorting hits according to a field containing integers.
   * @param reader  Index to use.
   * @param fieldname  Fieldable containg integer values.
   * @return  Comparator for sorting hits.
   * @throws IOException If an error occurs reading the index.
   */
  static ScoreDocComparator comparatorInt (final IndexReader reader, final String fieldname)
  throws IOException {
    final String field = fieldname.intern();
    final int[] fieldOrder = FieldCache.DEFAULT.getInts (reader, field);
    return new ScoreDocComparator() {

      public final int compare (final ScoreDoc i, final ScoreDoc j) {
        final int fi = fieldOrder[i.doc];
        final int fj = fieldOrder[j.doc];
        if (fi < fj) return -1;
        if (fi > fj) return 1;
        return 0;
      }

      public Comparable sortValue (final ScoreDoc i) {
        return new Integer (fieldOrder[i.doc]);
      }

      public int sortType() {
        return SortField.INT;
      }
    };
  }

  /**
   * Returns a comparator for sorting hits according to a field containing integers.
   * @param reader  Index to use.
   * @param fieldname  Fieldable containg integer values.
   * @return  Comparator for sorting hits.
   * @throws IOException If an error occurs reading the index.
   */
  static ScoreDocComparator comparatorLong (final IndexReader reader, final String fieldname)
  throws IOException {
    final String field = fieldname.intern();
    final long[] fieldOrder = ExtendedFieldCache.EXT_DEFAULT.getLongs (reader, field);
    return new ScoreDocComparator() {

      public final int compare (final ScoreDoc i, final ScoreDoc j) {
        final long li = fieldOrder[i.doc];
        final long lj = fieldOrder[j.doc];
        if (li < lj) return -1;
        if (li > lj) return 1;
        return 0;
      }

      public Comparable sortValue (final ScoreDoc i) {
        return new Long(fieldOrder[i.doc]);
      }

      public int sortType() {
        return SortField.LONG;
      }
    };
  }

  /**
   * Returns a comparator for sorting hits according to a field containing floats.
   * @param reader  Index to use.
   * @param fieldname  Fieldable containg float values.
   * @return  Comparator for sorting hits.
   * @throws IOException If an error occurs reading the index.
   */
  static ScoreDocComparator comparatorFloat (final IndexReader reader, final String fieldname)
  throws IOException {
    final String field = fieldname.intern();
    final float[] fieldOrder = FieldCache.DEFAULT.getFloats (reader, field);
    return new ScoreDocComparator () {

      public final int compare (final ScoreDoc i, final ScoreDoc j) {
        final float fi = fieldOrder[i.doc];
        final float fj = fieldOrder[j.doc];
        if (fi < fj) return -1;
        if (fi > fj) return 1;
        return 0;
      }

      public Comparable sortValue (final ScoreDoc i) {
        return new Float (fieldOrder[i.doc]);
      }

      public int sortType() {
        return SortField.FLOAT;
      }
    };
  }


  /**
   * Returns a comparator for sorting hits according to a field containing doubles.
   * @param reader  Index to use.
   * @param fieldname  Fieldable containg float values.
   * @return  Comparator for sorting hits.
   * @throws IOException If an error occurs reading the index.
   */
  static ScoreDocComparator comparatorDouble(final IndexReader reader, final String fieldname)
  throws IOException {
    final String field = fieldname.intern();
    final double[] fieldOrder = ExtendedFieldCache.EXT_DEFAULT.getDoubles (reader, field);
    return new ScoreDocComparator () {

      public final int compare (final ScoreDoc i, final ScoreDoc j) {
        final double di = fieldOrder[i.doc];
        final double dj = fieldOrder[j.doc];
        if (di < dj) return -1;
        if (di > dj) return 1;
        return 0;
      }

      public Comparable sortValue (final ScoreDoc i) {
        return new Double (fieldOrder[i.doc]);
      }

      public int sortType() {
        return SortField.DOUBLE;
      }
    };
  }

  /**
   * Returns a comparator for sorting hits according to a field containing strings.
   * @param reader  Index to use.
   * @param fieldname  Fieldable containg string values.
   * @return  Comparator for sorting hits.
   * @throws IOException If an error occurs reading the index.
   */
  static ScoreDocComparator comparatorString (final IndexReader reader, final String fieldname)
  throws IOException {
    final String field = fieldname.intern();
    final FieldCache.StringIndex index = FieldCache.DEFAULT.getStringIndex (reader, field);
    return new ScoreDocComparator () {

      public final int compare (final ScoreDoc i, final ScoreDoc j) {
        final int fi = index.order[i.doc];
        final int fj = index.order[j.doc];
        if (fi < fj) return -1;
        if (fi > fj) return 1;
        return 0;
      }

      public Comparable sortValue (final ScoreDoc i) {
        return index.lookup[index.order[i.doc]];
      }

      public int sortType() {
        return SortField.STRING;
      }
    };
  }

  /**
   * Returns a comparator for sorting hits according to a field containing strings.
   * @param reader  Index to use.
   * @param fieldname  Fieldable containg string values.
   * @return  Comparator for sorting hits.
   * @throws IOException If an error occurs reading the index.
   */
  static ScoreDocComparator comparatorStringLocale (final IndexReader reader, final String fieldname, final Locale locale)
  throws IOException {
    final Collator collator = Collator.getInstance (locale);
    final String field = fieldname.intern();
    final String[] index = FieldCache.DEFAULT.getStrings (reader, field);
    return new ScoreDocComparator() {

    	public final int compare(final ScoreDoc i, final ScoreDoc j) {
			String is = index[i.doc];
			String js = index[j.doc];
			if (is == js) {
				return 0;
			} else if (is == null) {
				return -1;
			} else if (js == null) {
				return 1;
			} else {
				return collator.compare(is, js);
			}
		}

      public Comparable sortValue (final ScoreDoc i) {
        return index[i.doc];
      }

      public int sortType() {
        return SortField.STRING;
      }
    };
  }

  static class StringFieldable implements Fieldable {
    public String val;

    public void setBoost(float boost) {
    }

    public float getBoost() {
      return 0;
    }

    public String name() {
      return null;
    }

    public String stringValue() {
      return val;
    }

    public Reader readerValue() {
      return null;
    }

    public byte[] binaryValue() {
      return new byte[0];
    }

    public TokenStream tokenStreamValue() {
      return null;
    }

    public boolean isStored() {
      return true;
    }

    public boolean isIndexed() {
      return true;
    }

    public boolean isTokenized() {
      return true;
    }

    public boolean isCompressed() {
      return false;
    }

    public boolean isTermVectorStored() {
      return false;
    }

    public boolean isStoreOffsetWithTermVector() {
      return false;
    }

    public boolean isStorePositionWithTermVector() {
      return false;
    }

    public boolean isBinary() {
      return false;
    }

    public boolean getOmitNorms() {
      return false;
    }

    public void setOmitNorms(boolean omitNorms) {
    }

    public boolean isLazy() {
      return false;
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

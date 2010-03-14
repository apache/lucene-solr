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

package org.apache.solr.tst;

import org.apache.lucene.search.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;

import java.util.*;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URL;

import org.apache.lucene.util.OpenBitSet;
import org.apache.solr.search.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * @version $Id$
 * 
 * @deprecated Test against the real request handlers instead.
 */
@Deprecated
public class TestRequestHandler implements SolrRequestHandler {
  private static Logger log = LoggerFactory.getLogger(SolrIndexSearcher.class);

  public void init(NamedList args) {
    SolrCore.log.info( "Unused request handler arguments:" + args);
  }

  // use test instead of assert since asserts may be turned off
  public void test(boolean condition) {
    try {
      if (!condition) {
        throw new RuntimeException("test requestHandler: assertion failed!");
      }
    } catch (RuntimeException e) {
      SolrException.log(log,e);
      throw(e);
    }
  }


  private long numRequests;
  private long numErrors;

  private final Pattern splitList=Pattern.compile(",| ");


  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    numRequests++;

    // TODO: test if lucene will accept an escaped ';', otherwise
    // we need to un-escape them before we pass to QueryParser
    try {
      String sreq = req.getQueryString();
      if (sreq==null) throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Missing queryString");
      List<String> commands = StrUtils.splitSmart(sreq,';');

      String qs = commands.size() >= 1 ? commands.get(0) : "";
      Query query = QueryParsing.parseQuery(qs, req.getSchema());

      // find fieldnames to return (fieldlist)
      String fl = req.getParam("fl");
      int flags=0;
      if (fl != null) {
        // TODO - this could become more efficient if widely used.
        // TODO - should field order be maintained?
        String[] flst = splitList.split(fl,0);
        if (flst.length > 0 && !(flst.length==1 && flst[0].length()==0)) {
          Set<String> set = new HashSet<String>();
          for (String fname : flst) {
            if ("score".equals(fname)) flags |= SolrIndexSearcher.GET_SCORES;
            set.add(fname);
          }
          rsp.setReturnFields(set);
        }
      }


      // If the first non-query, non-filter command is a simple sort on an indexed field, then
      // we can use the Lucene sort ability.
      Sort sort = null;
      if (commands.size() >= 2) {
        sort = QueryParsing.parseSort(commands.get(1), req.getSchema());
      }

      SolrIndexSearcher searcher = req.getSearcher();

      /***
      Object o = searcher.cacheLookup("dfllNode", query);
      if (o == null) {
        searcher.cacheInsert("dfllNode",query,"Hello Bob");
      } else {
        System.out.println("User Cache Hit On " + o);
      }
      ***/

      int start=req.getStart();
      int limit=req.getLimit();

      Query filterQuery=null;
      DocSet filter=null;
      Filter lfilter=null;

      DocList results = req.getSearcher().getDocList(query, null, sort, req.getStart(), req.getLimit(), flags);
      rsp.add(null, results);


      if (qs.startsWith("values")) {
        rsp.add("testname1","testval1");

        rsp.add("testarr1",new String[]{"my val 1","my val 2"});

        NamedList nl = new NamedList();
        nl.add("myInt", 333);
        nl.add("myNullVal", null);
        nl.add("myFloat",1.414213562f);
        nl.add("myDouble", 1e100d);
        nl.add("myBool", false);
        nl.add("myLong",999999999999L);

        Document doc = new Document();
        doc.add(new Field("id","55",Field.Store.YES, Field.Index.NOT_ANALYZED));
        nl.add("myDoc",doc);

        nl.add("myResult",results);
        nl.add("myStr","&wow! test escaping: a&b<c&");
        nl.add(null, "this value had a null name...");
        nl.add("myIntArray", new Integer[] { 100, 5, -10, 42 });
        nl.add("epoch", new Date(0));
        nl.add("currDate", new Date(System.currentTimeMillis()));
        rsp.add("myNamedList", nl);
      } else if (qs.startsWith("fields")) {
        NamedList nl = new NamedList();
        Collection flst;
        flst = searcher.getReader().getFieldNames(IndexReader.FieldOption.INDEXED);
        nl.add("indexed",flst);
        flst = searcher.getReader().getFieldNames(IndexReader.FieldOption.UNINDEXED);
        nl.add("unindexed",flst);
        rsp.add("fields", nl);
      }

      test(results.size() <= limit);
      test(results.size() <= results.matches());
      // System.out.println("limit="+limit+" results.size()="+results.size()+" matches="+results.matches());
      test((start==0 && limit>=results.matches()) ? results.size()==results.matches() : true );

      //
      // test against hits
      //
      TopFieldDocs hits = searcher.search(query, lfilter, 1000, sort);
      test(hits.totalHits == results.matches());


      DocList rrr2 = results.subset(start,limit);
      test(rrr2 == results);

      DocIterator iter=results.iterator();


      /***
      for (int i=0; i<hits.length(); i++) {
        System.out.println("doc="+hits.id(i) + " score="+hits.score(i));
      }
      ***/

      for (int i=0; i<results.size(); i++) {
        test( iter.nextDoc() == hits.scoreDocs[i].doc);

        // Document doesn't implement equals()
        // test( searcher.document(i).equals(hits.doc(i)));
      }


      DocList results2 = req.getSearcher().getDocList(query,query,sort,start,limit);
      test(results2.size()==results.size() && results2.matches()==results.matches());
      DocList results3 = req.getSearcher().getDocList(query,query,null,start,limit);
      test(results3.size()==results.size() && results3.matches()==results.matches());

      //
      // getting both the list and set
      //
      DocListAndSet both = searcher.getDocListAndSet(query,filter,sort,start, limit);
      test( both.docList.equals(results) );
      test( both.docList.matches() == both.docSet.size() );
      test( (start==0 && both.docSet.size() <= limit) ? both.docSet.equals(both.docList) : true);

      // use the result set as a filter itself...
      DocListAndSet both2 = searcher.getDocListAndSet(query,both.docSet,sort,start, limit);
      test( both2.docList.equals(both.docList) );
      test( both2.docSet.equals(both.docSet) );

      // use the query as a filter itself...
      DocListAndSet both3 = searcher.getDocListAndSet(query,query,sort,start, limit);
      test( both3.docList.equals(both.docList) );
      test( both3.docSet.equals(both.docSet) );

      OpenBitSet bits = both.docSet.getBits();
      OpenBitSet neg = ((OpenBitSet)bits.clone());
      neg.flip(0, bits.capacity());

      // use the negative as a filter (should result in 0 matches)
      // todo - fix if filter is not null
      both2 = searcher.getDocListAndSet(query,new BitDocSet(neg),sort, start, limit);
      test( both2.docList.size() == 0 );
      test( both2.docList.matches() == 0 );
      test( both2.docSet.size() == 0 );

      DocSet allResults=searcher.getDocSet(query,filter);
      test ( allResults.equals(both.docSet) );

      if (filter != null) {
        DocSet res=searcher.getDocSet(query);
        test( res.size() >= results.size() );
        test( res.intersection(filter).equals(both.docSet));

        test( res.intersectionSize(filter) == both.docSet.size() );
        if (filterQuery != null) {
          test( searcher.numDocs(filterQuery,res) == both.docSet.size() );
        }
      }


    } catch (Exception e) {
      rsp.setException(e);
      numErrors++;
      return;
    }
  }


    //////////////////////// SolrInfoMBeans methods //////////////////////


  public String getName() {
    return TestRequestHandler.class.getName();
  }

  public String getVersion() {
    return SolrCore.version;
  }

  public String getDescription() {
    return "A test handler that runs some sanity checks on results";
  }

  public Category getCategory() {
    return Category.QUERYHANDLER;
  }

  public String getSourceId() {
    return "$Id$";
  }

  public String getSource() {
    return "$URL$";
  }

  public URL[] getDocs() {
    return null;
  }

  public NamedList getStatistics() {
    NamedList lst = new NamedList();
    lst.add("requests", numRequests);
    lst.add("errors", numErrors);
    return lst;
  }



}



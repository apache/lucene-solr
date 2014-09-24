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

package org.apache.solr.update;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.HashBasedRouter;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrIndexSplitter {
  public static Logger log = LoggerFactory.getLogger(SolrIndexSplitter.class);

  SolrIndexSearcher searcher;
  SchemaField field;
  List<DocRouter.Range> ranges;
  DocRouter.Range[] rangesArr; // same as ranges list, but an array for extra speed in inner loops
  List<String> paths;
  List<SolrCore> cores;
  DocRouter router;
  HashBasedRouter hashRouter;
  int numPieces;
  int currPartition = 0;
  String routeFieldName;
  String splitKey;

  public SolrIndexSplitter(SplitIndexCommand cmd) {
    searcher = cmd.getReq().getSearcher();
    ranges = cmd.ranges;
    paths = cmd.paths;
    cores = cmd.cores;
    router = cmd.router;
    hashRouter = router instanceof HashBasedRouter ? (HashBasedRouter)router : null;

    if (ranges == null) {
      numPieces =  paths != null ? paths.size() : cores.size();
    } else  {
      numPieces = ranges.size();
      rangesArr = ranges.toArray(new DocRouter.Range[ranges.size()]);
    }
    routeFieldName = cmd.routeFieldName;
    if (routeFieldName == null) {
      field = searcher.getSchema().getUniqueKeyField();
    } else  {
      field = searcher.getSchema().getField(routeFieldName);
    }
    if (cmd.splitKey != null) {
      splitKey = getRouteKey(cmd.splitKey);
    }
  }

  public void split() throws IOException {

    List<LeafReaderContext> leaves = searcher.getRawReader().leaves();
    List<FixedBitSet[]> segmentDocSets = new ArrayList<>(leaves.size());

    log.info("SolrIndexSplitter: partitions=" + numPieces + " segments="+leaves.size());

    for (LeafReaderContext readerContext : leaves) {
      assert readerContext.ordInParent == segmentDocSets.size();  // make sure we're going in order
      FixedBitSet[] docSets = split(readerContext);
      segmentDocSets.add( docSets );
    }


    // would it be more efficient to write segment-at-a-time to each new index?
    // - need to worry about number of open descriptors
    // - need to worry about if IW.addIndexes does a sync or not...
    // - would be more efficient on the read side, but prob less efficient merging

    for (int partitionNumber=0; partitionNumber<numPieces; partitionNumber++) {
      log.info("SolrIndexSplitter: partition #" + partitionNumber + " partitionCount=" + numPieces + (ranges != null ? " range=" + ranges.get(partitionNumber) : ""));

      boolean success = false;

      RefCounted<IndexWriter> iwRef = null;
      IndexWriter iw = null;
      if (cores != null) {
        SolrCore subCore = cores.get(partitionNumber);
        iwRef = subCore.getUpdateHandler().getSolrCoreState().getIndexWriter(subCore);
        iw = iwRef.get();
      } else {
        SolrCore core = searcher.getCore();
        String path = paths.get(partitionNumber);
        iw = SolrIndexWriter.create("SplittingIndexWriter"+partitionNumber + (ranges != null ? " " + ranges.get(partitionNumber) : ""), path,
                                    core.getDirectoryFactory(), true, core.getLatestSchema(),
                                    core.getSolrConfig().indexConfig, core.getDeletionPolicy(), core.getCodec());
      }

      try {
        // This removes deletions but optimize might still be needed because sub-shards will have the same number of segments as the parent shard.
        for (int segmentNumber = 0; segmentNumber<leaves.size(); segmentNumber++) {
          log.info("SolrIndexSplitter: partition #" + partitionNumber + " partitionCount=" + numPieces + (ranges != null ? " range=" + ranges.get(partitionNumber) : "") + " segment #"+segmentNumber + " segmentCount=" + leaves.size());
          IndexReader subReader = new LiveDocsReader( leaves.get(segmentNumber), segmentDocSets.get(segmentNumber)[partitionNumber] );
          iw.addIndexes(subReader);
        }
        success = true;
      } finally {
        if (iwRef != null) {
          iwRef.decref();
        } else {
          if (success) {
            iw.close();
          } else {
            IOUtils.closeWhileHandlingException(iw);
          }
        }
      }

    }

  }



  FixedBitSet[] split(LeafReaderContext readerContext) throws IOException {
    LeafReader reader = readerContext.reader();
    FixedBitSet[] docSets = new FixedBitSet[numPieces];
    for (int i=0; i<docSets.length; i++) {
      docSets[i] = new FixedBitSet(reader.maxDoc());
    }
    Bits liveDocs = reader.getLiveDocs();

    Fields fields = reader.fields();
    Terms terms = fields==null ? null : fields.terms(field.getName());
    TermsEnum termsEnum = terms==null ? null : terms.iterator(null);
    if (termsEnum == null) return docSets;

    BytesRef term = null;
    DocsEnum docsEnum = null;

    CharsRefBuilder idRef = new CharsRefBuilder();
    for (;;) {
      term = termsEnum.next();
      if (term == null) break;

      // figure out the hash for the term

      // FUTURE: if conversion to strings costs too much, we could
      // specialize and use the hash function that can work over bytes.
      field.getType().indexedToReadable(term, idRef);
      String idString = idRef.toString();

      if (splitKey != null) {
        // todo have composite routers support these kind of things instead
        String part1 = getRouteKey(idString);
        if (part1 == null)
          continue;
        if (!splitKey.equals(part1))  {
          continue;
        }
      }

      int hash = 0;
      if (hashRouter != null) {
        hash = hashRouter.sliceHash(idString, null, null, null);
      }

      docsEnum = termsEnum.docs(liveDocs, docsEnum, DocsEnum.FLAG_NONE);
      for (;;) {
        int doc = docsEnum.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) break;
        if (ranges == null) {
          docSets[currPartition].set(doc);
          currPartition = (currPartition + 1) % numPieces;
        } else  {
          for (int i=0; i<rangesArr.length; i++) {      // inner-loop: use array here for extra speed.
            if (rangesArr[i].includes(hash)) {
              docSets[i].set(doc);
            }
          }
        }
      }
    }

    return docSets;
  }

  public static String getRouteKey(String idString) {
    int idx = idString.indexOf(CompositeIdRouter.SEPARATOR);
    if (idx <= 0) return null;
    String part1 = idString.substring(0, idx);
    int commaIdx = part1.indexOf(CompositeIdRouter.bitsSeparator);
    if (commaIdx > 0) {
      if (commaIdx + 1 < part1.length())  {
        char ch = part1.charAt(commaIdx + 1);
        if (ch >= '0' && ch <= '9') {
          part1 = part1.substring(0, commaIdx);
        }
      }
    }
    return part1;
  }


  // change livedocs on the reader to delete those docs we don't want
  static class LiveDocsReader extends FilterLeafReader {
    final FixedBitSet liveDocs;
    final int numDocs;

    public LiveDocsReader(LeafReaderContext context, FixedBitSet liveDocs) throws IOException {
      super(context.reader());
      this.liveDocs = liveDocs;
      this.numDocs = liveDocs.cardinality();
    }

    @Override
    public int numDocs() {
      return numDocs;
    }

    @Override
    public Bits getLiveDocs() {
      return liveDocs;
    }
  }

}

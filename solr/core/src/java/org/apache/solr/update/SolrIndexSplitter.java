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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.OpenBitSet;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.HashBasedRouter;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    List<AtomicReaderContext> leaves = searcher.getTopReaderContext().leaves();
    List<OpenBitSet[]> segmentDocSets = new ArrayList<OpenBitSet[]>(leaves.size());

    log.info("SolrIndexSplitter: partitions=" + numPieces + " segments="+leaves.size());

    for (AtomicReaderContext readerContext : leaves) {
      assert readerContext.ordInParent == segmentDocSets.size();  // make sure we're going in order
      OpenBitSet[] docSets = split(readerContext);
      segmentDocSets.add( docSets );
    }


    // would it be more efficient to write segment-at-a-time to each new index?
    // - need to worry about number of open descriptors
    // - need to worry about if IW.addIndexes does a sync or not...
    // - would be more efficient on the read side, but prob less efficient merging

    IndexReader[] subReaders = new IndexReader[leaves.size()];
    for (int partitionNumber=0; partitionNumber<numPieces; partitionNumber++) {
      log.info("SolrIndexSplitter: partition #" + partitionNumber + (ranges != null ? " range=" + ranges.get(partitionNumber) : ""));

      for (int segmentNumber = 0; segmentNumber<subReaders.length; segmentNumber++) {
        subReaders[segmentNumber] = new LiveDocsReader( leaves.get(segmentNumber), segmentDocSets.get(segmentNumber)[partitionNumber] );
      }

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
        // This merges the subreaders and will thus remove deletions (i.e. no optimize needed)
        iw.addIndexes(subReaders);
        success = true;
      } finally {
        if (iwRef != null) {
          iwRef.decref();
        } else {
          if (success) {
            IOUtils.close(iw);
          } else {
            IOUtils.closeWhileHandlingException(iw);
          }
        }
      }

    }

  }



  OpenBitSet[] split(AtomicReaderContext readerContext) throws IOException {
    AtomicReader reader = readerContext.reader();
    OpenBitSet[] docSets = new OpenBitSet[numPieces];
    for (int i=0; i<docSets.length; i++) {
      docSets[i] = new OpenBitSet(reader.maxDoc());
    }
    Bits liveDocs = reader.getLiveDocs();

    Fields fields = reader.fields();
    Terms terms = fields==null ? null : fields.terms(field.getName());
    TermsEnum termsEnum = terms==null ? null : terms.iterator(null);
    if (termsEnum == null) return docSets;

    BytesRef term = null;
    DocsEnum docsEnum = null;

    CharsRef idRef = new CharsRef(100);
    for (;;) {
      term = termsEnum.next();
      if (term == null) break;

      // figure out the hash for the term

      // FUTURE: if conversion to strings costs too much, we could
      // specialize and use the hash function that can work over bytes.
      idRef = field.getType().indexedToReadable(term, idRef);
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
        if (doc == DocsEnum.NO_MORE_DOCS) break;
        if (ranges == null) {
          docSets[currPartition].fastSet(doc);
          currPartition = (currPartition + 1) % numPieces;
        } else  {
          for (int i=0; i<rangesArr.length; i++) {      // inner-loop: use array here for extra speed.
            if (rangesArr[i].includes(hash)) {
              docSets[i].fastSet(doc);
            }
          }
        }
      }
    }

    return docSets;
  }

  private String getRouteKey(String idString) {
    int idx = idString.indexOf(CompositeIdRouter.separator);
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
  static class LiveDocsReader extends FilterAtomicReader {
    final OpenBitSet liveDocs;
    final int numDocs;

    public LiveDocsReader(AtomicReaderContext context, OpenBitSet liveDocs) throws IOException {
      super(context.reader());
      this.liveDocs = liveDocs;
      this.numDocs = (int)liveDocs.cardinality();
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




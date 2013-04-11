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
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.util.Hash;
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
  int numPieces;
  int currPartition = 0;

  public SolrIndexSplitter(SplitIndexCommand cmd) {
    field = cmd.getReq().getSchema().getUniqueKeyField();
    searcher = cmd.getReq().getSearcher();
    ranges = cmd.ranges;
    paths = cmd.paths;
    cores = cmd.cores;
    if (ranges == null) {
      numPieces =  paths != null ? paths.size() : cores.size();
    } else  {
      numPieces = ranges.size();
      rangesArr = ranges.toArray(new DocRouter.Range[ranges.size()]);
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
                                    core.getDirectoryFactory(), true, core.getSchema(),
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

    for (;;) {
      term = termsEnum.next();
      if (term == null) break;

      // figure out the hash for the term
      // TODO: hook in custom hashes (or store hashes)
      // TODO: performance implications of using indexedToReadable?
      CharsRef ref = new CharsRef(term.length);
      ref = field.getType().indexedToReadable(term, ref);
      int hash = Hash.murmurhash3_x86_32(ref, ref.offset, ref.length, 0);
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




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

package org.apache.solr.response.transform;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.BitsFilteredPostingsEnum;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.response.transform.ChildDocTransformerFactory.NUM_SEP_CHAR;
import static org.apache.solr.response.transform.ChildDocTransformerFactory.PATH_SEP_CHAR;
import static org.apache.solr.schema.IndexSchema.NEST_PATH_FIELD_NAME;

class ChildDocTransformer extends DocTransformer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String ANON_CHILD_KEY = "_childDocuments_";

  private final String name;
  private final BitSetProducer parentsFilter; // if null; resolve parent via uniqueKey instead
  private final DocSet childDocSet;
  private final int limit;
  private final boolean isNestedSchema;
  private final SolrReturnFields childReturnFields;
  private String[] extraRequestedFields;

  ChildDocTransformer(String name, BitSetProducer parentsFilter, DocSet childDocSet,
                      SolrReturnFields returnFields, boolean isNestedSchema, int limit,
                      String uniqueKeyField) {
    this.name = name;
    this.parentsFilter = parentsFilter;
    this.childDocSet = childDocSet;
    this.limit = limit;
    this.isNestedSchema = isNestedSchema;
    this.childReturnFields = returnFields!=null? returnFields: new SolrReturnFields();
    this.extraRequestedFields = parentsFilter == null ? new String[]{uniqueKeyField} : null;
  }

  @Override
  public String getName()  {
    return name;
  }

  @Override
  public boolean needsSolrIndexSearcher() { return true; }

  @Override
  public String[] getExtraRequestFields() {
    return extraRequestedFields;
  }

  private int getPrevRootGivenFilter(LeafReaderContext leafReaderContext, int segRootId) throws IOException {
    final BitSet segParentsBitSet = parentsFilter.getBitSet(leafReaderContext);
    if (segParentsBitSet != null) {
      return segRootId == 0 ? -1 : segParentsBitSet.prevSetBit(segRootId - 1);
    }
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "Parent filter '" + parentsFilter + "' doesn't match any parent documents");
  }

  private int getPrevRootGivenId(LeafReaderContext leafReaderContext, int segRootId,
                                 BytesRef idBytes) throws IOException {
    final LeafReader reader = leafReaderContext.reader();
    final Terms terms = reader.terms(IndexSchema.ROOT_FIELD_NAME); // never returns null here
    final TermsEnum iterator = terms.iterator();
    if (iterator.seekExact(idBytes)) {
      PostingsEnum docs = iterator.postings(null, PostingsEnum.NONE);
      docs = BitsFilteredPostingsEnum.wrap(docs, reader.getLiveDocs());
      int id = docs.nextDoc();
      if (id != DocIdSetIterator.NO_MORE_DOCS) {
        assert id <= segRootId : "integrity violation";
        return id - 1;
      }
    }
    return segRootId - 1; // thus no child docs
  }

  @Override
  public void transform(SolrDocument rootDoc, int rootDocId) {
    // note: this algorithm works if both if we have have _nest_path_  and also if we don't!

    try {

      // lookup what the *previous* rootDocId is, and figure which segment this is
      final SolrIndexSearcher searcher = context.getSearcher();
      final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
      final int seg = ReaderUtil.subIndex(rootDocId, leaves);
      final LeafReaderContext leafReaderContext = leaves.get(seg);
      final Bits liveDocs = leafReaderContext.reader().getLiveDocs();
      final int segBaseId = leafReaderContext.docBase;
      final int segRootId = rootDocId - segBaseId;

      // can return be -1 and that's okay  (happens for very first block)
      final int segPrevRootId;
      if (parentsFilter != null) {
        segPrevRootId = getPrevRootGivenFilter(leafReaderContext, segRootId);
      } else {
        final IndexSchema schema = searcher.getSchema();
        final String idStr = schema.printableUniqueKey(rootDoc);
        if (idStr == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "[child] requires fl to include the ID");
        }
        final BytesRef idBytes = schema.indexableUniqueKey(idStr);
        segPrevRootId = getPrevRootGivenId(leafReaderContext, rootDocId, idBytes);
      }

      if (segPrevRootId == (segRootId - 1)) {
        // doc has no children, return fast
        return;
      }

      // we'll need this soon...
      final SortedDocValues segPathDocValues = DocValues.getSorted(leafReaderContext.reader(), NEST_PATH_FIELD_NAME);
      // passing a different SortedDocValues obj since the child documents which come after are of smaller docIDs,
      // and the iterator can not be reversed.
      // The root doc is the input document to be transformed, and is not necessarily the root doc of the block of docs.
      final String rootDocPath = getPathByDocId(segRootId, DocValues.getSorted(leafReaderContext.reader(), NEST_PATH_FIELD_NAME));

      // the key in the Map is the document's ancestors key (one above the parent), while the key in the intermediate
      // MultiMap is the direct child document's key(of the parent document)
      final Map<String, Multimap<String, SolrDocument>> pendingParentPathsToChildren = new HashMap<>();

      final int firstChildId = segBaseId + segPrevRootId + 1;
      int matches = 0;
      // Loop each child ID up to the parent (exclusive).
      for (int docId = firstChildId; docId < rootDocId; ++docId) {
        final int segDocId = docId - segBaseId;

        // check whether doc is "live"
        if (liveDocs != null && !liveDocs.get(segDocId)) {
          // doc is not "live"; return fast
          continue;
        }

        // get the path.  (note will default to ANON_CHILD_KEY if schema is not nested or empty string if blank)
        final String fullDocPath = getPathByDocId(segDocId, segPathDocValues);

        if (isNestedSchema && !fullDocPath.startsWith(rootDocPath)) {
          // is not a descendant of the transformed doc; return fast.
          continue;
        }

        // Is this doc a direct ancestor of another doc we've seen?
        boolean isAncestor = pendingParentPathsToChildren.containsKey(fullDocPath);

        // Do we need to do anything with this doc (either ancestor or matched the child query)
        if (isAncestor || childDocSet == null || childDocSet.exists(docId)) {

          // If we reached the limit, only add if it's an ancestor
          if (limit != -1 && matches >= limit && !isAncestor) {
            continue;
          }
          ++matches; // note: includes ancestors that are not necessarily in childDocSet

          // load the doc
          SolrDocument doc = searcher.getDocFetcher().solrDoc(docId, childReturnFields);
          if(childReturnFields.getTransformer() != null) {
            if(childReturnFields.getTransformer().context == null) {
              childReturnFields.getTransformer().setContext(context);
            }
            childReturnFields.getTransformer().transform(doc, docId);
          }

          if (isAncestor) {
            // if this path has pending child docs, add them.
            addChildrenToParent(doc, pendingParentPathsToChildren.remove(fullDocPath)); // no longer pending
          }

          // get parent path
          String parentDocPath = getParentPath(fullDocPath);
          String lastPath = getLastPath(fullDocPath);
          // put into pending:
          // trim path if the doc was inside array, see trimPathIfArrayDoc()
          // e.g. toppings#1/ingredients#1 -> outer map key toppings#1
          // -> inner MultiMap key ingredients
          // or lonely#/lonelyGrandChild# -> outer map key lonely#
          // -> inner MultiMap key lonelyGrandChild#
          pendingParentPathsToChildren.computeIfAbsent(parentDocPath, x -> ArrayListMultimap.create())
              .put(trimLastPoundIfArray(lastPath), doc); // multimap add (won't replace)
        }
      }

      if (pendingParentPathsToChildren.isEmpty()) {
        // no child docs matched the child filter; return fast.
        return;
      }

      // only children of parent remain
      assert pendingParentPathsToChildren.keySet().size() == 1;

      // size == 1, so get the last remaining entry
      addChildrenToParent(rootDoc, pendingParentPathsToChildren.values().iterator().next());

    } catch (IOException e) {
      //TODO DWS: reconsider this unusual error handling approach; shouldn't we rethrow?
      log.warn("Could not fetch child documents", e);
      rootDoc.put(getName(), "Could not fetch child documents");
    }
  }

  private static void addChildrenToParent(SolrDocument parent, Multimap<String, SolrDocument> children) {
    for (String childLabel : children.keySet()) {
      addChildrenToParent(parent, children.get(childLabel), childLabel);
    }
  }

  private static void addChildrenToParent(SolrDocument parent, Collection<SolrDocument> children, String cDocsPath) {
    // if no paths; we do not need to add the child document's relation to its parent document.
    if (cDocsPath.equals(ANON_CHILD_KEY)) {
      parent.addChildDocuments(children);
      return;
    }
    // lookup leaf key for these children using path
    // depending on the label, add to the parent at the right key/label
    String trimmedPath = trimLastPound(cDocsPath);
    // if the child doc's path does not end with #, it is an array(same string is returned by ChildDocTransformer#trimLastPound)
    if (!parent.containsKey(trimmedPath) && (trimmedPath == cDocsPath)) {
      List<SolrDocument> list = new ArrayList<>(children);
      parent.setField(trimmedPath, list);
      return;
    }
    // is single value
    parent.setField(trimmedPath, ((List)children).get(0));
  }

  private static String getLastPath(String path) {
    int lastIndexOfPathSepChar = path.lastIndexOf(PATH_SEP_CHAR);
    if(lastIndexOfPathSepChar == -1) {
      return path;
    }
    return path.substring(lastIndexOfPathSepChar + 1);
  }

  private static String trimLastPoundIfArray(String path) {
    // remove index after last pound sign and if there is an array index e.g. toppings#1 -> toppings
    // or return original string if child doc is not in an array ingredients# -> ingredients#
    final int indexOfSepChar = path.lastIndexOf(NUM_SEP_CHAR);
    if (indexOfSepChar == -1) {
      return path;
    }
    int lastIndex = path.length() - 1;
    boolean singleDocVal = indexOfSepChar == lastIndex;
    return singleDocVal ? path: path.substring(0, indexOfSepChar);
  }

  private static String trimLastPound(String path) {
    // remove index after last pound sign and index from e.g. toppings#1 -> toppings
    int lastIndex = path.lastIndexOf('#');
    return lastIndex == -1 ? path : path.substring(0, lastIndex);
  }

  /**
   * Returns the *parent* path for this document.
   * Children of the root will yield null.
   */
  private static String getParentPath(String currDocPath) {
    // chop off leaf (after last '/')
    // if child of leaf then return null (special value)
    int lastPathIndex = currDocPath.lastIndexOf(PATH_SEP_CHAR);
    return lastPathIndex == -1 ? null : currDocPath.substring(0, lastPathIndex);
  }

  /** Looks up the nest path.  If there is none, returns {@link #ANON_CHILD_KEY}. */
  private String getPathByDocId(int segDocId, SortedDocValues segPathDocValues) throws IOException {
    if (!isNestedSchema) {
      return ANON_CHILD_KEY;
    }
    int numToAdvance = segPathDocValues.docID() == -1 ? segDocId : segDocId - (segPathDocValues.docID());
    assert numToAdvance >= 0;
    boolean advanced = segPathDocValues.advanceExact(segDocId);
    return advanced ? segPathDocValues.binaryValue().utf8ToString(): "";
  }
}

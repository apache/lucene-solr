/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Code to maintain and access indices.
 *
 * <h2>Table Of Contents</h2>
 *
 * <ol>
 *   <li><a href="#index">Index APIs</a>
 *       <ul>
 *         <li><a href="#writer">IndexWriter</a>
 *         <li><a href="#reader">IndexReader</a>
 *         <li><a href="#segments">Segments and docids</a>
 *       </ul>
 *   <li><a href="#field_types">Field types</a>
 *       <ul>
 *         <li><a href="#postings-desc">Postings</a>
 *         <li><a href="#stored-fields">Stored Fields</a>
 *         <li><a href="#docvalues">DocValues</a>
 *         <li><a href="#points">Points</a>
 *       </ul>
 *   <li><a href="#postings">Postings APIs</a>
 *       <ul>
 *         <li><a href="#fields">Fields</a>
 *         <li><a href="#terms">Terms</a>
 *         <li><a href="#documents">Documents</a>
 *         <li><a href="#positions">Positions</a>
 *       </ul>
 *   <li><a href="#stats">Index Statistics</a>
 *       <ul>
 *         <li><a href="#termstats">Term-level</a>
 *         <li><a href="#fieldstats">Field-level</a>
 *         <li><a href="#segmentstats">Segment-level</a>
 *         <li><a href="#documentstats">Document-level</a>
 *       </ul>
 * </ol>
 *
 * <a id="index"></a>
 *
 * <h2>Index APIs</h2>
 *
 * <a id="writer"></a>
 *
 * <h3>IndexWriter</h3>
 *
 * <p>{@link org.apache.lucene.index.IndexWriter} is used to create an index, and to add, update and
 * delete documents. The IndexWriter class is thread safe, and enforces a single instance per index.
 * Creating an IndexWriter creates a new index or opens an existing index for writing, in a {@link
 * org.apache.lucene.store.Directory}, depending on the configuration in {@link
 * org.apache.lucene.index.IndexWriterConfig}. A Directory is an abstraction that typically
 * represents a local file-system directory (see various implementations of {@link
 * org.apache.lucene.store.FSDirectory}), but it may also stand for some other storage, such as RAM.
 * <a id="reader"></a>
 *
 * <h3>IndexReader</h3>
 *
 * <p>{@link org.apache.lucene.index.IndexReader} is used to read data from the index, and supports
 * searching. Many thread-safe readers may be {@link org.apache.lucene.index.DirectoryReader#open}
 * concurrently with a single (or no) writer. Each reader maintains a consistent "point in time"
 * view of an index and must be explicitly refreshed (see {@link
 * org.apache.lucene.index.DirectoryReader#openIfChanged}) in order to incorporate writes that may
 * occur after it is opened. <a id="segments"></a>
 *
 * <h3>Segments and docids</h3>
 *
 * <p>Lucene's index is composed of segments, each of which contains a subset of all the documents
 * in the index, and is a complete searchable index in itself, over that subset. As documents are
 * written to the index, new segments are created and flushed to directory storage. Segments are
 * immutable; updates and deletions may only create new segments and do not modify existing ones.
 * Over time, the writer merges groups of smaller segments into single larger ones in order to
 * maintain an index that is efficient to search, and to reclaim dead space left behind by deleted
 * (and updated) documents.
 *
 * <p>Each document is identified by a 32-bit number, its "docid," and is composed of a collection
 * of Field values of diverse types (postings, stored fields, doc values, and points). Docids come
 * in two flavors: global and per-segment. A document's global docid is just the sum of its
 * per-segment docid and that segment's base docid offset. External, high-level APIs only handle
 * global docids, but internal APIs that reference a {@link org.apache.lucene.index.LeafReader},
 * which is a reader for a single segment, deal in per-segment docids.
 *
 * <p>Docids are assigned sequentially within each segment (starting at 0). Thus the number of
 * documents in a segment is the same as its maximum docid; some may be deleted, but their docids
 * are retained until the segment is merged. When segments merge, their documents are assigned new
 * sequential docids. Accordingly, docid values must always be treated as internal implementation,
 * not exposed as part of an application, nor stored or referenced outside of Lucene's internal
 * APIs. <a id="field_types"></a>
 *
 * <h2>Field Types</h2>
 *
 * <a id="postings-desc"></a>
 *
 * <p>Lucene supports a variety of different document field data structures. Lucene's core, the
 * inverted index, is comprised of "postings." The postings, with their term dictionary, can be
 * thought of as a map that provides efficient lookup given a {@link org.apache.lucene.index.Term}
 * (roughly, a word or token), to (the ordered list of) {@link org.apache.lucene.document.Document}s
 * containing that Term. Codecs may additionally record {@link
 * org.apache.lucene.index.ImpactsEnum#getImpacts impacts} alongside postings in order to be able to
 * skip over low-scoring documents at search time. Postings do not provide any way of retrieving
 * terms given a document, short of scanning the entire index. <a id="stored-fields"></a>
 *
 * <p>Stored fields are essentially the opposite of postings, providing efficient retrieval of field
 * values given a docid. All stored field values for a document are stored together in a block.
 * Different types of stored field provide high-level datatypes such as strings and numbers on top
 * of the underlying bytes. Stored field values are usually retrieved by the searcher using an
 * implementation of {@link org.apache.lucene.index.StoredFieldVisitor}. <a id="docvalues"></a>
 *
 * <p>{@link org.apache.lucene.index.DocValues} fields are what are sometimes referred to as
 * columnar, or column-stride fields, by analogy to relational database terminology, in which
 * documents are considered as rows, and fields, columns. DocValues fields store values per-field: a
 * value for every document is held in a single data structure, providing for rapid, sequential
 * lookup of a field-value given a docid. These fields are used for efficient value-based sorting,
 * and for faceting, but they are not useful for filtering. <a id="points"></a>
 *
 * <p>{@link org.apache.lucene.index.PointValues} represent numeric values using a kd-tree data
 * structure. Efficient 1- and higher dimensional implementations make these the choice for numeric
 * range and interval queries, and geo-spatial queries. <a id="postings"></a>
 *
 * <h2>Postings APIs</h2>
 *
 * <a id="fields"></a>
 *
 * <h3>Fields </h3>
 *
 * <p>{@link org.apache.lucene.index.Fields} is the initial entry point into the postings APIs, this
 * can be obtained in several ways:
 *
 * <pre class="prettyprint">
 * // access indexed fields for an index segment
 * Fields fields = reader.fields();
 * // access term vector fields for a specified document
 * Fields fields = reader.getTermVectors(docid);
 * </pre>
 *
 * Fields implements Java's Iterable interface, so it's easy to enumerate the list of fields:
 *
 * <pre class="prettyprint">
 * // enumerate list of fields
 * for (String field : fields) {
 *   // access the terms for this field
 *   Terms terms = fields.terms(field);
 * }
 * </pre>
 *
 * <a id="terms"></a>
 *
 * <h3>Terms </h3>
 *
 * <p>{@link org.apache.lucene.index.Terms} represents the collection of terms within a field,
 * exposes some metadata and <a href="#fieldstats">statistics</a>, and an API for enumeration.
 *
 * <pre class="prettyprint">
 * // metadata about the field
 * System.out.println("positions? " + terms.hasPositions());
 * System.out.println("offsets? " + terms.hasOffsets());
 * System.out.println("payloads? " + terms.hasPayloads());
 * // iterate through terms
 * TermsEnum termsEnum = terms.iterator(null);
 * BytesRef term = null;
 * while ((term = termsEnum.next()) != null) {
 *   doSomethingWith(termsEnum.term());
 * }
 * </pre>
 *
 * {@link org.apache.lucene.index.TermsEnum} provides an iterator over the list of terms within a
 * field, some <a href="#termstats">statistics</a> about the term, and methods to access the term's
 * <a href="#documents">documents</a> and <a href="#positions">positions</a>.
 *
 * <pre class="prettyprint">
 * // seek to a specific term
 * boolean found = termsEnum.seekExact(new BytesRef("foobar"));
 * if (found) {
 *   // get the document frequency
 *   System.out.println(termsEnum.docFreq());
 *   // enumerate through documents
 *   PostingsEnum docs = termsEnum.postings(null, null);
 *   // enumerate through documents and positions
 *   PostingsEnum docsAndPositions = termsEnum.postings(null, null, PostingsEnum.FLAG_POSITIONS);
 * }
 * </pre>
 *
 * <a id="documents"></a>
 *
 * <h3>Documents </h3>
 *
 * <p>{@link org.apache.lucene.index.PostingsEnum} is an extension of {@link
 * org.apache.lucene.search.DocIdSetIterator}that iterates over the list of documents for a term,
 * along with the term frequency within that document.
 *
 * <pre class="prettyprint">
 * int docid;
 * while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
 *   System.out.println(docid);
 *   System.out.println(docsEnum.freq());
 *  }
 * </pre>
 *
 * <a id="positions"></a>
 *
 * <h3>Positions </h3>
 *
 * <p>PostingsEnum also allows iteration of the positions a term occurred within the document, and
 * any additional per-position information (offsets and payload). The information available is
 * controlled by flags passed to TermsEnum#postings
 *
 * <pre class="prettyprint">
 * int docid;
 * PostingsEnum postings = termsEnum.postings(null, null, PostingsEnum.FLAG_PAYLOADS | PostingsEnum.FLAG_OFFSETS);
 * while ((docid = postings.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
 *   System.out.println(docid);
 *   int freq = postings.freq();
 *   for (int i = 0; i &lt; freq; i++) {
 *    System.out.println(postings.nextPosition());
 *    System.out.println(postings.startOffset());
 *    System.out.println(postings.endOffset());
 *    System.out.println(postings.getPayload());
 *   }
 * }
 * </pre>
 *
 * <a id="stats"></a>
 *
 * <h2>Index Statistics</h2>
 *
 * <a id="termstats"></a>
 *
 * <h3>Term statistics </h3>
 *
 * <ul>
 *   <li>{@link org.apache.lucene.index.TermsEnum#docFreq}: Returns the number of documents that
 *       contain at least one occurrence of the term. This statistic is always available for an
 *       indexed term. Note that it will also count deleted documents, when segments are merged the
 *       statistic is updated as those deleted documents are merged away.
 *   <li>{@link org.apache.lucene.index.TermsEnum#totalTermFreq}: Returns the number of occurrences
 *       of this term across all documents. Like docFreq(), it will also count occurrences that
 *       appear in deleted documents.
 * </ul>
 *
 * <a id="fieldstats"></a>
 *
 * <h3>Field statistics </h3>
 *
 * <ul>
 *   <li>{@link org.apache.lucene.index.Terms#size}: Returns the number of unique terms in the
 *       field. This statistic may be unavailable (returns <code>-1</code>) for some Terms
 *       implementations such as {@link org.apache.lucene.index.MultiTerms}, where it cannot be
 *       efficiently computed. Note that this count also includes terms that appear only in deleted
 *       documents: when segments are merged such terms are also merged away and the statistic is
 *       then updated.
 *   <li>{@link org.apache.lucene.index.Terms#getDocCount}: Returns the number of documents that
 *       contain at least one occurrence of any term for this field. This can be thought of as a
 *       Field-level docFreq(). Like docFreq() it will also count deleted documents.
 *   <li>{@link org.apache.lucene.index.Terms#getSumDocFreq}: Returns the number of postings
 *       (term-document mappings in the inverted index) for the field. This can be thought of as the
 *       sum of {@link org.apache.lucene.index.TermsEnum#docFreq} across all terms in the field, and
 *       like docFreq() it will also count postings that appear in deleted documents.
 *   <li>{@link org.apache.lucene.index.Terms#getSumTotalTermFreq}: Returns the number of tokens for
 *       the field. This can be thought of as the sum of {@link
 *       org.apache.lucene.index.TermsEnum#totalTermFreq} across all terms in the field, and like
 *       totalTermFreq() it will also count occurrences that appear in deleted documents.
 * </ul>
 *
 * <a id="segmentstats"></a>
 *
 * <h3>Segment statistics </h3>
 *
 * <ul>
 *   <li>{@link org.apache.lucene.index.IndexReader#maxDoc}: Returns the number of documents
 *       (including deleted documents) in the index.
 *   <li>{@link org.apache.lucene.index.IndexReader#numDocs}: Returns the number of live documents
 *       (excluding deleted documents) in the index.
 *   <li>{@link org.apache.lucene.index.IndexReader#numDeletedDocs}: Returns the number of deleted
 *       documents in the index.
 *   <li>{@link org.apache.lucene.index.Fields#size}: Returns the number of indexed fields.
 * </ul>
 *
 * <a id="documentstats"></a>
 *
 * <h3>Document statistics </h3>
 *
 * <p>Document statistics are available during the indexing process for an indexed field: typically
 * a {@link org.apache.lucene.search.similarities.Similarity} implementation will store some of
 * these values (possibly in a lossy way), into the normalization value for the document in its
 * {@link org.apache.lucene.search.similarities.Similarity#computeNorm} method.
 *
 * <ul>
 *   <li>{@link org.apache.lucene.index.FieldInvertState#getLength}: Returns the number of tokens
 *       for this field in the document. Note that this is just the number of times that {@link
 *       org.apache.lucene.analysis.TokenStream#incrementToken} returned true, and is unrelated to
 *       the values in {@link
 *       org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute}.
 *   <li>{@link org.apache.lucene.index.FieldInvertState#getNumOverlap}: Returns the number of
 *       tokens for this field in the document that had a position increment of zero. This can be
 *       used to compute a document length that discounts artificial tokens such as synonyms.
 *   <li>{@link org.apache.lucene.index.FieldInvertState#getPosition}: Returns the accumulated
 *       position value for this field in the document: computed from the values of {@link
 *       org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute} and including {@link
 *       org.apache.lucene.analysis.Analyzer#getPositionIncrementGap}s across multivalued fields.
 *   <li>{@link org.apache.lucene.index.FieldInvertState#getOffset}: Returns the total character
 *       offset value for this field in the document: computed from the values of {@link
 *       org.apache.lucene.analysis.tokenattributes.OffsetAttribute} returned by {@link
 *       org.apache.lucene.analysis.TokenStream#end}, and including {@link
 *       org.apache.lucene.analysis.Analyzer#getOffsetGap}s across multivalued fields.
 *   <li>{@link org.apache.lucene.index.FieldInvertState#getUniqueTermCount}: Returns the number of
 *       unique terms encountered for this field in the document.
 *   <li>{@link org.apache.lucene.index.FieldInvertState#getMaxTermFrequency}: Returns the maximum
 *       frequency across all unique terms encountered for this field in the document.
 * </ul>
 *
 * <p>Additional user-supplied statistics can be added to the document as DocValues fields and
 * accessed via {@link org.apache.lucene.index.LeafReader#getNumericDocValues}.
 */
package org.apache.lucene.index;

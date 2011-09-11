package org.apache.lucene.store.instantiated;

/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectorOffsetInfo;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SimilarityProvider;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BitVector;

/**
 * This class, similar to {@link org.apache.lucene.index.IndexWriter}, has no locking mechanism.
 * <p>
 * {@link org.apache.lucene.store.instantiated.InstantiatedIndexReader} is navigating
 * the same instances in memory as this writer is updating so searchers active while
 * you are committing are bound to throw exceptions.
 * <p>
 * Consider using InstantiatedIndex as if it was immutable.
 *
 * @see org.apache.lucene.index.IndexWriter 
 */
public class InstantiatedIndexWriter implements Closeable {

  private PrintStream infoStream = null;

  private final InstantiatedIndex index;
  private final Analyzer analyzer;

  private SimilarityProvider similarityProvider = IndexSearcher.getDefaultSimilarityProvider(); // how to normalize;

  private transient Set<String> fieldNameBuffer;
  /**
   * linked to ensure chronological order
   */
  private Map<InstantiatedDocument, Map<FieldSetting, Map<String /*text*/, TermDocumentInformationFactory>>> termDocumentInformationFactoryByDocument = new LinkedHashMap<InstantiatedDocument, Map<FieldSetting, Map<String /*text*/, TermDocumentInformationFactory>>>(2000);

  private Set<InstantiatedDocument> unflushedDocuments = new HashSet<InstantiatedDocument>();

  public InstantiatedIndexWriter(InstantiatedIndex index) throws IOException {
    this(index, null);
  }

  public InstantiatedIndexWriter(InstantiatedIndex index, Analyzer analyzer) throws IOException {
    this(index, analyzer, false);
  }

  public InstantiatedIndexWriter(InstantiatedIndex index, Analyzer analyzer, boolean create) throws IOException {
    this.index = index;
    this.analyzer = analyzer;
    fieldNameBuffer = new HashSet<String>();
    if (create) {
      this.index.initialize();
    }
  }

  private int mergeFactor = 2500;

  /**
   * The sweetspot for this implementation is somewhere around 2500 at 2K text large documents.
   * <p/>
   * Benchmark output:
   * <pre>
   *  ------------> Report sum by Prefix (MAddDocs) and Round (8 about 8 out of 160153)
   *  Operation      round  mrg buf cmpnd   runCnt   recsPerRun        rec/s  elapsedSec    avgUsedMem    avgTotalMem
   *  MAddDocs_20000     0   10  10  true        1        20000         81,4      245,68   200 325 152    268 156 928
   *  MAddDocs_20000 -   1 1000  10  true -  -   1 -  -   20000 -  -   494,1 -  -  40,47 - 247 119 072 -  347 025 408
   *  MAddDocs_20000     2   10 100  true        1        20000        104,8      190,81   233 895 552    363 720 704
   *  MAddDocs_20000 -   3 2000 100  true -  -   1 -  -   20000 -  -   527,2 -  -  37,94 - 266 136 448 -  378 273 792
   *  MAddDocs_20000     4   10  10 false        1        20000        103,2      193,75   222 089 792    378 273 792
   *  MAddDocs_20000 -   5 3000  10 false -  -   1 -  -   20000 -  -   545,2 -  -  36,69 - 237 917 152 -  378 273 792
   *  MAddDocs_20000     6   10 100 false        1        20000        102,7      194,67   237 018 976    378 273 792
   *  MAddDocs_20000 -   7 4000 100 false -  -   1 -  -   20000 -  -   535,8 -  -  37,33 - 309 680 640 -  501 968 896
   * </pre>
   *
   * @see org.apache.lucene.index.LogMergePolicy#setMergeFactor(int)
   */
  public void setMergeFactor(int mergeFactor) {
    this.mergeFactor = mergeFactor;
  }

  /**
   * @see org.apache.lucene.index.LogMergePolicy#getMergeFactor()
   */
  public int getMergeFactor() {
    return mergeFactor;
  }


  /**
   * If non-null, information about merges and a message when
   * maxFieldLength is reached could be printed to this -- currently
   * not yet implemented.
   */
  public void setInfoStream(PrintStream infoStream) {
    this.infoStream = infoStream;
  }


  public void abort() throws IOException {
    // what not
  }


  public void addIndexes(IndexReader[] readers) {
    throw new RuntimeException("Not implemented");
  }


  public PrintStream getInfoStream() {
    return infoStream;
  }


  /**
   * Flushes all changes to an index and closes all associated files.
   */
  public void close() throws IOException {
    commit();
  }

  /**
   * Returns the number of documents currently in this index.
   */
  public int docCount() {
    // todo: not certain. see http://www.nabble.com/IndexWriter.docCount-tf3128882.html#a8669483
    return index.getDocumentsByNumber().length /* - index.getDeletedDocuments().size() */ + unflushedDocuments.size();
  }

  /**
   * Locks the index and commits the buffered documents.
   */
  public void commit() throws IOException {

    // todo write lock, unless held by caller

    boolean orderedTermsDirty = false;
    Set<InstantiatedTerm> dirtyTerms = new HashSet<InstantiatedTerm>(1000);
    
    Map<String, FieldSetting> fieldSettingsByFieldName = new HashMap<String, FieldSetting>();
    for (String fieldName : fieldNameBuffer) {
      fieldSettingsByFieldName.put(fieldName, new FieldSetting(fieldName));
    }

    InstantiatedDocument[] documentsByNumber = new InstantiatedDocument[index.getDocumentsByNumber().length + termDocumentInformationFactoryByDocument.size()];
    System.arraycopy(index.getDocumentsByNumber(), 0, documentsByNumber, 0, index.getDocumentsByNumber().length);
    int documentNumber = index.getDocumentsByNumber().length;

    List<InstantiatedTerm> orderedTerms = new ArrayList<InstantiatedTerm>(index.getOrderedTerms().length + 5000);
    for (InstantiatedTerm instantiatedTerm : index.getOrderedTerms()) {
      orderedTerms.add(instantiatedTerm);
    }

    // update norm array with fake values for new documents
    Map<String, byte[]> normsByFieldNameAndDocumentNumber = new HashMap<String, byte[]>(index.getTermsByFieldAndText().size());
    Set<String> fieldNames = new HashSet<String>(20);
    fieldNames.addAll(index.getNormsByFieldNameAndDocumentNumber().keySet());
    fieldNames.addAll(fieldNameBuffer);
    for (String field : index.getTermsByFieldAndText().keySet()) {
      byte[] norms = new byte[index.getDocumentsByNumber().length + termDocumentInformationFactoryByDocument.size()];
      byte[] oldNorms = index.getNormsByFieldNameAndDocumentNumber().get(field);
      if (oldNorms != null) {
        System.arraycopy(oldNorms, 0, norms, 0, oldNorms.length);
        Arrays.fill(norms, oldNorms.length, norms.length, (byte) 0);
      } else {
        Arrays.fill(norms, 0, norms.length, (byte) 0);
      }
      normsByFieldNameAndDocumentNumber.put(field, norms);
      fieldNames.remove(field);
    }
    for (String field : fieldNames) {
      //System.out.println(field);
      byte[] norms = new byte[index.getDocumentsByNumber().length + termDocumentInformationFactoryByDocument.size()];
      Arrays.fill(norms, 0, norms.length, (byte) 0);
      normsByFieldNameAndDocumentNumber.put(field, norms);
    }
    fieldNames.clear();
    index.setNormsByFieldNameAndDocumentNumber(normsByFieldNameAndDocumentNumber);

    for (Map.Entry<InstantiatedDocument, Map<FieldSetting, Map<String /*text*/, TermDocumentInformationFactory>>> eDocumentTermDocInfoByTermTextAndField : termDocumentInformationFactoryByDocument.entrySet()) {

      InstantiatedDocument document = eDocumentTermDocInfoByTermTextAndField.getKey();

      // assign document number
      document.setDocumentNumber(documentNumber++);
      documentsByNumber[document.getDocumentNumber()] = document;

      // set norms, prepare document and create optimized size collections.

      int numFieldsWithTermVectorsInDocument = 0;
      int termsInDocument = 0;
      for (Map.Entry<FieldSetting, Map<String /*text*/, TermDocumentInformationFactory>> eFieldTermDocInfoFactoriesByTermText : eDocumentTermDocInfoByTermTextAndField.getValue().entrySet()) {
        if (eFieldTermDocInfoFactoriesByTermText.getKey().storeTermVector) {
          numFieldsWithTermVectorsInDocument += eFieldTermDocInfoFactoriesByTermText.getValue().size();
        }
        termsInDocument += eFieldTermDocInfoFactoriesByTermText.getValue().size();

        if (eFieldTermDocInfoFactoriesByTermText.getKey().indexed && !eFieldTermDocInfoFactoriesByTermText.getKey().omitNorms) {
          final String fieldName = eFieldTermDocInfoFactoriesByTermText.getKey().fieldName;
          final FieldInvertState invertState = new FieldInvertState();
          invertState.setBoost(eFieldTermDocInfoFactoriesByTermText.getKey().boost);
          invertState.setLength(eFieldTermDocInfoFactoriesByTermText.getKey().fieldLength);
          normsByFieldNameAndDocumentNumber.get(fieldName)[document.getDocumentNumber()] = similarityProvider.get(fieldName).computeNorm(invertState);
        } else {
          System.currentTimeMillis();
        }

      }

      /** used for term vectors only, i think.. */
      Map<InstantiatedTerm, InstantiatedTermDocumentInformation> informationByTermOfCurrentDocument = new HashMap<InstantiatedTerm, InstantiatedTermDocumentInformation>(termsInDocument);


      Map<String, FieldSetting> documentFieldSettingsByFieldName = new HashMap<String, FieldSetting>(eDocumentTermDocInfoByTermTextAndField.getValue().size());

      // terms...
      for (Map.Entry<FieldSetting, Map<String /*text*/, TermDocumentInformationFactory>> eFieldSetting_TermDocInfoFactoriesByTermText : eDocumentTermDocInfoByTermTextAndField.getValue().entrySet()) {
        documentFieldSettingsByFieldName.put(eFieldSetting_TermDocInfoFactoriesByTermText.getKey().fieldName, eFieldSetting_TermDocInfoFactoriesByTermText.getKey());

        // find or create term
        for (Map.Entry<String /*text*/, TermDocumentInformationFactory> eTermText_TermDocInfoFactory : eFieldSetting_TermDocInfoFactoriesByTermText.getValue().entrySet()) {

          // get term..
          InstantiatedTerm term;
          Map<String, InstantiatedTerm> termsByText = index.getTermsByFieldAndText().get(eFieldSetting_TermDocInfoFactoriesByTermText.getKey().fieldName);
          if (termsByText == null) {
            termsByText = new HashMap<String, InstantiatedTerm>(1000);
            index.getTermsByFieldAndText().put(eFieldSetting_TermDocInfoFactoriesByTermText.getKey().fieldName, termsByText);
            term = new InstantiatedTerm(eFieldSetting_TermDocInfoFactoriesByTermText.getKey().fieldName, eTermText_TermDocInfoFactory.getKey());
            termsByText.put(eTermText_TermDocInfoFactory.getKey(), term);
            int pos = Collections.binarySearch(orderedTerms, term, InstantiatedTerm.comparator);
            pos = -1 - pos;
            orderedTerms.add(pos, term);
            orderedTermsDirty = true;
          } else {
            term = termsByText.get(eTermText_TermDocInfoFactory.getKey());
            if (term == null) {
              term = new InstantiatedTerm(eFieldSetting_TermDocInfoFactoriesByTermText.getKey().fieldName, eTermText_TermDocInfoFactory.getKey());
              termsByText.put(eTermText_TermDocInfoFactory.getKey(), term);
              int pos = Collections.binarySearch(orderedTerms, term, InstantiatedTerm.comparator);
              pos = -1 - pos;
              orderedTerms.add(pos, term);
              orderedTermsDirty = true;
            }
          }

          // create association term document information
          //
          // [Term]-- {0..*} | {0..* ordered} --(field)[Document]
          //
          //                 |
          //        [TermDocumentInformation]

          int[] positions = new int[eTermText_TermDocInfoFactory.getValue().termPositions.size()];
          for (int i = 0; i < positions.length; i++) {
            positions[i] = eTermText_TermDocInfoFactory.getValue().termPositions.get(i);
          }

          byte[][] payloads = new byte[eTermText_TermDocInfoFactory.getValue().payloads.size()][];
          for (int i = 0; i < payloads.length; i++) {
            payloads[i] = eTermText_TermDocInfoFactory.getValue().payloads.get(i);
          }

          // couple

          InstantiatedTermDocumentInformation info = new InstantiatedTermDocumentInformation(term, document, /*eTermText_TermDocInfoFactory.getValue().termFrequency,*/ positions, payloads);

          // todo optimize, this should be cached and updated to array in batches rather than appending the array once for every position!
          InstantiatedTermDocumentInformation[] associatedDocuments;
          if (term.getAssociatedDocuments() != null) {
            associatedDocuments = new InstantiatedTermDocumentInformation[term.getAssociatedDocuments().length + 1];
            System.arraycopy(term.getAssociatedDocuments(), 0, associatedDocuments, 0, term.getAssociatedDocuments().length);
          } else {
            associatedDocuments = new InstantiatedTermDocumentInformation[1];
          }
          associatedDocuments[associatedDocuments.length - 1] = info;          
          term.setAssociatedDocuments(associatedDocuments);
          term.addPositionsCount(positions.length);

          // todo optimize, only if term vector?
          informationByTermOfCurrentDocument.put(term, info);


          dirtyTerms.add(term);
        }

        // term vector offsets
        if (eFieldSetting_TermDocInfoFactoriesByTermText.getKey().storeOffsetWithTermVector) {
          for (Map.Entry<InstantiatedTerm, InstantiatedTermDocumentInformation> e : informationByTermOfCurrentDocument.entrySet()) {
            if (eFieldSetting_TermDocInfoFactoriesByTermText.getKey().fieldName.equals(e.getKey().field())) {
              TermDocumentInformationFactory factory = eFieldSetting_TermDocInfoFactoriesByTermText.getValue().get(e.getKey().text());
              e.getValue().setTermOffsets(factory.termOffsets.toArray(new TermVectorOffsetInfo[factory.termOffsets.size()]));
            }
          }
        }
      }

      Map<String, List<InstantiatedTermDocumentInformation>> termDocumentInformationsByField = new HashMap<String, List<InstantiatedTermDocumentInformation>>();
      for (Map.Entry<InstantiatedTerm, InstantiatedTermDocumentInformation> eTerm_TermDocumentInformation : informationByTermOfCurrentDocument.entrySet()) {
        List<InstantiatedTermDocumentInformation> termDocumentInformations = termDocumentInformationsByField.get(eTerm_TermDocumentInformation.getKey().field());
        if (termDocumentInformations == null) {
          termDocumentInformations = new ArrayList<InstantiatedTermDocumentInformation>();
          termDocumentInformationsByField.put(eTerm_TermDocumentInformation.getKey().field(), termDocumentInformations);
        }
        termDocumentInformations.add(eTerm_TermDocumentInformation.getValue());
      }

      for (Map.Entry<String, List<InstantiatedTermDocumentInformation>> eField_TermDocInfos : termDocumentInformationsByField.entrySet()) {

        CollectionUtil.quickSort(eField_TermDocInfos.getValue(), tdComp);

        // add term vector
        if (documentFieldSettingsByFieldName.get(eField_TermDocInfos.getKey()).storeTermVector) {
          if (document.getVectorSpace() == null) {
            document.setVectorSpace(new HashMap<String, List<InstantiatedTermDocumentInformation>>(documentFieldSettingsByFieldName.size()));
          }
          document.getVectorSpace().put(eField_TermDocInfos.getKey(), eField_TermDocInfos.getValue());
        }

      }
      fieldSettingsByFieldName.putAll(documentFieldSettingsByFieldName);
    }

    // order document informations in dirty terms
    for (InstantiatedTerm term : dirtyTerms) {
      // todo optimize, i believe this is useless, that the natural order is document number?
      ArrayUtil.mergeSort(term.getAssociatedDocuments(), InstantiatedTermDocumentInformation.documentNumberComparator);

//      // update association class reference for speedy skipTo()
//      for (int i = 0; i < term.getAssociatedDocuments().length; i++) {
//        term.getAssociatedDocuments()[i].setIndexFromTerm(i);
//      }
    }


    // flush to writer
    index.setDocumentsByNumber(documentsByNumber);
    index.setOrderedTerms(orderedTerms.toArray(new InstantiatedTerm[orderedTerms.size()]));

    for (FieldSetting fieldSetting : fieldSettingsByFieldName.values()) {
      index.getFieldSettings().merge(fieldSetting);
    }
    // set term index
    if (orderedTermsDirty) {
      // todo optimize, only update from start position
      for (int i = 0; i < index.getOrderedTerms().length; i++) {
        index.getOrderedTerms()[i].setTermIndex(i);
      }

    }

    // remove deleted documents
    IndexReader indexDeleter = index.indexReaderFactory();
    if (unflushedDeletions.size() > 0) {
      for (Term term : unflushedDeletions) {
        indexDeleter.deleteDocuments(term);
      }
      unflushedDeletions.clear();
    }


    // all done, clear buffers
    unflushedDocuments.clear();
    termDocumentInformationFactoryByDocument.clear();
    fieldNameBuffer.clear();


    // update deleted documents bitset
    if (index.getDeletedDocuments() != null) {
      BitVector deletedDocuments = new BitVector(index.getDocumentsByNumber().length);
      for (int i = 0; i < index.getDeletedDocuments().size(); i++) {
        if (index.getDeletedDocuments().get(i)) {
          deletedDocuments.set(i);
        }
      }
      index.setDeletedDocuments(deletedDocuments);
    }

    index.setVersion(System.currentTimeMillis());

    // todo unlock

    indexDeleter.close();

  }

  private static final Comparator<InstantiatedTermDocumentInformation> tdComp = new Comparator<InstantiatedTermDocumentInformation>() {
    public int compare(InstantiatedTermDocumentInformation instantiatedTermDocumentInformation, InstantiatedTermDocumentInformation instantiatedTermDocumentInformation1) {
      return instantiatedTermDocumentInformation.getTerm().getTerm().compareTo(instantiatedTermDocumentInformation1.getTerm().getTerm());
    }
  };

  /**
   * Adds a document to this index.
   */
  public void addDocument(Document doc) throws IOException {
    addDocument(doc, getAnalyzer());
  }

  /**
   * Adds a document to this index, using the provided analyzer instead of the
   * value of {@link #getAnalyzer()}.
   *
   * @param doc
   * @param analyzer
   * @throws IOException
   */
  public void addDocument(Document doc, Analyzer analyzer) throws IOException {
    addDocument(new InstantiatedDocument(doc), analyzer);
  }

  /**
   * Tokenizes a document and adds it to the buffer.
   * Try to do all calculations in this method rather than in commit, as this is a non locking method.
   * Remember, this index implementation expects unlimited memory for maximum speed.
   *
   * @param document
   * @param analyzer
   * @throws IOException
   */
  protected void addDocument(InstantiatedDocument document, Analyzer analyzer) throws IOException {

    if (document.getDocumentNumber() != null) {
      throw new RuntimeException("Document number already set! Are you trying to add a document that already is bound to this or another index?");
    }

    // todo: write lock

    // normalize settings per field name in document

    Map<String /* field name */, FieldSetting> fieldSettingsByFieldName = new HashMap<String, FieldSetting>();
    for (IndexableField field : document.getDocument()) {
      FieldSetting fieldSetting = fieldSettingsByFieldName.get(field.name());
      if (fieldSetting == null) {
        fieldSetting = new FieldSetting();
        fieldSetting.fieldName = field.name();
        fieldSettingsByFieldName.put(fieldSetting.fieldName, fieldSetting);
        fieldNameBuffer.add(fieldSetting.fieldName);
      }

      // todo: fixme: multiple fields with the same name does not mean field boost += more boost.
      fieldSetting.boost *= field.boost();
      //fieldSettings.dimensions++;


      // once fieldSettings, always fieldSettings.
      if (field.fieldType().omitNorms()) {
        fieldSetting.omitNorms = true;
      }
      if (field.fieldType().indexed() ) {
        fieldSetting.indexed = true;
      }
      if (field.fieldType().tokenized()) {
        fieldSetting.tokenized = true;
      }
      if (field.fieldType().stored()) {
        fieldSetting.stored = true;
      }
      if (field.binaryValue() != null) {
        fieldSetting.isBinary = true;
      }
      if (field.fieldType().storeTermVectors()) {
        fieldSetting.storeTermVector = true;
      }
      if (field.fieldType().storeTermVectorPositions()) {
        fieldSetting.storePositionWithTermVector = true;
      }
      if (field.fieldType().storeTermVectorOffsets()) {
        fieldSetting.storeOffsetWithTermVector = true;
      }
    }

    Map<IndexableField, LinkedList<Token>> tokensByField = new LinkedHashMap<IndexableField, LinkedList<Token>>(20);

    // tokenize indexed fields.
    for (Iterator<IndexableField> it = document.getDocument().iterator(); it.hasNext();) {

      IndexableField field = it.next();

      FieldSetting fieldSetting = fieldSettingsByFieldName.get(field.name());

      if (field.fieldType().indexed()) {

        LinkedList<Token> tokens = new LinkedList<Token>();
        tokensByField.put(field, tokens);

        if (field.fieldType().tokenized()) {
          final TokenStream tokenStream;
          // todo readerValue(), binaryValue()
          if (field.tokenStreamValue() != null) {
            tokenStream = field.tokenStreamValue();
          } else {
            tokenStream = analyzer.reusableTokenStream(field.name(), new StringReader(field.stringValue()));
          }

          // reset the TokenStream to the first token          
          tokenStream.reset();

          while (tokenStream.incrementToken()) {
            // TODO: this is a simple workaround to still work with tokens, not very effective, but as far as I know, this writer should get removed soon:
            final Token token = new Token();
            for (Iterator<AttributeImpl> atts = tokenStream.getAttributeImplsIterator(); atts.hasNext();) {
              final AttributeImpl att = atts.next();
              try {
                att.copyTo(token);
              } catch (Exception e) {
                // ignore unsupported attributes,
                // this may fail to copy some attributes, if a special combined AttributeImpl is used, that
                // implements basic attributes supported by Token and also other customized ones in one class.
              }
            }
            tokens.add(token); // the vector will be built on commit.
            fieldSetting.fieldLength++;
          }
          tokenStream.end();
          tokenStream.close();
        } else {
          // untokenized
          String fieldVal = field.stringValue();
          Token token = new Token(0, fieldVal.length(), "untokenized");
          token.setEmpty().append(fieldVal);
          tokens.add(token);
          fieldSetting.fieldLength++;
        }
      }

      if (!field.fieldType().stored()) {
        //it.remove();
      }
    }


    Map<FieldSetting, Map<String /*text*/, TermDocumentInformationFactory>> termDocumentInformationFactoryByTermTextAndFieldSetting = new HashMap<FieldSetting, Map<String /*text*/, TermDocumentInformationFactory>>();
    termDocumentInformationFactoryByDocument.put(document, termDocumentInformationFactoryByTermTextAndFieldSetting);

    // build term vector, term positions and term offsets
    for (Map.Entry<IndexableField, LinkedList<Token>> eField_Tokens : tokensByField.entrySet()) {
      FieldSetting fieldSetting = fieldSettingsByFieldName.get(eField_Tokens.getKey().name());

      Map<String, TermDocumentInformationFactory> termDocumentInformationFactoryByTermText = termDocumentInformationFactoryByTermTextAndFieldSetting.get(fieldSettingsByFieldName.get(eField_Tokens.getKey().name()));
      if (termDocumentInformationFactoryByTermText == null) {
        termDocumentInformationFactoryByTermText = new HashMap<String /*text*/, TermDocumentInformationFactory>();
        termDocumentInformationFactoryByTermTextAndFieldSetting.put(fieldSettingsByFieldName.get(eField_Tokens.getKey().name()), termDocumentInformationFactoryByTermText);
      }

      int lastOffset = 0;

      // for each new field, move positions a bunch.
      if (fieldSetting.position > 0) {
        // todo what if no analyzer set, multiple fields with same name and index without tokenization?
        fieldSetting.position += analyzer.getPositionIncrementGap(fieldSetting.fieldName);
      }

      for (Token token : eField_Tokens.getValue()) {

        TermDocumentInformationFactory termDocumentInformationFactory = termDocumentInformationFactoryByTermText.get(token.toString());
        if (termDocumentInformationFactory == null) {
          termDocumentInformationFactory = new TermDocumentInformationFactory();
          termDocumentInformationFactoryByTermText.put(token.toString(), termDocumentInformationFactory);
        }
        //termDocumentInformationFactory.termFrequency++;

        fieldSetting.position += (token.getPositionIncrement() - 1);
        termDocumentInformationFactory.termPositions.add(fieldSetting.position++);

        if (token.getPayload() != null && token.getPayload().length() > 0) {
          termDocumentInformationFactory.payloads.add(token.getPayload().toByteArray());
          fieldSetting.storePayloads = true;
        } else {
          termDocumentInformationFactory.payloads.add(null);
        }

        if (eField_Tokens.getKey().fieldType().storeTermVectorOffsets()) {

          termDocumentInformationFactory.termOffsets.add(new TermVectorOffsetInfo(fieldSetting.offset + token.startOffset(), fieldSetting.offset + token.endOffset()));
          lastOffset = fieldSetting.offset + token.endOffset();
        }


      }

      if (eField_Tokens.getKey().fieldType().storeTermVectorOffsets()) {
        fieldSetting.offset = lastOffset + 1;
      }

    }


    unflushedDocuments.add(document);

    // if too many documents in buffer, commit.
    if (unflushedDocuments.size() >= getMergeFactor()) {
      commit(/*lock*/);
    }

    // todo: unlock write lock

  }


  private Set<Term> unflushedDeletions = new HashSet<Term>();

  public void deleteDocuments(Term term) throws IOException {
    unflushedDeletions.add(term);
  }

  public void deleteDocuments(Term[] terms) throws IOException {
    for (Term term : terms) {
      deleteDocuments(term);
    }
  }

  public void updateDocument(Term term, Document doc) throws IOException {
    updateDocument(term, doc, getAnalyzer());
  }

  public void updateDocument(Term term, Document doc, Analyzer analyzer) throws IOException {
    deleteDocuments(term);
    addDocument(doc, analyzer);
  }

  public SimilarityProvider getSimilarityProvider() {
    return similarityProvider;
  }

  public void setSimilarityProvider(SimilarityProvider similarityProvider) {
    this.similarityProvider = similarityProvider;
  }

  public Analyzer getAnalyzer() {
    return analyzer;
  }

  private class TermDocumentInformationFactory {
    private LinkedList<byte[]> payloads = new LinkedList<byte[]>();
    private LinkedList<Integer> termPositions = new LinkedList<Integer>();
    private LinkedList<TermVectorOffsetInfo> termOffsets = new LinkedList<TermVectorOffsetInfo>();
  }


  static class FieldSetting extends org.apache.lucene.store.instantiated.FieldSetting {

    float boost = 1;
    int position = 0;
    int offset;
    int fieldLength = 0;

    boolean omitNorms = false;
    boolean isBinary = false;

    private FieldSetting() {
    }

    private FieldSetting(String fieldName) {
      super(fieldName);
    }
  }


}

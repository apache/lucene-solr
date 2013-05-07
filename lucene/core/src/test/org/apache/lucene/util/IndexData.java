package org.apache.lucene.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;

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

/**
 * Contains terms and stored fields extracted from a reader, to be used to
 * compare indexes which contain similar documents.
 */
public class IndexData {
  
  ArrayList<DocumentData> nonEmpty;
  
  public IndexData(IndexReader reader) throws IOException {
    Bits liveDocs = MultiFields.getLiveDocs(reader);
    
    ArrayList<DocumentData> docs = new ArrayList<DocumentData>();
    for (int i = 0; i < reader.maxDoc(); i++) {
      if (liveDocs == null || liveDocs.get(i)) {
        docs.add(new DocumentData(reader.document(i)));
      } else {
        docs.add(new DocumentData(new StoredDocument()));
      }
    }
    
    Fields fields = MultiFields.getFields(reader);
    if (fields != null) {
      Iterator<String> fieldsIterator = fields.iterator();
      while (fieldsIterator.hasNext()) {
        String field = fieldsIterator.next();
        Terms terms = fields.terms(field);
        TermsEnum termsIterator = terms.iterator(null);
        BytesRef term;
        while ((term = termsIterator.next()) != null) {
          DocsEnum termDocs = MultiFields.getTermDocsEnum(reader, liveDocs,
              field, term);
          while (termDocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            docs.get(termDocs.docID()).addTerm(field, term.toString(),
                termDocs.freq());
          }
        }
      }
    }
    
    nonEmpty = new ArrayList<DocumentData>();
    for (DocumentData doc : docs) {
      if (!doc.isEmpty()) {
        nonEmpty.add(doc);
      }
    }
  }
  
  @Override
  public String toString() {
    return toString(nonEmpty);
  }
  
  public String toString(boolean ordered) {
    if (!ordered) {
      return toString();
    }

    ArrayList<DocumentData> sorted = new ArrayList<IndexData.DocumentData>(
        nonEmpty);
    Collections.sort(sorted, new DocumentDataComparator());
    return toString(sorted);
  }

  public String toString(ArrayList<DocumentData> documentDatas) {
    StringBuilder builder = new StringBuilder(this.getClass().getSimpleName());
    builder.append('\n');
    int counter = 0;
    for (DocumentData doc : documentDatas) {
      builder.append('\t');
      builder.append("document ");
      builder.append(counter++);
      builder.append('\n');
      
      // print stored fields
      List<StorableField> storedFields = doc.storedFields;
      if (!storedFields.isEmpty()) {
        builder.append('\t');
        builder.append('\t');
        builder.append("storedFields");
        builder.append('\n');
        for (StorableField storableField : storedFields) {
          builder.append('\t');
          builder.append('\t');
          builder.append('\t');
          builder.append(storableField.name());
          builder.append('\t');
          builder.append(storableField.stringValue());
          builder.append('\n');
        }
      }
      
      // print terms
      Map<String,Integer> termsMap = doc.termsMap;
      if (!termsMap.isEmpty()) {
        builder.append('\t');
        builder.append('\t');
        builder.append("termsMap");
        builder.append('\n');
        
        List<String> termsList = new ArrayList<String>(termsMap.keySet());
        Collections.sort(termsList);
        for (String term : termsList) {
          builder.append('\t');
          builder.append('\t');
          builder.append('\t');
          builder.append(term);
          builder.append('\t');
          builder.append(termsMap.get(term));
          builder.append('\n');
        }
      }
    }
    return builder.toString();
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((nonEmpty == null) ? 0 : nonEmpty.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    IndexData other = (IndexData) obj;
    if (nonEmpty == null) {
      if (other.nonEmpty != null) return false;
    } else if (!nonEmpty.equals(other.nonEmpty)) {
      return false;
    }
    return true;
  }
  
  /**
   * Test if two index datas contain the same documents, ignoring documents
   * order.
   */
  public static boolean equalsNoOrder(IndexData id1, IndexData id2) {
    if (id1.nonEmpty == null) {
      if (id2.nonEmpty != null) return false;
    } else if (id2.nonEmpty == null) {
      return false;
    }

    ArrayList<DocumentData> sorted1 = new ArrayList<IndexData.DocumentData>(
        id1.nonEmpty);
    ArrayList<DocumentData> sorted2 = new ArrayList<IndexData.DocumentData>(
        id2.nonEmpty);

    Collections.sort(sorted1, new DocumentDataComparator());
    Collections.sort(sorted2, new DocumentDataComparator());
    
    Iterator<DocumentData> iterator1 = sorted1.iterator();
    Iterator<DocumentData> iterator2 = sorted2.iterator();
    
    while (iterator1.hasNext()) {
      if (!iterator2.hasNext()) {
        return false;
      }
      if (!iterator1.next().equals(iterator2.next())) {
        return false;
      }
    }
    
    return true;
  }

  public static boolean testEquality(ArrayList<DocumentData> sorted1,
      ArrayList<DocumentData> sorted2) {
    Iterator<DocumentData> iterator1 = sorted1.iterator();
    Iterator<DocumentData> iterator2 = sorted2.iterator();
    
    while (iterator1.hasNext()) {
      if (!iterator2.hasNext()) {
        return false;
      }
      if (!iterator1.next().equals(iterator2.next())) {
        return false;
      }
    }
    
    return true;
  }
  
  private class DocumentData {
    
    List<StorableField> storedFields;
    
    TreeMap<String,Integer> termsMap = new TreeMap<String,Integer>();
    
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((storedFields == null) ? 0 : storedFields.hashCode());
      result = prime * result + ((termsMap == null) ? 0 : termsMap.hashCode());
      return result;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      DocumentData other = (DocumentData) obj;
      if (storedFields == null) {
        if (other.storedFields != null) {
          return false;
        }
      } else {
        // compare stored fields on by one
        if (storedFields.size() != other.storedFields.size()) {
          return false;
        }
        Iterator<StorableField> fieldsThis = storedFields.iterator();
        Iterator<StorableField> fieldsOther = other.storedFields.iterator();
        while (fieldsThis.hasNext()) {
          StorableField fieldThis = fieldsThis.next();
          StorableField fieldOther = fieldsOther.next();
          if (!fieldThis.name().equals(fieldOther.name())) {
            return false;
          }
          if (!fieldThis.stringValue().equals(fieldOther.stringValue())) {
            return false;
          }
        }
      }
      if (termsMap == null) {
        if (other.termsMap != null) {
          return false;
        }
      } else if (!termsMap.equals(other.termsMap)) {
        return false;
      }
      return true;
    }
    
    public DocumentData(StoredDocument document) {
      storedFields = document.getFields();
      Collections.sort(storedFields, new StorableFieldsComparator());
    }
    
    public boolean isEmpty() {
      return storedFields.isEmpty() && termsMap.isEmpty();
    }
    
    public void addTerm(String field, String term, int freq) {
      termsMap.put(field + ":" + term, freq);
    }
    
  }
  
  private static class DocumentDataComparator implements Comparator<DocumentData> {
    
    @Override
    public int compare(DocumentData doc1, DocumentData doc2) {
      // start with index terms
      Map<String,Integer> terms1 = doc1.termsMap;
      Map<String,Integer> terms2 = doc2.termsMap;
      if (terms1.isEmpty() && !terms2.isEmpty()) {
        return -1;
      } else if (!terms1.isEmpty() && terms2.isEmpty()) {
        return 1;
      } else {
        Iterator<String> iter1 = terms1.keySet().iterator();
        Iterator<String> iter2 = terms2.keySet().iterator();
        while (iter1.hasNext()) {
          if (!iter2.hasNext()) {
            return -1;
          } else {
            String t1 = iter1.next();
            String t2 = iter2.next();
            int compTerm = t1.compareTo(t2);
            if (compTerm != 0) {
              return compTerm;
            }
            int compFreq = terms1.get(t1) - terms2.get(t2);
            if (compFreq != 0) {
              return compFreq;
            }
          }
        }
        if (iter2.hasNext()) {
          return 1;
        }
      }
      
      // now check stored fields
      List<StorableField> stored1 = doc1.storedFields;
      List<StorableField> stored2 = doc2.storedFields;
      if (stored1.isEmpty() && !stored2.isEmpty()) {
        return -1;
      } else if (!stored1.isEmpty() && stored2.isEmpty()) {
        return 1;
      } else {
        Iterator<StorableField> iter1 = stored1.iterator();
        Iterator<StorableField> iter2 = stored2.iterator();
        while (iter1.hasNext()) {
          if (!iter2.hasNext()) {
            return -1;
          } else {
            StorableField f1 = iter1.next();
            StorableField f2 = iter2.next();
            int compFieldName = f1.name().compareTo(f2.name());
            if (compFieldName != 0) {
              return compFieldName;
            }
            compFieldName = f1.stringValue().compareTo(f2.stringValue());
            if (compFieldName != 0) {
              return compFieldName;
            }
          }
        }
        if (iter2.hasNext()) {
          return 1;
        }
      }
      return 0;
      
    }
  }
  
  private class StorableFieldsComparator implements Comparator<StorableField> {

    @Override
    public int compare(StorableField f0, StorableField f1) {
      return f0.stringValue().compareTo(f1.stringValue());
    }
    
  }
}

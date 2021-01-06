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

package org.apache.lucene.luke.models.util.twentynewsgroups;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.email.UAX29URLEmailAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;

/** Data holder class for a newsgroups message */
public class Message {

  private String from;
  private String[] newsgroups;
  private String subject;
  private String messageId;
  private String date;
  private String organization;
  private String body;
  private int lines;

  public String getFrom() {
    return from;
  }

  public void setFrom(String from) {
    this.from = from;
  }

  public String[] getNewsgroups() {
    return newsgroups;
  }

  public void setNewsgroups(String[] newsgroups) {
    this.newsgroups = newsgroups;
  }

  public String getSubject() {
    return subject;
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }

  public String getMessageId() {
    return messageId;
  }

  public void setMessageId(String messageId) {
    this.messageId = messageId;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public String getOrganization() {
    return organization;
  }

  public void setOrganization(String organization) {
    this.organization = organization;
  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = body;
  }

  public int getLines() {
    return lines;
  }

  public void setLines(int lines) {
    this.lines = lines;
  }

  public Document toLuceneDoc() {
    Document doc = new Document();

    if (Objects.nonNull(getFrom())) {
      doc.add(new TextField("from", getFrom(), Field.Store.YES));
    }

    if (Objects.nonNull(getNewsgroups())) {
      for (String newsgroup : getNewsgroups()) {
        doc.add(new StringField("newsgroup", newsgroup, Field.Store.YES));
        doc.add(new SortedSetDocValuesField("newsgroup_sort", new BytesRef(newsgroup)));
      }
    }

    if (Objects.nonNull(getSubject())) {
      doc.add(new Field("subject", getSubject(), SUBJECT_FIELD_TYPE));
    }

    if (Objects.nonNull(getMessageId())) {
      doc.add(new StringField("messageId", getMessageId(), Field.Store.YES));
    }

    if (Objects.nonNull(getDate())) {
      doc.add(new StoredField("date_raw", getDate()));
    }

    if (getOrganization() != null) {
      doc.add(new TextField("organization", getOrganization(), Field.Store.YES));
    }

    doc.add(new IntPoint("lines_range", getLines()));
    doc.add(new SortedNumericDocValuesField("lines_sort", getLines()));
    doc.add(new StoredField("lines_raw", String.valueOf(getLines())));

    if (Objects.nonNull(getBody())) {
      doc.add(new Field("body", getBody(), BODY_FIELD_TYPE));
    }

    return doc;
  }

  public static Analyzer createLuceneAnalyzer() {
    Map<String, Analyzer> map = new HashMap<>();
    map.put("from", new UAX29URLEmailAnalyzer());
    return new PerFieldAnalyzerWrapper(new StandardAnalyzer(), map);
  }

  private static final FieldType SUBJECT_FIELD_TYPE;

  private static final FieldType BODY_FIELD_TYPE;

  static {
    SUBJECT_FIELD_TYPE = new FieldType();
    SUBJECT_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    SUBJECT_FIELD_TYPE.setTokenized(true);
    SUBJECT_FIELD_TYPE.setStored(true);

    BODY_FIELD_TYPE = new FieldType();
    BODY_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    BODY_FIELD_TYPE.setTokenized(true);
    BODY_FIELD_TYPE.setStored(true);
    BODY_FIELD_TYPE.setStoreTermVectors(true);
    BODY_FIELD_TYPE.setStoreTermVectorPositions(true);
    BODY_FIELD_TYPE.setStoreTermVectorOffsets(true);
  }
}

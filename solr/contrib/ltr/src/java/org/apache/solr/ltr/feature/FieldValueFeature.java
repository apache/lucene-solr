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
package org.apache.solr.ltr.feature;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * This feature returns the value of a field in the current document.
 * The field must have stored="true" or docValues="true" properties.
 * Example configuration:
 * <pre>{
  "name":  "rawHits",
  "class": "org.apache.solr.ltr.feature.FieldValueFeature",
  "params": {
      "field": "hits"
  }
}</pre>
 *
 * <p>There are 4 different types of FeatureScorers that a FieldValueFeatureWeight may use.
 * The chosen scorer depends on the field attributes.</p>
 *
 * <p>FieldValueFeatureScorer (FVFS): used for stored=true, no matter if docValues=true or docValues=false</p>
 *
 * <p>NumericDocValuesFVFS: used for stored=false and docValues=true, if docValueType == NUMERIC</p>
 * <p>SortedDocValuesFVFS: used for stored=false and docValues=true, if docValueType == SORTED
 *
 * <p>DefaultValueFVFS: used for stored=false and docValues=true, a fallback scorer that is used on segments
 * where no document has a value set in the field of this feature</p>
 */
public class FieldValueFeature extends Feature {

  private String field;
  private Set<String> fieldAsSet;

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
    fieldAsSet = Collections.singleton(field);
  }

  @Override
  public LinkedHashMap<String,Object> paramsToMap() {
    final LinkedHashMap<String,Object> params = defaultParamsToMap();
    params.put("field", field);
    return params;
  }

  @Override
  protected void validate() throws FeatureException {
    if (field == null || field.isEmpty()) {
      throw new FeatureException(getClass().getSimpleName()+
          ": field must be provided");
    }
  }

  public FieldValueFeature(String name, Map<String,Object> params) {
    super(name, params);
  }

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores,
      SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi)
          throws IOException {
    return new FieldValueFeatureWeight(searcher, request, originalQuery, efi);
  }

  public class FieldValueFeatureWeight extends FeatureWeight {
    private final SchemaField schemaField;

    public FieldValueFeatureWeight(IndexSearcher searcher,
        SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi) {
      super(FieldValueFeature.this, searcher, request, originalQuery, efi);
      if (searcher instanceof SolrIndexSearcher) {
        schemaField = ((SolrIndexSearcher) searcher).getSchema().getFieldOrNull(field);
      } else { // some tests pass a null or a non-SolrIndexSearcher searcher
        schemaField = null;
      }
    }

    /**
     * Return a FeatureScorer that uses docValues or storedFields if no docValues are present
     *
     * @param context the segment this FeatureScorer is working with
     * @return FeatureScorer for the current segment and field
     * @throws IOException as defined by abstract class Feature
     */
    @Override
    public FeatureScorer scorer(LeafReaderContext context) throws IOException {
      if (schemaField != null && !schemaField.stored() && schemaField.hasDocValues()) {

        final FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(field);
        final DocValuesType docValuesType = fieldInfo != null ? fieldInfo.getDocValuesType() : DocValuesType.NONE;

        if (DocValuesType.NUMERIC.equals(docValuesType)) {
          return new NumericDocValuesFieldValueFeatureScorer(this, context,
                  DocIdSetIterator.all(DocIdSetIterator.NO_MORE_DOCS), schemaField.getType().getNumberType());
        } else if (DocValuesType.SORTED.equals(docValuesType)) {
          return new SortedDocValuesFieldValueFeatureScorer(this, context,
                  DocIdSetIterator.all(DocIdSetIterator.NO_MORE_DOCS));
        } else if (DocValuesType.NONE.equals(docValuesType)) {
          // Using a fallback feature scorer because this segment has no documents with a doc value for the current field
          return new DefaultValueFieldValueFeatureScorer(this, DocIdSetIterator.all(DocIdSetIterator.NO_MORE_DOCS));
        }
        throw new IllegalArgumentException("Doc values type " + docValuesType.name() + " of field " + field
                + " is not supported");
      }
      return new FieldValueFeatureScorer(this, context,
          DocIdSetIterator.all(DocIdSetIterator.NO_MORE_DOCS));
    }

    /**
     * A FeatureScorer that reads the stored value for a field
     */
    public class FieldValueFeatureScorer extends FeatureScorer {

      LeafReaderContext context = null;

      public FieldValueFeatureScorer(FeatureWeight weight,
          LeafReaderContext context, DocIdSetIterator itr) {
        super(weight, itr);
        this.context = context;
      }

      @Override
      public float score() throws IOException {

        try {
          final Document document = context.reader().document(itr.docID(),
              fieldAsSet);
          final IndexableField indexableField = document.getField(field);
          if (indexableField == null) {
            return getDefaultValue();
          }
          final Number number = indexableField.numericValue();
          if (number != null) {
            return number.floatValue();
          } else {
            final String string = indexableField.stringValue();
            if (string.length() == 1) {
              // boolean values in the index are encoded with the
              // a single char contained in TRUE_TOKEN or FALSE_TOKEN
              // (see BoolField)
              if (string.charAt(0) == BoolField.TRUE_TOKEN[0]) {
                return 1;
              }
              if (string.charAt(0) == BoolField.FALSE_TOKEN[0]) {
                return 0;
              }
            }
          }
        } catch (final IOException e) {
          throw new FeatureException(
              e.toString() + ": " +
                  "Unable to extract feature for "
                  + name, e);
        }
        return getDefaultValue();
      }

      @Override
      public float getMaxScore(int upTo) throws IOException {
        return Float.POSITIVE_INFINITY;
      }
    }

    /**
     * A FeatureScorer that reads the numeric docValues for a field
     */
    public final class NumericDocValuesFieldValueFeatureScorer extends FeatureScorer {
      private final NumericDocValues docValues;
      private final NumberType numberType;

      public NumericDocValuesFieldValueFeatureScorer(final FeatureWeight weight, final LeafReaderContext context,
                                              final DocIdSetIterator itr, final NumberType numberType) {
        super(weight, itr);
        this.numberType = numberType;

        NumericDocValues docValues;
        try {
          docValues = DocValues.getNumeric(context.reader(), field);
        } catch (IOException e) {
          throw new IllegalArgumentException("Could not read numeric docValues for field " + field);
        }
        this.docValues = docValues;
      }

      @Override
      public float score() throws IOException {
        if (docValues.advanceExact(itr.docID())) {
          return readNumericDocValues();
        }
        return FieldValueFeature.this.getDefaultValue();
      }

      /**
       * Read the numeric value for a field and convert the different number types to float.
       *
       * @return The numeric value that the docValues contain for the current document
       * @throws IOException if docValues cannot be read
       */
      private float readNumericDocValues() throws IOException {
        if (NumberType.FLOAT.equals(numberType)) {
          // convert float value that was stored as long back to float
          return Float.intBitsToFloat((int) docValues.longValue());
        } else if (NumberType.DOUBLE.equals(numberType)) {
          // handle double value conversion
          return (float) Double.longBitsToDouble(docValues.longValue());
        }
        // just take the long value
        return docValues.longValue();
      }

      @Override
      public float getMaxScore(int upTo) throws IOException {
        return Float.POSITIVE_INFINITY;
      }
    }

    /**
     * A FeatureScorer that reads the sorted docValues for a field
     */
    public final class SortedDocValuesFieldValueFeatureScorer extends FeatureScorer {
      private final SortedDocValues docValues;

      public SortedDocValuesFieldValueFeatureScorer(final FeatureWeight weight, final LeafReaderContext context,
                                              final DocIdSetIterator itr) {
        super(weight, itr);

        SortedDocValues docValues;
        try {
          docValues = DocValues.getSorted(context.reader(), field);
        } catch (IOException e) {
          throw new IllegalArgumentException("Could not read sorted docValues for field " + field);
        }
        this.docValues = docValues;
      }

      @Override
      public float score() throws IOException {
        if (docValues.advanceExact(itr.docID())) {
          int ord = docValues.ordValue();
          return readSortedDocValues(docValues.lookupOrd(ord));
        }
        return FieldValueFeature.this.getDefaultValue();
      }

      /**
       * Interprets the bytesRef either as true / false token or tries to read it as number string
       *
       * @param bytesRef the value of the field that should be used as score
       * @return the input converted to a number
       */
      private float readSortedDocValues(BytesRef bytesRef) {
        String string = bytesRef.utf8ToString();
        if (string.length() == 1) {
          // boolean values in the index are encoded with the
          // a single char contained in TRUE_TOKEN or FALSE_TOKEN
          // (see BoolField)
          if (string.charAt(0) == BoolField.TRUE_TOKEN[0]) {
            return 1;
          }
          if (string.charAt(0) == BoolField.FALSE_TOKEN[0]) {
            return 0;
          }
        }
        return FieldValueFeature.this.getDefaultValue();
      }

      @Override
      public float getMaxScore(int upTo) throws IOException {
        return Float.POSITIVE_INFINITY;
      }
    }

    /**
     * A FeatureScorer that always returns the default value.
     *
     * It is used as a fallback for cases when a segment does not have any documents that contain doc values for a field.
     * By doing so, we prevent a fallback to the FieldValueFeatureScorer, which would also return the default value but
     * in a less performant way because it would first try to read the stored fields for the doc (which aren't present).
     */
    public final class DefaultValueFieldValueFeatureScorer extends FeatureScorer {
      public DefaultValueFieldValueFeatureScorer(final FeatureWeight weight, final DocIdSetIterator itr) {
        super(weight, itr);
      }

      @Override
      public float score() throws IOException {
        return FieldValueFeature.this.getDefaultValue();
      }

      @Override
      public float getMaxScore(int upTo) throws IOException {
        return Float.POSITIVE_INFINITY;
      }
    }
  }
}

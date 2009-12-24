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

package org.apache.solr.schema;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.TextResponseWriter;
import org.apache.solr.request.XMLWriter;
import org.apache.solr.search.MultiValueSource;
import org.apache.solr.search.QParser;
import org.apache.solr.search.function.DocValues;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.distance.DistanceUtils;

import java.io.IOException;
import java.util.Map;

/**
 * A point type that indexes a point in an n-dimensional space as separate fields and uses
 * range queries for bounding box calculations.
 * <p/>
 * <p/>
 * NOTE: There can only be one sub type
 */
public class PointType extends CoordinateFieldType {
  /**
   * 2 dimensional by default
   */
  public static final int DEFAULT_DIMENSION = 2;
  public static final String DIMENSION = "dimension";

  protected IndexSchema schema;   // needed for retrieving SchemaFields


  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    SolrParams p = new MapSolrParams(args);
    dimension = p.getInt(DIMENSION, DEFAULT_DIMENSION);
    if (dimension < 1) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "The dimension must be > 0: " + dimension);
    }
    args.remove(DIMENSION);
    this.schema = schema;
    super.init(schema, args);

  }


  @Override
  public boolean isPolyField() {
    return true;   // really only true if the field is indexed
  }

  @Override
  public Fieldable[] createFields(SchemaField field, String externalVal, float boost) {
    String[] point = DistanceUtils.parsePoint(null, externalVal, dimension);
    return createFields(field, dynFieldProps, subType, externalVal, boost, point);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    return new PointTypeValueSource(field, dimension, subType, parser);
  }


  //It never makes sense to create a single field, so make it impossible to happen
  @Override
  public Field createField(SchemaField field, String externalVal, float boost) {
    throw new UnsupportedOperationException("PointType uses multiple fields.  field=" + field.getName());
  }

  @Override
  public void write(XMLWriter xmlWriter, String name, Fieldable f) throws IOException {
    xmlWriter.writeStr(name, f.stringValue());
  }

  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException {
    writer.writeStr(name, f.stringValue(), false);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Sorting not suported on DualPointType " + field.getName());
  }

  @Override
  /**
   * Care should be taken in calling this with higher order dimensions for performance reasons.
   */
  public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    //Query could look like: [x1,y1 TO x2,y2] for 2 dimension, but could look like: [x1,y1,z1 TO x2,y2,z2], and can be extrapolated to n-dimensions
    //thus, this query essentially creates a box, cube, etc.
    String[] p1 = DistanceUtils.parsePoint(null, part1, dimension);
    String[] p2 = DistanceUtils.parsePoint(null, part2, dimension);
    BooleanQuery result = new BooleanQuery(true);
    String name = field.getName() + "_";
    String suffix = POLY_FIELD_SEPARATOR + subType.typeName;
    int len = name.length();
    StringBuilder bldr = new StringBuilder(len + 3 + suffix.length());//should be enough buffer to handle most values of j.
    bldr.append(name);
    for (int i = 0; i < dimension; i++) {
      bldr.append(i).append(suffix);
      SchemaField subSF = schema.getField(bldr.toString());
      // points must currently be ordered... should we support specifying any two opposite corner points?

      /*new TermRangeQuery(
     field.getName() + i + POLY_FIELD_SEPARATOR + subType.typeName,
     subType.toInternal(p1[i]),
     subType.toInternal(p2[i]),
     minInclusive, maxInclusive);*/
      result.add(subType.getRangeQuery(parser, subSF, p1[i], p2[i], minInclusive, maxInclusive), BooleanClause.Occur.MUST);
      bldr.setLength(len);
    }
    return result;
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    Query result = null;

    String[] p1 = DistanceUtils.parsePoint(null, externalVal, dimension);
    //TODO: should we assert that p1.length == dimension?
    BooleanQuery bq = new BooleanQuery(true);
    String name = field.getName() + "_";
    String suffix = POLY_FIELD_SEPARATOR + subType.typeName;
    int len = name.length();
    StringBuilder bldr = new StringBuilder(len + 3 + suffix.length());//should be enough buffer to handle most values of j.
    bldr.append(name);
    for (int i = 0; i < dimension; i++) {
      bldr.append(i).append(suffix);
      SchemaField sf1 = schema.getField(bldr.toString());
      Query tq = subType.getFieldQuery(parser, sf1, p1[i]);
      //new TermQuery(new Term(bldr.toString(), subType.toInternal(p1[i])));
      bq.add(tq, BooleanClause.Occur.MUST);
      bldr.setLength(len);
    }
    result = bq;

    return result;
  }

  class PointTypeValueSource extends MultiValueSource {
    protected SchemaField field;
    protected FieldType subType;
    protected int dimension;
    private QParser parser;

    public PointTypeValueSource(SchemaField field, int dimension, FieldType subType, QParser parser) {
      this.field = field;
      this.dimension = dimension;
      this.subType = subType;
      this.parser = parser;
    }

    @Override
    public void createWeight(Map context, Searcher searcher) throws IOException {
      String name = field.getName();
      String suffix = FieldType.POLY_FIELD_SEPARATOR + subType.typeName;
      int len = name.length();
      StringBuilder bldr = new StringBuilder(len + 3 + suffix.length());//should be enough buffer to handle most values of j.
      bldr.append(name);
      for (int i = 0; i < dimension; i++) {
        bldr.append(i).append(suffix);
        SchemaField sf = schema.getField(bldr.toString());
        subType.getValueSource(sf, parser).createWeight(context, searcher);
        bldr.setLength(len);
      }
    }

    public int dimension() {
      return dimension;
    }

    @Override
    public DocValues getValues(Map context, IndexReader reader) throws IOException {
      final DocValues[] valsArr1 = new DocValues[dimension];
      String name = field.getName();
      String suffix = FieldType.POLY_FIELD_SEPARATOR + subType.typeName;
      int len = name.length();
      StringBuilder bldr = new StringBuilder(len + 3 + suffix.length());//should be enough buffer to handle most values of j.
      bldr.append(name);
      for (int i = 0; i < dimension; i++) {
        bldr.append(i).append(suffix);
        SchemaField sf = schema.getField(bldr.toString());
        valsArr1[i] = subType.getValueSource(sf, parser).getValues(context, reader);
        bldr.setLength(len);
      }
      return new DocValues() {
        //TODO: not sure how to handle the other types at this moment
        @Override
        public void doubleVal(int doc, double[] vals) {
          //TODO: check whether vals.length == dimension or assume its handled elsewhere?
          for (int i = 0; i < dimension; i++) {
            vals[i] = valsArr1[i].doubleVal(doc);
          }
        }


        @Override
        public String toString(int doc) {
          StringBuilder sb = new StringBuilder("point(");
          boolean firstTime = true;
          for (DocValues docValues : valsArr1) {
            if (firstTime == false) {
              sb.append(",");
            } else {
              firstTime = true;
            }
            sb.append(docValues.toString(doc));
          }
          sb.append(")");
          return sb.toString();
        }
      };
    }

    public String description() {
      StringBuilder sb = new StringBuilder();
      sb.append("point(");
      sb.append("fld=").append(field.name).append(", subType=").append(subType.typeName)
              .append(", dimension=").append(dimension).append(')');
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof PointTypeValueSource)) return false;

      PointTypeValueSource that = (PointTypeValueSource) o;

      if (dimension != that.dimension) return false;
      if (!field.equals(that.field)) return false;
      if (!subType.equals(that.subType)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = field.hashCode();
      result = 31 * result + subType.hashCode();
      result = 31 * result + dimension;
      return result;
    }
  }

}



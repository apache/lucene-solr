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
package org.apache.solr.schema;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.NRShape;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.DateMathParser;
import org.locationtech.spatial4j.shape.Shape;

/**
 * A field for indexed dates and date ranges. It's mostly compatible with DatePointField.  It has the potential to allow
 * efficient faceting, similar to facet.enum.
 *
 * @see NumberRangePrefixTreeStrategy
 * @see DateRangePrefixTree
 */
public class DateRangeField extends AbstractSpatialPrefixTreeFieldType<NumberRangePrefixTreeStrategy>
  implements DateValueFieldType { // used by ParseDateFieldUpdateProcessorFactory

  private static final String OP_PARAM = "op";//local-param to resolve SpatialOperation

  private static final DateRangePrefixTree tree = new DateRangePrefixTree(DateRangePrefixTree.JAVA_UTIL_TIME_COMPAT_CAL);

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
  }

  @Override
  protected NumberRangePrefixTreeStrategy newPrefixTreeStrategy(String fieldName) {
    return new NumberRangePrefixTreeStrategy(tree, fieldName);
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object val) {
    if (val instanceof Date || val instanceof Calendar)//From URP?
      val = tree.toUnitShape(val);
    return super.createFields(field, val);
  }

  @Override
  protected String getStoredValue(Shape shape, String shapeStr) {
    // even if shapeStr is set, it might have included some dateMath, so see if we can resolve it first:
    if (shape instanceof UnitNRShape) {
      UnitNRShape unitShape = (UnitNRShape) shape;
      if (unitShape.getLevel() == tree.getMaxLevels()) {
        //fully precise date. We can be fully compatible with DatePointField (incl. 'Z')
        return shape.toString() + 'Z';
      }
    }
    return (shapeStr == null ? shape.toString() : shapeStr);//we don't normalize ranges here; should we?
  }

  // Won't be called because we override getStoredValue? any way; easy to implement in terms of that
  @Override
  public String shapeToString(Shape shape) {
    return getStoredValue(shape, null);
  }


  @Override
  public NRShape parseShape(String str) {
    if (str.contains(" TO ")) {
      //TODO parsing range syntax doesn't support DateMath on either side or exclusive/inclusive
      try {
        return tree.parseShape(str);
      } catch (ParseException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Couldn't parse date because: "+ e.getMessage(), e);
      }
    } else {
      return tree.toShape(parseCalendar(str));
    }
  }

  private Calendar parseCalendar(String str) {
    if (str.startsWith("NOW") || str.lastIndexOf('Z') >= 0) { //  ? but not if Z is last char ?   Ehh, whatever.
      //use Solr standard date format parsing rules:
      //TODO add DMP utility to return ZonedDateTime alternative, then set cal fields manually, which is faster?
      Date date = DateMathParser.parseMath(null, str);
      Calendar cal = tree.newCal();
      cal.setTime(date);
      return cal;
    } else {
      try {
        return tree.parseCalendar(str);
      } catch (ParseException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Couldn't parse date because: "+ e.getMessage(), e);
      }

    }
  }

  /** For easy compatibility with {@link DateMathParser#parseMath(Date, String)}. */
  public Date parseMath(Date now, String rawval) {
    return DateMathParser.parseMath(now, rawval);
  }

  @Override
  protected SpatialArgs parseSpatialArgs(QParser parser, String externalVal) {
    //We avoid SpatialArgsParser entirely because it isn't very Solr-friendly
    final Shape shape = parseShape(externalVal);
    final SolrParams localParams = parser.getLocalParams();
    SpatialOperation op = SpatialOperation.Intersects;
    if (localParams != null) {
      String opStr = localParams.get(OP_PARAM);
      if (opStr != null)
        op = SpatialOperation.get(opStr);
    }
    return new SpatialArgs(op, shape);
  }

  @Override
  protected Query getSpecializedRangeQuery(QParser parser, SchemaField field, String startStr, String endStr, boolean minInclusive, boolean maxInclusive) {
    if (parser == null) {//null when invoked by SimpleFacets.  But getQueryFromSpatialArgs expects to get localParams.
      final SolrRequestInfo requestInfo = SolrRequestInfo.getRequestInfo();
      parser = new QParser("", null, requestInfo.getReq().getParams(), requestInfo.getReq()) {
        @Override
        public Query parse() throws SyntaxError {
          throw new IllegalStateException();
        }
      };
    }

    Calendar startCal;
    if (startStr == null) {
      startCal = tree.newCal();
    } else {
      startCal = parseCalendar(startStr);
      if (!minInclusive) {
        startCal.add(Calendar.MILLISECOND, 1);
      }
    }
    Calendar endCal;
    if (endStr == null) {
      endCal = tree.newCal();
    } else {
      endCal = parseCalendar(endStr);
      if (!maxInclusive) {
        endCal.add(Calendar.MILLISECOND, -1);
      }
    }
    Shape shape = tree.toRangeShape(tree.toShape(startCal), tree.toShape(endCal));
    SpatialArgs spatialArgs = new SpatialArgs(SpatialOperation.Intersects, shape);
    return getQueryFromSpatialArgs(parser, field, spatialArgs);
  }

}

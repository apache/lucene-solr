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
import java.util.Iterator;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.spatial.ShapeValues;
import org.apache.lucene.spatial.ShapeValuesSource;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.JSONResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.schema.AbstractSpatialFieldType;
import org.apache.solr.schema.SchemaField;
import org.locationtech.spatial4j.io.GeoJSONWriter;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.io.SupportedFormats;
import org.locationtech.spatial4j.shape.Shape;


/**
 * This DocumentTransformer will write a {@link Shape} to the SolrDocument using
 * the requested format.  Supported formats include:
 * <ul>
 *  <li>GeoJSON</li>
 *  <li>WKT</li>
 *  <li>Polyshape</li>
 * </ul>
 * For more information see: <a href="https://github.com/locationtech/spatial4j/blob/master/FORMATS.md">spatial4j/FORMATS.md</a>
 * 
 * The shape is either read from a stored field, or a ValueSource.
 * 
 * This transformer is useful when:
 * <ul>
 *  <li>You want to return a format different than the stored encoding (WKT vs GeoJSON)</li>
 *  <li>The {@link Shape} is stored in a {@link ValueSource}, not a stored field</li>
 *  <li>the value is not stored in a format the output understands (ie, raw GeoJSON)</li>
 * </ul>
 * 
 */
public class GeoTransformerFactory extends TransformerFactory
{ 
  @Override
  public DocTransformer create(String display, SolrParams params, SolrQueryRequest req) {

    String fname = params.get("f", display);
    if(fname.startsWith("[") && fname.endsWith("]")) {
      fname = display.substring(1,display.length()-1);
    }
    SchemaField sf = req.getSchema().getFieldOrNull(fname);
    if(sf==null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, 
          this.getClass().getSimpleName() +" using unknown field: "+fname);
    }
    if(!(sf.getType() instanceof AbstractSpatialFieldType)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, 
          "GeoTransformer requested non-spatial field: "+fname + " ("+sf.getType().getClass().getSimpleName()+")");
    }

    final GeoFieldUpdater updater = new GeoFieldUpdater();
    updater.field = fname;
    updater.display = display;
    updater.display_error = display+"_error"; 
        
    final ShapeValuesSource shapes;
    AbstractSpatialFieldType<?> sdv = (AbstractSpatialFieldType<?>)sf.getType();
    SpatialStrategy strategy = sdv.getStrategy(fname);
    if(strategy instanceof CompositeSpatialStrategy) {
      shapes = ((CompositeSpatialStrategy)strategy)
          .getGeometryStrategy().makeShapeValueSource();
    }
    else if(strategy instanceof SerializedDVStrategy) {
      shapes = ((SerializedDVStrategy)strategy)
          .makeShapeValueSource();
    }
    else
      shapes = null;
    
    
    String writerName = params.get("w", "GeoJSON");
    updater.formats = strategy.getSpatialContext().getFormats();
    updater.writer = updater.formats.getWriter(writerName);
    if(updater.writer==null) {
      StringBuilder str = new StringBuilder();
      str.append( "Unknown Spatial Writer: " ).append(writerName);
      str.append(" [");
      for(ShapeWriter w : updater.formats.getWriters()) {
        str.append(w.getFormatName()).append(' ');
      }
      str.append("]");
      throw new SolrException(ErrorCode.BAD_REQUEST, str.toString());
    }
    
    QueryResponseWriter qw = req.getCore().getQueryResponseWriter(req);
    updater.isJSON =
        (qw.getClass() == JSONResponseWriter.class) &&
        (updater.writer instanceof GeoJSONWriter);


    // Using ValueSource
    if(shapes!=null) {
      return new DocTransformer() {
        @Override
        public String getName() {
          return display;
        }

        @Override
        public void transform(SolrDocument doc, int docid) throws IOException {
          int leafOrd = ReaderUtil.subIndex(docid, context.getSearcher().getTopReaderContext().leaves());
          LeafReaderContext ctx = context.getSearcher().getTopReaderContext().leaves().get(leafOrd);
          ShapeValues values = shapes.getValues(ctx);
          int segmentDoc = docid - ctx.docBase;
          if (values.advanceExact(segmentDoc)) {
            updater.setValue(doc, values.value());
          }
        }
      };

    }
    
    // Using the raw stored values
    return new DocTransformer() {
      
      @Override
      public void transform(SolrDocument doc, int docid) throws IOException {
        Object val = doc.remove(updater.field);
        if(val!=null) {
          updater.setValue(doc, val);
        }
      }
      
      @Override
      public String getName() {
        return updater.display;
      }

      @Override
      public String[] getExtraRequestFields() {
        return new String[] {updater.field};
      }
    };
  }

}

class GeoFieldUpdater {
  String field;
  String display;
  String display_error;
  
  boolean isJSON;
  ShapeWriter writer;
  SupportedFormats formats;
  
  void addShape(SolrDocument doc, Shape shape) {
    if(isJSON) {
      doc.addField(display, new WriteableGeoJSON(shape, writer));
    }
    else {
      doc.addField(display, writer.toString(shape));
    }
  }
  
  void setValue(SolrDocument doc, Object val) {
    doc.remove(display);
    if(val != null) {
      if(val instanceof Iterable) {
        @SuppressWarnings({"rawtypes"})
        Iterator iter = ((Iterable)val).iterator();
        while(iter.hasNext()) {
          addValue(doc, iter.next());
        }
      }
      else {
        addValue(doc, val);
      }
    }
  }
    
  void addValue(SolrDocument doc, Object val) {
    if(val == null) {
      return;
    }
    
    if(val instanceof Shape) {
      addShape(doc, (Shape)val);
    }
    // Don't explode on 'InvalidShpae'
    else if( val instanceof Exception) {
      doc.setField( display_error, ((Exception)val).toString() );
    }
    else {
      // Use the stored value
      if(val instanceof IndexableField) {
        val = ((IndexableField)val).stringValue();
      }
      try {
        addShape(doc, formats.read(val.toString()));
      }
      catch(Exception ex) {
        doc.setField( display_error, ex.toString() );
      }
    }
  }
}


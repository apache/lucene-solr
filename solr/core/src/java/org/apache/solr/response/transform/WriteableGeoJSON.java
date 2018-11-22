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

import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.TextWriter;
import org.apache.solr.common.util.WriteableValue;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.shape.Shape;

/**
 * This will let the writer add values to the response directly
 */
public class WriteableGeoJSON extends WriteableValue {

  public final Shape shape;
  public final ShapeWriter jsonWriter;
  
  public WriteableGeoJSON(Shape shape, ShapeWriter jsonWriter) {
    this.shape = shape;
    this.jsonWriter = jsonWriter;
  }

  @Override
  public Object resolve(Object o, JavaBinCodec codec) throws IOException {
    codec.writeStr(jsonWriter.toString(shape));
    return null; // this means we wrote it
  }

  @Override
  public void write(String name, TextWriter writer) throws IOException {
    jsonWriter.write(writer.getWriter(), shape);
  }

  @Override
  public String toString() {
    return jsonWriter.toString(shape);
  }
}

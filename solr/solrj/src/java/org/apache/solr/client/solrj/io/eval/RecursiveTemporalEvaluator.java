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
package org.apache.solr.client.solrj.io.eval;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public abstract class RecursiveTemporalEvaluator extends RecursiveEvaluator implements OneValueWorker {
  protected static final long serialVersionUID = 1L;
  
  private String functionName;
  
  public RecursiveTemporalEvaluator(StreamExpression expression, StreamFactory factory, String functionName) throws IOException{
    super(expression, factory);
    this.functionName = functionName;
    
    if(1 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting one value but found %d",expression,containedEvaluators.size()));
    }
  }
  
  public String getFunction() {
    return functionName;
  }
  
  protected abstract Object getDatePart(TemporalAccessor value);
    
  public Object normalizeInputType(Object value) throws StreamEvaluatorException {
    if(null == value){
      return value;
    }
    
    // There is potential for various types to be coming in which are valid temporal values.
    // The order in which we check these types is actually somewhat critical in how we handle them.
    
    if(value instanceof Instant){
      // Convert to LocalDateTime at UTC
      return LocalDateTime.ofInstant((Instant)value, ZoneOffset.UTC);
    }
    else if(value instanceof TemporalAccessor){
      // Any other TemporalAccessor can just be returned
      return value;
    }
    else if(value instanceof Long){
      // Convert to Instant and recurse in
      return normalizeInputType(Instant.ofEpochMilli((Long)value));
    }
    else if(value instanceof Date){
      // Convert to Instant and recurse in
      return normalizeInputType(((Date)value).toInstant());
    }
    else if(value instanceof String){
      String valueStr = (String)value;      
      if (!valueStr.isEmpty()) {
        try {
          // Convert to Instant and recurse in
          return normalizeInputType(Instant.parse(valueStr));
        } catch (DateTimeParseException e) {
          throw new UncheckedIOException(new IOException(String.format(Locale.ROOT, "Invalid parameter %s - The String must be formatted in the ISO_INSTANT date format.", valueStr)));
        }
      }
    }
    else if(value instanceof List){
      // for each list value, recurse in
      return ((List<?>)value).stream().map(innerValue -> normalizeInputType(innerValue)).collect(Collectors.toList());
    }

    throw new UncheckedIOException(new IOException(String.format(Locale.ROOT, "Invalid parameter %s - The parameter must be a string formatted ISO_INSTANT or of type Long,Instant,Date,LocalDateTime or TemporalAccessor.", String.valueOf(value))));
  }
  
  public Object doWork(Object value) {
    if(null == value){
      return null;
    }
    else if(value instanceof List<?>){
      return ((List<?>)value).stream().map(innerValue -> doWork(innerValue)).collect(Collectors.toList());
    }
    else{
      // We know it's an Instant
      try {
        return getDatePart((TemporalAccessor)value);
      } catch (UnsupportedTemporalTypeException utte) {
        throw new UncheckedIOException(new IOException(String.format(Locale.ROOT, "It is not possible to call '%s' function on %s", getFunction(), value.getClass().getName())));
      }
    }
  }
}

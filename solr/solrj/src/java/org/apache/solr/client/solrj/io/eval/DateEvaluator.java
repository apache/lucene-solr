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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.Locale;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class DateEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;

  private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US);
  private SimpleDateFormat parseFormat;


  static {
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public DateEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object values[]) throws IOException {
    String sdate = values[0].toString();
    String template = values[1].toString();

    if(sdate.startsWith("\"")) {
      sdate =sdate.replace("\"", "");
    }

    if(template.startsWith("\"")) {
      template =template.replace("\"", "");
    }


    if(parseFormat == null) {
      String timeZone = "UTC";
      if(values.length == 3) {
        timeZone = values[2].toString();
      }
      parseFormat = new SimpleDateFormat(template, Locale.US);
      parseFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
    }

    try {
      Date date = parseFormat.parse(sdate);
      return dateFormat.format(date);
    } catch(Exception e) {
      throw new IOException(e);
    }
  }
}

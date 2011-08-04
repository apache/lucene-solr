package org.apache.lucene.queryParser.standard.config;

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

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.standard.processors.ParametricRangeQueryNodeProcessor;
import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute is used by {@link ParametricRangeQueryNodeProcessor} processor
 * and must be defined in the {@link QueryConfigHandler}. This attribute tells
 * the processor which {@link Resolution} to use when parsing the date. <br/>
 * 
 * @see org.apache.lucene.queryParser.standard.config.DateResolutionAttribute
 */
public class DateResolutionAttributeImpl extends AttributeImpl 
				implements DateResolutionAttribute {

  private DateTools.Resolution dateResolution = null;

  public DateResolutionAttributeImpl() {
	  dateResolution = null; //default in 2.4
  }

  public void setDateResolution(DateTools.Resolution dateResolution) {
    this.dateResolution = dateResolution;
  }

  public DateTools.Resolution getDateResolution() {
    return this.dateResolution;
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copyTo(AttributeImpl target) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object other) {

    if (other instanceof DateResolutionAttributeImpl) {
    	DateResolutionAttributeImpl dateResAttr = (DateResolutionAttributeImpl) other;

      if (dateResAttr.getDateResolution() == getDateResolution()
          || dateResAttr.getDateResolution().equals(getDateResolution())) {

        return true;

      }

    }

    return false;

  }

  @Override
  public int hashCode() {
    return (this.dateResolution == null) ? 0 : this.dateResolution.hashCode();
  }

  @Override
  public String toString() {
    return "<dateResolutionAttribute dateResolution='" + this.dateResolution
        + "'/>";
  }

}

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

import java.util.Locale;

import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.standard.processors.ParametricRangeQueryNodeProcessor;
import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute is used by processor {@link ParametricRangeQueryNodeProcessor}
 * and must be defined in the {@link QueryConfigHandler}. This attribute tells
 * the processor what is the default {@link Locale} used to parse a date. <br/>
 * 
 * @see org.apache.lucene.queryParser.standard.config.LocaleAttribute
 */
public class LocaleAttributeImpl extends AttributeImpl
				implements LocaleAttribute {

  private Locale locale = Locale.getDefault();

  public LocaleAttributeImpl() {
	  locale = Locale.getDefault(); //default in 2.4
  }

  public void setLocale(Locale locale) {
    this.locale = locale;
  }

  public Locale getLocale() {
    return this.locale;
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

    if (other instanceof LocaleAttributeImpl) {
    	LocaleAttributeImpl localeAttr = (LocaleAttributeImpl) other;

      if (localeAttr.locale == this.locale
          || (this.locale != null && localeAttr.locale != null && this.locale
              .equals(localeAttr.locale))) {

        return true;

      }

    }

    return false;

  }

  @Override
  public int hashCode() {
    return (this.locale == null) ? 0 : this.locale.hashCode();
  }

  @Override
  public String toString() {
    return "<localeAttribute locale=" + this.locale + "/>";
  }

}

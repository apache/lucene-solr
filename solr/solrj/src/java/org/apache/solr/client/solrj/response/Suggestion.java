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
package org.apache.solr.client.solrj.response;
/**
 * This class models a Suggestion coming from Solr Suggest Component.
 * It is a direct mapping fo the Json object Solr is returning.
 */
public class Suggestion {
    private String term;
    private long weight;
    private String payload;

    public Suggestion(String term, long weight, String payload) {
        this.term = term;
        this.weight = weight;
        this.payload = payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Suggestion)) return false;

        Suggestion that = (Suggestion) o;

        return payload.equals(that.payload) && term.equals(that.term);

    }

    @Override
    public int hashCode() {
        int result = term.hashCode();
        result = 31 * result + payload.hashCode();
        return result;
    }

    public String getTerm() {
        return term;
    }

    public long getWeight() {
        return weight;
    }

    public String getPayload() {
        return payload;
    }

}

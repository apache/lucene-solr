// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.solr.handler.dataimport;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.IOException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;

public class SilkcloudIndexDocument
{
    private Map<String, Object> fields;
    
    public SilkcloudIndexDocument() {
        this.fields = new HashMap<String, Object>();
    }
    
    public Map<String, Object> getFields() {
        return this.fields;
    }
    
    public void setFields(final Map<String, Object> fields) {
        this.fields = fields;
    }
    
    @JsonCreator
    public static SilkcloudIndexDocument create(final String jsonString) throws JsonParseException, JsonMappingException, IOException {
        final ObjectMapper mapper = new ObjectMapper();
        SilkcloudIndexDocument doc = null;
        doc = (SilkcloudIndexDocument)mapper.readValue(jsonString, (Class)SilkcloudIndexDocument.class);
        return doc;
    }
}

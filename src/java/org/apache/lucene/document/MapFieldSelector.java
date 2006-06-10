/*
 * MapFieldSelector.java
 *
 * Created on May 2, 2006, 6:49 PM
 *
 */

package org.apache.lucene.document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A FieldSelector based on a Map of field names to FieldSelectorResults
 *
 * @author Chuck Williams
 */
public class MapFieldSelector implements FieldSelector {
    
    Map fieldSelections;
    
    /** Create a a MapFieldSelector
     * @param fieldSelections maps from field names to FieldSelectorResults
     */
    public MapFieldSelector(Map fieldSelections) {
        this.fieldSelections = fieldSelections;
    }
    
    /** Create a a MapFieldSelector
     * @param fields fields to LOAD.  All other fields are NO_LOAD.
     */
    public MapFieldSelector(List fields) {
        fieldSelections = new HashMap(fields.size()*5/3);
        for (int i=0; i<fields.size(); i++)
            fieldSelections.put(fields.get(i), FieldSelectorResult.LOAD);
    }
    
    /** Create a a MapFieldSelector
     * @param fields fields to LOAD.  All other fields are NO_LOAD.
     */
    public MapFieldSelector(String[] fields) {
        fieldSelections = new HashMap(fields.length*5/3);
        for (int i=0; i<fields.length; i++)
            fieldSelections.put(fields[i], FieldSelectorResult.LOAD);
    }
    
    /** Load field according to its associated value in fieldSelections
     * @param field a field name
     * @return the fieldSelections value that field maps to or NO_LOAD if none.
     */
    public FieldSelectorResult accept(String field) {
        FieldSelectorResult selection = (FieldSelectorResult) fieldSelections.get(field);
        return selection!=null ? selection : FieldSelectorResult.NO_LOAD;
    }
    
}

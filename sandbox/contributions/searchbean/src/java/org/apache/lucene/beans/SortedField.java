/*
 * SortedField.java
 *
 * Created on May 20, 2002, 4:15 PM
 */

package org.apache.lucene.beans;


import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

import java.util.Hashtable;

import java.io.File;
import java.io.IOException;

/**
 *
 * @author  carlson
 */
public class SortedField {
    
    private String fieldName;
    
    private String[] fieldValues;
    
    private static Hashtable fieldList = new Hashtable(); //keeps track of all fields
    
    /** Creates a new instance of SortedField */
    public SortedField() {
    }
    
    /** add a field so that is can be used to sort
     * @param fieldName the name of the field to add
     * @param indexPath path to Lucene index directory
     */
    public static void addField(String fieldName, String indexPath) throws IOException{
        IndexReader ir = IndexReader.open(indexPath);
        addField(fieldName, ir);
    }
    
    /** add a field so that is can be used to sort
     * @param fieldName the name of the field to add
     * @param indexFile File pointing to Lucene index directory
     */
    public static void addField(String fieldName, File indexFile) throws IOException{
        IndexReader ir = IndexReader.open(indexFile);
        addField(fieldName, ir);
    }
    
    
    /** add a field so that is can be used to sort
     * @param fieldName the name of the field to add
     * @param directory Lucene Directory
     */
    public static void addField(String fieldName, Directory directory) throws IOException{
        IndexReader ir = IndexReader.open(directory);
        addField(fieldName, ir);
    }
    
    private static void addField(String fieldName, IndexReader ir) throws IOException{
        SortedField sortedField = new SortedField();
        sortedField.addSortedField(fieldName,ir);
        //long start = System.currentTimeMillis();
        fieldList.put(fieldName, sortedField);
        //logger.info("adding data from field "+fieldName+" took "+(System.currentTimeMillis()-start));
    }
    
    /** adds the data from the index into a string array
     */
    private void addSortedField(String fieldName, IndexReader ir) throws IOException{
        int numDocs = ir.numDocs();
        fieldValues = new String[numDocs];
        for (int i=0; i<numDocs; i++) {
            if (ir.isDeleted(i) == false){
                fieldValues[i] = ir.document(i).get(fieldName);
            } else {
                fieldValues[i] = "";
            }
        }
        ir.close();
    }
    
    /** returns the value of the field
     * @param globalID Lucene's global document ID
     * @return value of field
     */
    public String getFieldValue(int globalID) {
        return fieldValues[globalID];
    }
    
    /** provides way to retrieve a SortedField once you add it
     * @param fieldName name of field to lookup
     * @return SortedField field to use when sorting
     */
    public static SortedField getSortedField(String fieldName){
        return  (SortedField) fieldList.get(fieldName);
    }
    
    /** Getter for property fieldName.
     * @return Value of property fieldName.
     */
    public java.lang.String getFieldName() {
        return fieldName;
    }
}

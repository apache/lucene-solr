

/* First created by JCasGen Sat May 07 22:33:38 JST 2011 */
package org.apache.solr.uima.ts;

import org.apache.uima.jcas.JCas; 
import org.apache.uima.jcas.JCasRegistry;
import org.apache.uima.jcas.cas.TOP_Type;

import org.apache.uima.jcas.tcas.Annotation;


/** 
 * Updated by JCasGen Sat May 07 22:33:38 JST 2011
 * XML source: /Users/koji/Documents/workspace/DummyEntityAnnotator/desc/DummyEntityAEDescriptor.xml
 * @generated */
public class EntityAnnotation extends Annotation {
  /** @generated
   * @ordered 
   */
  public final static int typeIndexID = JCasRegistry.register(EntityAnnotation.class);
  /** @generated
   * @ordered 
   */
  public final static int type = typeIndexID;
  /** @generated  */
  public              int getTypeIndexID() {return typeIndexID;}
 
  /** Never called.  Disable default constructor
   * @generated */
  protected EntityAnnotation() {}
    
  /** Internal - constructor used by generator 
   * @generated */
  public EntityAnnotation(int addr, TOP_Type type) {
    super(addr, type);
    readObject();
  }
  
  /** @generated */
  public EntityAnnotation(JCas jcas) {
    super(jcas);
    readObject();   
  } 

  /** @generated */  
  public EntityAnnotation(JCas jcas, int begin, int end) {
    super(jcas);
    setBegin(begin);
    setEnd(end);
    readObject();
  }   

  /** <!-- begin-user-doc -->
    * Write your own initialization here
    * <!-- end-user-doc -->
  @generated modifiable */
  private void readObject() {}
     
 
    
  //*--------------*
  //* Feature: name

  /** getter for name - gets 
   * @generated */
  public String getName() {
    if (EntityAnnotation_Type.featOkTst && ((EntityAnnotation_Type)jcasType).casFeat_name == null)
      jcasType.jcas.throwFeatMissing("name", "org.apache.solr.uima.ts.EntityAnnotation");
    return jcasType.ll_cas.ll_getStringValue(addr, ((EntityAnnotation_Type)jcasType).casFeatCode_name);}
    
  /** setter for name - sets  
   * @generated */
  public void setName(String v) {
    if (EntityAnnotation_Type.featOkTst && ((EntityAnnotation_Type)jcasType).casFeat_name == null)
      jcasType.jcas.throwFeatMissing("name", "org.apache.solr.uima.ts.EntityAnnotation");
    jcasType.ll_cas.ll_setStringValue(addr, ((EntityAnnotation_Type)jcasType).casFeatCode_name, v);}    
   
    
  //*--------------*
  //* Feature: entity

  /** getter for entity - gets 
   * @generated */
  public String getEntity() {
    if (EntityAnnotation_Type.featOkTst && ((EntityAnnotation_Type)jcasType).casFeat_entity == null)
      jcasType.jcas.throwFeatMissing("entity", "org.apache.solr.uima.ts.EntityAnnotation");
    return jcasType.ll_cas.ll_getStringValue(addr, ((EntityAnnotation_Type)jcasType).casFeatCode_entity);}
    
  /** setter for entity - sets  
   * @generated */
  public void setEntity(String v) {
    if (EntityAnnotation_Type.featOkTst && ((EntityAnnotation_Type)jcasType).casFeat_entity == null)
      jcasType.jcas.throwFeatMissing("entity", "org.apache.solr.uima.ts.EntityAnnotation");
    jcasType.ll_cas.ll_setStringValue(addr, ((EntityAnnotation_Type)jcasType).casFeatCode_entity, v);}    
  }

    
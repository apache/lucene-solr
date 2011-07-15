

/* First created by JCasGen Fri Mar 04 13:08:40 CET 2011 */
package org.apache.solr.uima.ts;

import org.apache.uima.jcas.JCas; 
import org.apache.uima.jcas.JCasRegistry;
import org.apache.uima.jcas.cas.TOP_Type;

import org.apache.uima.jcas.tcas.Annotation;


/** 
 * Updated by JCasGen Fri Mar 04 13:08:40 CET 2011
 * XML source: /Users/tommasoteofili/Documents/workspaces/lucene_workspace/lucene_dev/solr/contrib/uima/src/test/resources/DummySentimentAnalysisAEDescriptor.xml
 * @generated */
public class SentimentAnnotation extends Annotation {
  /** @generated
   * @ordered 
   */
  public final static int typeIndexID = JCasRegistry.register(SentimentAnnotation.class);
  /** @generated
   * @ordered 
   */
  public final static int type = typeIndexID;
  /** @generated  */
  public              int getTypeIndexID() {return typeIndexID;}
 
  /** Never called.  Disable default constructor
   * @generated */
  protected SentimentAnnotation() {}
    
  /** Internal - constructor used by generator 
   * @generated */
  public SentimentAnnotation(int addr, TOP_Type type) {
    super(addr, type);
    readObject();
  }
  
  /** @generated */
  public SentimentAnnotation(JCas jcas) {
    super(jcas);
    readObject();   
  } 

  /** @generated */  
  public SentimentAnnotation(JCas jcas, int begin, int end) {
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
  //* Feature: mood

  /** getter for mood - gets 
   * @generated */
  public String getMood() {
    if (SentimentAnnotation_Type.featOkTst && ((SentimentAnnotation_Type)jcasType).casFeat_mood == null)
      jcasType.jcas.throwFeatMissing("mood", "org.apache.solr.uima.ts.SentimentAnnotation");
    return jcasType.ll_cas.ll_getStringValue(addr, ((SentimentAnnotation_Type)jcasType).casFeatCode_mood);}
    
  /** setter for mood - sets  
   * @generated */
  public void setMood(String v) {
    if (SentimentAnnotation_Type.featOkTst && ((SentimentAnnotation_Type)jcasType).casFeat_mood == null)
      jcasType.jcas.throwFeatMissing("mood", "org.apache.solr.uima.ts.SentimentAnnotation");
    jcasType.ll_cas.ll_setStringValue(addr, ((SentimentAnnotation_Type)jcasType).casFeatCode_mood, v);}    
  }

    
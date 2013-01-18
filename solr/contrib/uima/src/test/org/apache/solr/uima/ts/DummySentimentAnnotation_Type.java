/* First created by JCasGen Fri Mar 04 13:08:40 CET 2011 */
package org.apache.solr.uima.ts;

import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.JCasRegistry;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.cas.impl.FSGenerator;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.impl.TypeImpl;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.impl.FeatureImpl;
import org.apache.uima.cas.Feature;
import org.apache.uima.jcas.tcas.Annotation_Type;

/** 
 * Updated by JCasGen Fri Mar 04 13:08:40 CET 2011
 * @generated */
public class DummySentimentAnnotation_Type extends Annotation_Type {
  /** @generated */
  @Override
  protected FSGenerator getFSGenerator() {return fsGenerator;}
  /** @generated */
  private final FSGenerator fsGenerator = 
    new FSGenerator() {
      @Override
      public FeatureStructure createFS(int addr, CASImpl cas) {
        if (DummySentimentAnnotation_Type.this.useExistingInstance) {
          // Return eq fs instance if already created
          FeatureStructure fs = DummySentimentAnnotation_Type.this.jcas.getJfsFromCaddr(addr);
          if (null == fs) {
            fs = new DummySentimentAnnotation(addr, DummySentimentAnnotation_Type.this);
            DummySentimentAnnotation_Type.this.jcas.putJfsFromCaddr(addr, fs);
            return fs;
          }
          return fs;
        } else return new DummySentimentAnnotation(addr, DummySentimentAnnotation_Type.this);
      }
    };
  /** @generated */
  public final static int typeIndexID = DummySentimentAnnotation.typeIndexID;
  /** @generated 
     @modifiable */
  public final static boolean featOkTst = JCasRegistry.getFeatOkTst("org.apache.solr.uima.ts.SentimentAnnotation");
 
  /** @generated */
  final Feature casFeat_mood;
  /** @generated */
  final int     casFeatCode_mood;
  /** @generated */ 
  public String getMood(int addr) {
        if (featOkTst && casFeat_mood == null)
      jcas.throwFeatMissing("mood", "org.apache.solr.uima.ts.SentimentAnnotation");
    return ll_cas.ll_getStringValue(addr, casFeatCode_mood);
  }
  /** @generated */    
  public void setMood(int addr, String v) {
        if (featOkTst && casFeat_mood == null)
      jcas.throwFeatMissing("mood", "org.apache.solr.uima.ts.SentimentAnnotation");
    ll_cas.ll_setStringValue(addr, casFeatCode_mood, v);}
    
  



  /** initialize variables to correspond with Cas Type and Features
  * @generated */
  public DummySentimentAnnotation_Type(JCas jcas, Type casType) {
    super(jcas, casType);
    casImpl.getFSClassRegistry().addGeneratorForType((TypeImpl)this.casType, getFSGenerator());

 
    casFeat_mood = jcas.getRequiredFeatureDE(casType, "mood", "uima.cas.String", featOkTst);
    casFeatCode_mood  = (null == casFeat_mood) ? JCas.INVALID_FEATURE_CODE : ((FeatureImpl)casFeat_mood).getCode();

  }
}



    

package org.apache.lucene.gdata.storage.lucenestorage;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.gdata.data.GDataAccount;

/**
 * Wrapps a User Object.
 * The wrapper provides also a Lucene repesentation of the user;
 * User Objects will not be Buffered in the lucene storage component. Each User will be written imidialtely.
 * @author Simon Willnauer
 *
 */
public class StorageAccountWrapper implements StorageWrapper{
    private static final Log LOG = LogFactory.getLog(StorageAccountWrapper.class);
    
    /**
     * Lucene field for the username
     */
    public static final String FIELD_ACCOUNTNAME = "accountName";
    /**
     * Lucene field for the password
     */
    public static final String FIELD_PASSWORD = "passwd";
    /**
     * Lucene field for the author name
     */
    public static final String FIELD_AUTHORNAME = "author";
    /**
     * Lucene field for the author mail address
     */
    public static final String FIELD_AUTHORMAIL = "authorMail";
    /**
     * Lucene field for the author link
     */
    public static final String FIELD_AUTHORHREF = "authorHref";
    /**
     * Lucene field fot the userroles
     */
    public static final String FIELD_ROLES = "userroles";
    private final GDataAccount user;
    /**
     * @param user - the user to be wrapped
     */
    public StorageAccountWrapper(final GDataAccount user) {
        if(user == null)
            throw new IllegalArgumentException("user must not be null");
        this.user = user;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageWrapper#getLuceneDocument()
     */
    public Document getLuceneDocument() {
        Document doc = new Document();
        
        doc.add(new Field(FIELD_ACCOUNTNAME,this.user.getName(),Field.Store.YES,Field.Index.UN_TOKENIZED));
        doc.add(new Field(FIELD_PASSWORD,this.user.getPassword()==null?"":this.user.getPassword(),Field.Store.YES,Field.Index.NO));
        doc.add(new Field(FIELD_AUTHORNAME,this.user.getAuthorname()==null?"":this.user.getAuthorname(),Field.Store.YES,Field.Index.NO));
        doc.add(new Field(FIELD_AUTHORMAIL,this.user.getAuthorMail()==null?"":this.user.getAuthorMail(),Field.Store.YES,Field.Index.NO));
        doc.add(new Field(FIELD_AUTHORHREF,this.user.getAuthorLink()==null?"":this.user.getAuthorLink().toString(),Field.Store.YES,Field.Index.NO));
        doc.add(new Field(FIELD_ROLES, Integer.toString(this.user.getRolesAsInt()),Field.Store.YES,Field.Index.NO)); 
       
        return doc;
    }
   
   
    
    
    /**
     * @param doc - a lucene document representation of an user
     * @return - the user to build from the document. or <code>null</code> if the document is <code>null</code>
     */
    public static GDataAccount buildEntity(final Document doc){
        if(doc == null)
            return null;
        
        GDataAccount user = new GDataAccount();
        user.setName(doc.get(FIELD_ACCOUNTNAME));
        user.setPassword(doc.get(FIELD_PASSWORD));
        user.setAuthorname(doc.get(FIELD_AUTHORNAME));
        user.setAuthorMail(doc.get(FIELD_AUTHORMAIL));
        try{
        user.setRolesAsInt(Integer.parseInt(doc.get(FIELD_ROLES)));
        }catch (NumberFormatException e) {
            LOG.info("Can't parse userroles: "+user.getName()+" throws NumberFormatException. -- skipping --",e);
        }
        try {
            if(doc.get(FIELD_AUTHORHREF)!= null)
                user.setAuthorLink(new URL(doc.get(FIELD_AUTHORHREF)));
        } catch (MalformedURLException e) {
            LOG.info("SPECIFIED URL for user: "+user.getName()+" throws MalformedURLException. -- skipping --",e);
        }
        return user;
    }
    
   

    /**
     * @return - the wrapped user
     */
    public GDataAccount getUser() {
        return this.user;
    }

}

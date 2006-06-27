/** 
 * Copyright 2004 The Apache Software Foundation 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 */

package org.apache.lucene.gdata.data;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import com.google.gdata.data.Person;

/**
 * The GData-Server system provides acccount to be associated with registered
 * feed. Every feed has an owner account. The account holder is automaticaly in
 * role to modify his feeds. One account can own <i>n</i> feeds having <i>m</i>
 * entries.
 * <p>
 * Additionally an account can be in role to modify other feeds, create accounts
 * or feeds. See {@link AccountRole} for detailed infomation about roles. One
 * account can also have more than one role. All roles in {@link AccountRole}
 * can be combined
 * </p>
 * <p>
 * For each account values for author name, author email and author link can be
 * set at creation time or during an update. These values will be used as the
 * corresponding values for the feed
 * {@link org.apache.lucene.gdata.data.ServerBaseFeed#addAuthor(Person)} if no
 * value for the feed has be specified.
 * </p>
 * 
 * @author Simon Willnauer
 * 
 */
public class GDataAccount {
    private String name;

    private String authorname;

    private String authorMail;

    private URL authorLink;

    private String password;

    private Set<AccountRole> roles = new HashSet<AccountRole>(4);

    /**
     * Creates a new GDataAccount. The default role {@link AccountRole#USER}
     * will be set.
     * 
     */
    public GDataAccount() {
        this.roles.add(AccountRole.USER);

    }

    /**
     * @return - the password
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * @param password -
     *            the account Password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return - the account name
     */
    public String getName() {
        return this.name;
    }

    /**
     * @param name
     *            The name to set.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return - the http link specified for the author
     */
    public URL getAuthorLink() {
        return this.authorLink;
    }

    /**
     * @param authorLink -
     *            the http link specified for the author
     */
    public void setAuthorLink(URL authorLink) {
        this.authorLink = authorLink;
    }

    /**
     * @return - the authors mail address
     */
    public String getAuthorMail() {
        return this.authorMail;
    }

    /**
     * @param authorMail -
     *            the authors mail address
     */
    public void setAuthorMail(String authorMail) {
        this.authorMail = authorMail;
    }

    /**
     * @return - the name specified as being the author name
     */
    public String getAuthorname() {
        return this.authorname;
    }

    /**
     * @param authorname -
     *            the name specified as being the author name
     */
    public void setAuthorname(String authorname) {
        this.authorname = authorname;
    }

    /**
     * Adds the given role to the role list
     * 
     * @param role -
     *            the role to add to the role list
     */
    public void setRole(AccountRole role) {
        if (role == null)
            return;
        this.roles.add(role);
    }

    /**
     * @return - the set containing all roles
     */
    public Set<AccountRole> getRoles() {
        return this.roles;
    }

    /**
     * @param role -
     *            the role to check
     * @return <code>true</code> if the role list contains the given role
     */
    public boolean isUserInRole(AccountRole role) {
        if (role == null)
            return false;
        return this.roles.contains(role);
    }

    /**
     * @see GDataAccount#setRolesAsInt(int)
     * @return - the integer representation for the user roles
     */
    public int getRolesAsInt() {
        // 1 as the Userrole is always set
        int bits = 1;
        for (AccountRole role : this.roles) {
            if (role == AccountRole.ENTRYAMINISTRATOR)
                bits ^= 2;
            else if (role == AccountRole.FEEDAMINISTRATOR)
                bits ^= 4;
            else if (role == AccountRole.USERADMINISTRATOR)
                bits ^= 8;

        }
        return bits;

    }

    /**
     * Sets the roles from a int representation.
     * <ol>
     * <li>The fist bit set indicates a {@link AccountRole#USER} - int value 1</li>
     * <li>The second bit set indicates a {@link AccountRole#ENTRYAMINISTRATOR} -
     * int value 2</li>
     * <li>The third bit set indicates a {@link AccountRole#FEEDAMINISTRATOR} -
     * int value 4</li>
     * <li>The forth bit set indicates a {@link AccountRole#USERADMINISTRATOR} -
     * int value 8</li>
     * <ol>
     * This method will only set roles, will not remove roles! A combination of
     * roles is also possible e.g. the int value 6 combines
     * {@link AccountRole#ENTRYAMINISTRATOR} and
     * {@link AccountRole#FEEDAMINISTRATOR}.
     * 
     * @param i -
     *            the integer used to set the roles
     */
    public void setRolesAsInt(int i) {

        if ((i & 2) > 0)
            this.roles.add(AccountRole.ENTRYAMINISTRATOR);
        if ((i & 4) > 0)
            this.roles.add(AccountRole.FEEDAMINISTRATOR);
        if ((i & 8) > 0)
            this.roles.add(AccountRole.USERADMINISTRATOR);

    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof GDataAccount) || o == null)
            return false;
        GDataAccount toCompare = (GDataAccount) o;
        if (this.name.equals(toCompare.name))
            return true;
        return false;

    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        int ret = 37;
        ret = 9 * ret + this.name.hashCode();
        return ret;
    }

    /**
     * Checks the requiered values for creating an account are set. Required
     * values are <tt>name</tt> and <tt>password</tt> the minimum length of
     * these values is 6.
     * 
     * @return <code>true</code> if an only if password and name are not <code>null</code> and the length is <tt>> 5</tt>
     */
    public boolean requiredValuesSet() {
        return (this.name != null && this.password != null
                && this.name.length() > 5 && this.password.length() > 5);
    }
    
    /**
     * @see java.lang.Object#toString()
     */
    public String toString(){
        StringBuilder builder = new StringBuilder("GdataAccount: ");
        builder.append("name: ").append(this.name);
        builder.append(" password: ").append((this.password!= null?" length: "+this.password.length():null));
        builder.append(" author: ").append(this.authorname);
        builder.append(" author email: ").append(this.authorMail);
        builder.append(" author link: ").append(this.authorLink);
        return builder.toString();
    }
    
    /**
     * checks whether the given integer matches the account role.
     * @param intRole - integer representation of a role
     * @param role - the accountrole to match
     * @return <code>true</code> if and only if the given roles match, otherwise <code>false</code>
     */
    public static boolean isInRole(int intRole, AccountRole role){
        if(role == AccountRole.USER)
            return (intRole&1)>0;
        if (role == AccountRole.ENTRYAMINISTRATOR)
            return (intRole&2) >0 ;
        else if (role == AccountRole.FEEDAMINISTRATOR)
            return (intRole&4) >0 ;
        else if (role == AccountRole.USERADMINISTRATOR)
            return (intRole&8) >0 ;
        return false;
    }
    
    /**
     * @return - a new Administartor accoutn 
     */
    public static final GDataAccount createAdminAccount(){
        GDataAccount retVal = new GDataAccount();
        retVal.setName("administrator");
        retVal.setPassword("password");
        retVal.setRole(AccountRole.USERADMINISTRATOR);
        retVal.setRole(AccountRole.FEEDAMINISTRATOR);
        retVal.setRole(AccountRole.ENTRYAMINISTRATOR);
        return retVal;
    }

    /**
     * This enum respesents all account roles an account can have.
     * 
     * @author Simon Willnauer
     * 
     */
    public enum AccountRole {

        /**
         * Can create / alter user
         */
        USERADMINISTRATOR,

        /**
         * Can create / alter feeds
         */
        FEEDAMINISTRATOR,
        /**
         * Can create / alter entries
         */
        ENTRYAMINISTRATOR,
        /**
         * can create / alter his own feed entries
         */
        USER
    }

}

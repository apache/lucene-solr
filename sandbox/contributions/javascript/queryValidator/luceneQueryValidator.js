// Author: Kelvin Tan  (kelvin@relevanz.com)
// JavaScript Lucene Query Validator
// Version: $Id$
// Tested: IE 6.0.2800 and Mozilla Firebird 0.7

// Special characters are + - && || ! ( ) { } [ ] ^ " ~ * ? : \
// Special words are (case-sensitive) AND NOT OR

// Makes wildcard queries case-insensitive if true.
// Refer to http://www.mail-archive.com/lucene-user@jakarta.apache.org/msg00646.html
var wildcardCaseInsensitive = true;

// Mutator method for wildcardCaseInsensitive.
// @param Should wildcard queries be case-insensitive?
function setWildcardCaseInsensitive(bool)
{
  wildcardCaseInsensitive = bool;
}

var alertUser = true;

function setAlertUser(bool)
{
  alertUser = bool;
}

// validates a lucene query.
// @param Form field that contains the query
function doCheckLuceneQuery(queryField)
{
  var query = queryField.value;
  if(query != null && query.length > 0)
  {
    // check for allowed characters
    var matches = query.match(/[^a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@# ]/);
    if(matches != null && matches.length > 0)
    {
      if(alertUser) alert("Invalid search query! The allowed characters are a-z A-Z 0-9.  _ + - : () \" & * ? | ! {} [ ] ^ ~ \\ @ #. Please try again.")
      return false;
    }
    
    // check wildcards are used properly
    matches = query.match(/^[\*]*$|([\s]\*)/);
    if(matches != null && matches.length > 0)
    {
      if(alertUser) alert("Invalid search query! The wildcard (*) character must be preceded by at least one alphabet or number. Please try again.")
      return false;
    }
    
    // check for && usage
    // NB: doesn't handle term1 && term2 && term3 in Firebird 0.7
    matches = query.match(/[&]{2}/);
    if(matches != null && matches.length > 0)
    {
      matches = query.match(/^([a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@#]+( && )?[a-zA-Z0-9_+\-:.()\"*?|!{}\[\]\^~\\@#]+[ ]*)+$/); // note missing & in pattern
      if(matches == null)
      {
        if(alertUser) alert("Invalid search query! Queries containing the special characters && must be in the form: term1 && term2. Please try again.")
        return false;
      }
    }
    
    // check ^ is used properly 
    matches = query.match(/^[^\^]*$|^([a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\~\\@#]+(\^[\d]+)?[ ]*)+$/); // note missing ^ in pattern
    if(matches == null)
    {
      if(alertUser) alert("Invalid search query! The caret (^) character must be preceded by alphanumeric characters and followed by numbers. Please try again.")
      return false;
    }
    
    // check ~ is used properly
    matches = query.match(/^[^~]*$|^([a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^\\@#]+(~[\d.]+)?[ ]*)+$/); // note missing ~in pattern
    if(matches == null)
    {
      if(alertUser) alert("Invalid search query! The tilde (~) character must be preceded by alphanumeric characters and followed by numbers. Please try again.")
      return false;
    }    
    
    // check ! is used properly 
    // NB: doesn't handle term1 ! term2 ! term3
    matches = query.match(/^[^!]*$|^([a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@#]+( ! )?[a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@#]+[ ]*)+$/);
    if(matches == null || matches.length == 0)
    {
      if(alertUser) alert("Invalid search query! Queries containing the special character ! must be in the form: term1 ! term2. Please try again.")
      return false;
    }    
    
    // check question marks are used properly
    matches = query.match(/^(\?)|([^a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@#]\?+)/);
    if(matches != null && matches.length > 0)
    {
        if(alertUser) alert("Invalid search query! The question mark (?) character must be preceded by at least one alphabet or number. Please try again.")
      return false;
    }
    
    // check parentheses are used properly
    matches = query.match(/^[^()]*$|^(\([a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@#]+\))*$/);
    if(matches == null || matches.length == 0)
    {
      if(alertUser) alert("Invalid search query! Parentheses must be closed and contain at least one alphabet or number. Please try again.")
      return false;
    }

    // check '+' and '-' are used properly      
    matches = query.match(/^[^\n+\-]*$|^([+-]?[a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@#]+[ ]?)+$/);
    if(matches == null || matches.length == 0)
    {
      if(alertUser) alert("Invalid search query! '+' and '-' modifiers must be followed by at least one alphabet or number. Please try again.")
      return false;
    }
    
    // check AND, OR and NOT are used properly
    matches = query.match(/AND|OR|NOT/);
    if(matches != null && matches.length > 0)
    {
      // I've no idea why the code below doesn't work since it's identical to the exclamation mark and && RE    
      //matches = query.match(/^([a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@#]+(?: AND )?(?: OR )?(?: NOT )?[a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@#]+[ ]*)+$/);
      
      // we'll notify the user that this query is not validated and an error may result
      if(alertUser)
      {
        return confirm("Validation for queries containing AND/OR/NOT is currently not supported. If your query is not well-formed, an error may result.");
      }
      /*
      if(matches == null || matches.length == 0)
      {
        if(alertUser) alert("Invalid search query!  Queries containing AND/OR/NOT must be in the form: term1 AND|OR|NOT term2 Please try again.")
        return false;
      }
      */
    }
    
    
    // check that quote marks are closed
    matches = query.match(/\"/g);
    if(matches != null && matches.length > 0)
    {
      var number = matches.length;
      if((number % 2) > 0)
      {
        if(alertUser) alert("Invalid search query! Please close all quote (\") marks.");
        return false;
      }
    }
    
    // check ':' is used properly
    matches = query.match(/^[^:]*$|^([a-zA-Z0-9_+\-.()\"*?&|!{}\[\]\^~\\@#]+(:[a-zA-Z0-9_+\-.()\"*?&|!{}\[\]\^~\\@#]+)?[ ]*)+$/); // note missing : in pattern
    if(matches == null || matches.length == 0)
    {
      if(alertUser) alert("Invalid search query! Field declarations (:) must be preceded by at least one alphabet or number and followed by at least one alphabet or number. Please try again.")
      return false;
    }
    
    if(wildcardCaseInsensitive)
    {
      if(query.indexOf("*") != -1)
      {
        var i = query.indexOf(':');
        if(i == -1)
        {
          queryField.value = query.toLowerCase();
        }
        else // found a wildcard field search
        {
          queryField.value = query.substring(0, i) + query.substring(i).toLowerCase();
        }
      }
    }

    return true;
  }
}

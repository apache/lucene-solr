// Author: Kelvin Tan  (kelvint at apache.org)
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

// Should the user be prompted with an alert box if validation fails?
var alertUser = true;

function setAlertUser(bool)
{
  alertUser = bool;
}

// validates a lucene query.
// @param Form field that contains the query
function doCheckLuceneQuery(queryField)
{
  return doCheckLuceneQueryValue(queryField.value)
}

// validates a lucene query.
// @param query string
function doCheckLuceneQueryValue(query)
{
  if(query != null && query.length > 0)
  {
    query = removeEscapes(query);
    
    // check for allowed characters
    if(!checkAllowedCharacters(query)) return false;
    
    // check * is used properly
    if(!checkAsterisk(query)) return false;
    
    // check for && usage
    if(!checkAmpersands(query)) return false;
    
    // check ^ is used properly 
    if(!checkCaret(query)) return false;
    
    // check ~ is used properly
    if(!checkSquiggle(query)) return false;
    
    // check ! is used properly 
    if(!checkExclamationMark(query)) return false;
    
    // check question marks are used properly
    if(!checkQuestionMark(query)) return false;
    
    // check parentheses are used properly
    if(!checkParentheses(query)) return false;
    
    // check '+' and '-' are used properly      
    if(!checkPlusMinus(query)) return false;
    
    // check AND, OR and NOT are used properly
    if(!checkANDORNOT(query)) return false;    
    
    // check that quote marks are closed
    if(!checkQuotes(query)) return false;
    
    // check ':' is used properly
    if(!checkColon(query)) return false;
    
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

// remove the escape character and the character immediately following it
function removeEscapes(query)
{
  return query.replace(/\\./g, "");
}

function checkAllowedCharacters(query)
{
  matches = query.match(/[^a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@#$%'= ]/);
  if(matches != null && matches.length > 0)
  {
    if(alertUser) alert("Invalid search query! The allowed characters are a-z A-Z 0-9.  _ + - : () \" & * ? | ! {} [ ] ^ ~ \\ @ = # % $ '. Please try again.")
    return false;
  }
  return true;
}

function checkAsterisk(query)
{
  matches = query.match(/^[\*]*$|[\s]\*|^\*[^\s]/);
  if(matches != null)
  {
    if(alertUser) alert("Invalid search query! The wildcard (*) character must be preceded by at least one alphabet or number. Please try again.")
    return false;
  }
  return true;
}

function checkAmpersands(query)
{
  // NB: doesn't handle term1 && term2 && term3 in Firebird 0.7
  matches = query.match(/[&]{2}/);
  if(matches != null && matches.length > 0)
  {
    matches = query.match(/^([a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@#$%'=]+( && )?[a-zA-Z0-9_+\-:.()\"*?|!{}\[\]\^~\\@#$%'=]+[ ]*)+$/); // note missing & in pattern
    if(matches == null)
    {
      if(alertUser) alert("Invalid search query! Queries containing the special characters && must be in the form: term1 && term2. Please try again.")
      return false;
    }
  }
  return true;
}

function checkCaret(query)
{
  //matches = query.match(/^[^\^]*$|^([a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\~\\@#]+(\^[\d]+)?[ ]*)+$/); // note missing ^ in pattern
  matches = query.match(/[^\\]\^([^\s]*[^0-9.]+)|[^\\]\^$/);
  if(matches != null)
  {
    if(alertUser) alert("Invalid search query! The caret (^) character must be preceded by alphanumeric characters and followed by numbers. Please try again.")
    return false;
  }
  return true;
}

function checkSquiggle(query)
{
  //matches = query.match(/^[^~]*$|^([a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^\\@#]+(~[\d.]+|[^\\]\\~)?[ ]*)+$/); // note missing ~ in pattern
  matches = query.match(/[^\\]~[^\s]*[^0-9\s]+/);
  if(matches != null)
  {
    if(alertUser) alert("Invalid search query! The tilde (~) character must be preceded by alphanumeric characters and followed by numbers. Please try again.")
    return false;
  }    
  return true;
}

function checkExclamationMark(query)
{
  // NB: doesn't handle term1 ! term2 ! term3
  matches = query.match(/^[^!]*$|^([a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@#$%'=]+( ! )?[a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@#$%'=]+[ ]*)+$/);
  if(matches == null || matches.length == 0)
  {
    if(alertUser) alert("Invalid search query! Queries containing the special character ! must be in the form: term1 ! term2. Please try again.")
    return false;
  }    
  return true;
}

function checkQuestionMark(query)
{
  matches = query.match(/^(\?)|([^a-zA-Z0-9_+\-:.()\"*?&|!{}\[\]\^~\\@#$%'=]\?+)/);
  if(matches != null && matches.length > 0)
  {
      if(alertUser) alert("Invalid search query! The question mark (?) character must be preceded by at least one alphabet or number. Please try again.")
    return false;
  }
  return true;
}

function checkParentheses(query)
{
  var hasLeft = false;
  var hasRight = false;
  matchLeft = query.match(/[(]/g);
  if(matchLeft != null) hasLeft = true
  matchRight = query.match(/[)]/g);
  if(matchRight != null) hasRight = true;
  
  if(hasLeft || hasRight)
  {
    if(hasLeft && !hasRight || hasRight && !hasLeft)
    {
        if(alertUser) alert("Invalid search query! Parentheses must be closed. Please try again.")
        return false;
    }
    else
    {
      var number = matchLeft.length + matchRight.length;
      if((number % 2) > 0 || matchLeft.length != matchRight.length)
      {
        if(alertUser) alert("Invalid search query! Parentheses must be closed. Please try again.")
        return false;
      }    
    }
    matches = query.match(/\(\)/);
    if(matches != null)
    {
      if(alertUser) alert("Invalid search query! Parentheses must contain at least one character. Please try again.")
      return false;    
    }
  }  
  return true;    
}

function checkPlusMinus(query)
{
  matches = query.match(/^[^\n+\-]*$|^([+-]?[a-zA-Z0-9_:.()\"*?&|!{}\[\]\^~\\@#$%'=]+[ ]?)+$/);
  if(matches == null || matches.length == 0)
  {
    if(alertUser) alert("Invalid search query! '+' and '-' modifiers must be followed by at least one alphabet or number. Please try again.")
    return false;
  }
  return true;
}

function checkANDORNOT(query)
{
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
  return true;
}

function checkQuotes(query)
{
  matches = query.match(/\"/g);
  if(matches != null && matches.length > 0)
  {
    var number = matches.length;
    if((number % 2) > 0)
    {
      if(alertUser) alert("Invalid search query! Please close all quote (\") marks.");
      return false;
    }
    matches = query.match(/""/);
    if(matches != null)
    {
      if(alertUser) alert("Invalid search query! Quotes must contain at least one character. Please try again.")
      return false;    
    }    
  }
  return true;
}

function checkColon(query)
{
  matches = query.match(/[^\\\s]:[\s]|[^\\\s]:$|[\s][^\\]?:|^[^\\\s]?:/);
  if(matches != null)
  {
    if(alertUser) alert("Invalid search query! Field declarations (:) must be preceded by at least one alphabet or number and followed by at least one alphabet or number. Please try again.")
    return false;
  }
  return true;
}
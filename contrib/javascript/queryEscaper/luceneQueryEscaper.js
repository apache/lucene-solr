// Special characters are + - && || ! ( ) { } [ ] ^ " ~ * ? : \
// Special words are (case-sensitive) AND NOT OR
// We escape the common ones, i.e. ! ? * ( ) " :

// escapes a lucene query.
// @param Form field that contains the query, or the query string
function doEscapeQuery(queryArg)
{
  var query = getQueryValue(queryArg);
  query = escapeAsterisk(query);
  query = escapeQuotes(query);
  query = escapeColon(query);
  query = escapeQuestionMark(query);
  query = escapeExclamationMark(query);
  query = escapeParentheses(query);  
  query = escapeSquareBrackets(query);  
  query = escapeBraces(query);  
  query = escapeCaret(query);  
  query = escapeSquiggle(query);  
  query = escapeDoubleAmpersands(query);  
  query = escapeDoubleBars(query);  
  return query;
}

function getQueryValue(queryArg)
{
  var query;
  // check if its a form field
  if(typeof(queryArg.form) != "undefined")
  {
    query = queryArg.value;
  }
  else
  {
    query = queryArg;
  }
  return query;
}

function escapeAsterisk(query)
{
  return query.replace(/[\*]/g, "\\*");
}

function escapeQuotes(query)
{
  return query.replace(/[\"]/g, "\\\"");
}

function escapeColon(query)
{
  return query.replace(/[\:]/g, "\\:");
}

function escapeQuestionMark(query)
{
  return query.replace(/[?]/g, "\\?");
}

function escapeExclamationMark(query)
{
  return query.replace(/[!]/g, "\\!");
}

function escapeParentheses(query)
{
  return query.replace(/[(]/g, "\\(").replace(/[)]/g, "\\)");
}

function escapeSquareBrackets(query)
{
  return query.replace(/[\[]/g, "\\[").replace(/[\]]/g, "\\]");
}

function escapeBraces(query)
{
  return query.replace(/[{]/g, "\\{").replace(/[}]/g, "\\}");
}

function escapeCaret(query)
{
  return query.replace(/[\^]/g, "\\^");
}

function escapeSquiggle(query)
{
  return query.replace(/[~]/g, "\\~");
}

function escapeDoubleAmpersands(query)
{
  return query.replace(/[&]{2}/g, "\\&\\&");
}

function escapeDoubleBars(query)
{
  return query.replace(/[\|]{2}/g, "\\|\\|");
}
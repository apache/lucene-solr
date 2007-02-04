# Copyright:: Copyright (c) 2007 Apache Software Foundation
# License::   Apache Version 2.0 (see http://www.apache.org/licenses/)

# Filters added to this controller apply to all controllers in the application.
# Likewise, all the methods added will be available for all controllers.

class ApplicationController < ActionController::Base
  # Pick a unique cookie name to distinguish our session data from others'
  session :session_key => '_flare_session_id'


  def query
    queries = session[:queries]
    if queries.nil? || queries.empty?
      query = "[* TO *]"
    else
      query = session[:queries].collect{|q| "#{q[:negative] ? '-' : ''}(#{q[:query]})"}.join(' AND ')
    end
    
    query
  end
  
  def filters
    session[:filters].collect {|filter| "#{filter[:negative] ? '-' : ''}#{filter[:field]}:\"#{filter[:value]}\""}
  end

end

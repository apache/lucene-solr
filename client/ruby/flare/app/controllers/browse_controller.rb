# Copyright:: Copyright (c) 2007 Apache Software Foundation
# License::   Apache Version 2.0 (see http://www.apache.org/licenses/)

class BrowseController < ApplicationController
  before_filter :setup_session

  def index
    @info = SOLR.send(Solr::Request::IndexInfo.new) # TODO move this call to only have it called when the index may have changed
    @facet_fields = @info.field_names.find_all {|v| v =~ /_facet$/}
    
    request = Solr::Request::Standard.new :query => query,
                                          :filter_queries => filters,
                                          :facets => {:fields => @facet_fields, :limit => 20 , :mincount => 1, :sort => :count, :debug_query=>true}
    @response = SOLR.send(request)                                          
    
    #TODO: call response.field_facets(??) - maybe field_facets should be return a higher level? 
  end
  
  def facet
    @facets = retrieve_field_facets("#{params[:field]}")
  end
  
  def auto_complete_for_search_query
    # TODO instead of "text", default to the default search field configured in schema.xml
    @values = retrieve_field_facets("text", 5, params['search']['query'].downcase)
    
    render :partial => 'suggest'
  end


  def add_query
    session[:queries] << {:query => params[:search][:query]}
    redirect_to :action => 'index'
  end
  
  def invert_query
    q = session[:queries][params[:index].to_i]
    q[:negative] = !q[:negative]
    redirect_to :action => 'index'
  end

  def remove_query
    session[:queries].delete_at(params[:index].to_i)
    redirect_to :action => 'index'
  end

  def invert_filter
    f = session[:filters][params[:index].to_i]
    f[:negative] = !f[:negative]
    redirect_to :action => 'index'
  end
  
  def remove_filter
    session[:filters].delete_at(params[:index].to_i)
    redirect_to :action => 'index'
  end
  
  def add_filter
    session[:filters] << {:field => params[:field], :value => params[:value]} 
    redirect_to :action => 'index'
  end
  
  def clear
    session[:queries] = nil
    session[:filters] = nil
    setup_session
    redirect_to :action => 'index'
  end
  
  private
  def setup_session
    session[:queries] ||= [] 
    session[:filters] ||= []
  end
  
  def retrieve_field_facets(field, limit=-1, prefix=nil)
    req = Solr::Request::Standard.new(:query => query,
       :filter_queries => filters,
       :facets => {:fields => [field],
                   :mincount => 1, :limit => limit, :prefix => prefix
                  },
       :rows => 0
    )
    
    results = SOLR.send(req)
    
    results.field_facets(field)
  end
  
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

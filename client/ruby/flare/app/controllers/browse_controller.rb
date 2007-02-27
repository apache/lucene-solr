# Copyright:: Copyright (c) 2007 Apache Software Foundation
# License::   Apache Version 2.0 (see http://www.apache.org/licenses/)

class BrowseController < ApplicationController
  before_filter :flare_before
  
  # def self.flare(options={})
  #   define_method() do
  #   end
  # end
  # 
  # flare do |f|
  #   f.facet_fields = []
  # end
  
  def index
    # TODO Add paging and sorting
    @info = solr(Solr::Request::IndexInfo.new) # TODO move this call to only have it called when the index may have changed
    @facet_fields = @info.field_names.find_all {|v| v =~ /_facet$/}
    @text_fields = @info.field_names.find_all {|v| v =~ /_text$/}
    
    session[:page] = params[:page].to_i if params[:page]
    session[:page] = 1 if session[:page] <= 0
        
    @results_per_page = 25
    
    @start = (session[:page] - 1) * @results_per_page
    
    request = Solr::Request::Standard.new(:query => query,
                                          :filter_queries => filters,
                                          :rows => @results_per_page,
                                          :start => @start,
                                          :facets => {
                                            :fields => @facet_fields, :limit => 20 , :mincount => 1, :sort => :count,
#                                            :queries => session[:saved].collect {|constraints| make_query(constraints)}
                                          },
                                          :highlighting => {:field_list => @text_fields})
    logger.info({:query => query, :filter_queries => filters}.inspect)
    @response = solr(request)
    
    #TODO: call response.field_facets(??) - maybe field_facets should be return a higher level? 
  end
  
  def facet
    @facets = retrieve_field_facets(params[:field_name])
  end
  
  def auto_complete_for_search_query
    # TODO instead of "text", default to the default search field configured in schema.xml
    @values = retrieve_field_facets("text", 5, params['search']['query'].downcase)
    
    render :partial => 'suggest'
  end

  def add_query
    session[:queries] << {:query => params[:search][:query]}
    session[:page] = 1
    redirect_to :action => 'index'
  end
  
  def update_query
    logger.debug "update_query: #{params.inspect}"
    session[:queries][params[:index].to_i][:query] = params[:value]
    session[:page] = 1
    render :update do |page|
      page.redirect_to '/browse'
    end
  end

  def invert_query
    q = session[:queries][params[:index].to_i]
    q[:negative] = !q[:negative]
    session[:page] = 1
    redirect_to :action => 'index'
  end

  def remove_query
    session[:queries].delete_at(params[:index].to_i)
    session[:page] = 1
    redirect_to :action => 'index'
  end

  def invert_filter
    f = session[:filters][params[:index].to_i]
    f[:negative] = !f[:negative]
    session[:page] = 1
    redirect_to :action => 'index'
  end
  
  def remove_filter
    session[:filters].delete_at(params[:index].to_i)
    session[:page] = 1
    redirect_to :action => 'index'
  end
  
  def add_filter
    session[:filters] << {:field => params[:field_name], :value => params[:value], :negative => (params[:negative] ? true : false)} 
    session[:page] = 1
    redirect_to :action => 'index'
  end
  
  def clear
    session[:queries] = nil
    session[:filters] = nil
    session[:page] = 1
    flare_before
    redirect_to :action => 'index'
  end
  
  def save
    session[:saved] ||= {}
    session[:saved][params[:name]] = {:filters => session[:filters], :queries => session[:queries]}
    redirect_to :action => 'index'
  end
  
  private
  def flare_before
    session[:queries] ||= [] 
    session[:filters] ||= []
    session[:page] ||= 1
  end
  
  def retrieve_field_facets(field, limit=-1, prefix=nil)
    req = Solr::Request::Standard.new(:query => query,
       :filter_queries => filters,
       :facets => {:fields => [field],
                   :mincount => 1, :limit => limit, :prefix => prefix, :missing => true, :sort => :count
                  },
       :rows => 0
    )
    
    results = SOLR.send(req)
    
    results.field_facets(field)
  end
  
  def make_query(constraints)
    queries = constraints[:queries]
    if queries.nil? || queries.empty?
      query = "*:*"
    else
      query = session[:queries].collect{|q| "#{q[:negative] ? '-' : ''}(#{q[:query]})"}.join(' AND ')
    end
    
    filter = constraints[:filters].collect do |filter|
      value = filter[:value]
      if value != "[* TO *]"
        value = "\"#{value}\""
      end
      "#{filter[:negative] ? '-' : ''}#{filter[:field]}:#{value}"
    end.join(" AND ")
    
    "#{query} AND #{filter}"
  end
  
end

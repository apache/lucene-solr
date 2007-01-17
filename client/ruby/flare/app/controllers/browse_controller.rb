# Copyright:: Copyright (c) 2007 Apache Software Foundation
# License::   Apache Version 2.0 (see http://www.apache.org/licenses/)

class BrowseController < ApplicationController
  before_filter :setup_session

  def index
    request = Solr::Request::Standard.new :query => session[:queries].join(' AND '), :filter_queries => session[:filters]
    @results = SOLR.send(request).data
  end
  
  def facet
    @field = "#{params[:field]}_facet"
    req = Solr::Request::Standard.new(:query => session[:queries].join(' AND '),
       :filter_queries => session[:filters],
       :facets => {:fields => [@field],
                   :mincount => 1, :sort => :count
                  },
       :rows => 0
    )
    
    results = SOLR.send(req)
    
    @facets = results.data['facet_counts']['facet_fields'][@field]
  end
  
  def add_query
    if session[:queries].size == 1
      session[:queries] = [] if session[:queries][0] == "[* TO *]"
    end
    session[:queries] << params[:query]
    redirect_to :action => 'index'
  end
  
  def add_filter
    session[:filters] << "#{params[:field]}:#{params[:value]}"
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
    session[:queries] = ["[* TO *]"] if session[:queries] == nil || session[:queries] .empty?
    session[:filters] ||= []
  end
end

class I18nController < ApplicationController
  def index
    @results = SOLR.query("acute").hits
  end
end
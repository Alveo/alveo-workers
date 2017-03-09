$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../lib")

require 'rsolr'
require 'active_record'
require 'models/item'
require 'models/document'
require 'models/collection'
require 'models/user'
require 'sesame_client'

def main(collection_name)
  require 'yaml'
  config = YAML.load_file("#{File.dirname(__FILE__)}/../config.yml")
  
  # Delete Solr records
  begin
    solr = RSolr.connect(url: config[:solr_worker][:url])
    solr.delete_by_query "collection_name_facet:#{collection_name}"
    solr.commit
  rescue Exception => e  
    puts "Failed clearing Solr records"
    puts e.message  
    # puts e.backtrace.inspect
  end
  
  # Delete Postgres records
  begin
    ActiveRecord::Base.establish_connection(config[:postgres_worker][:activerecord])
    if collection = Collection.where(name: collection_name).first
      Item.where(collection_id: collection.id).delete_all
      collection.delete
    end
  rescue Exception => e  
    puts "Failed clearing Postgres records (already done?)"
    puts e.message  
    # puts e.backtrace.inspect
  end
  
  # Delete Sesame records
  begin
    sesame = SesameClient.new(config[:sesame_worker])
    sesame.clear_repository(collection_name)
    sesame.close
  rescue Exception => e  
    puts "Failed clearing Sesame records"
    puts e.message  
    # puts e.backtrace.inspect
  end
end

if __FILE__ == $PROGRAM_NAME
  main(ARGV[0])
end

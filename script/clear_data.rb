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
  solr = RSolr.connect(url: config[:solr_worker][:url])
  solr.delete_by_query "collection_name_facet:#{collection_name}"
  solr.commit
  ActiveRecord::Base.establish_connection(config[:postgres_worker][:activerecord])
  if collection = Collection.where(name: collection_name).first
    Item.where(collection_id: collection.id).delete_all
    #Delete all documents that point to invalid items.
    sql = "DELETE FROM documents WHERE item_id NOT IN (SELECT id FROM items);"
    ActiveRecord::Base.connection.execute(sql)
  end
  sesame = SesameClient.new(config[:sesame_worker])
  sesame.clear_repository(collection_name)
  sesame.close
end


if __FILE__ == $PROGRAM_NAME
  main(ARGV[0])
end

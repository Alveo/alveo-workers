require 'rack'

module NewPostgresHelper

  # @@MIME_TYPE = Hash.new('application/octet-stream').merge({
  #   '.txt' => 'text/plain',
  #   '.xml' => 'text/xml',
  #   '.jpg' => 'image/jpeg',
  #   '.tif' => 'image/tif',
  #   '.mp3' => 'audio/mpeg',
  #   '.wav' => 'audio/wav',
  #   '.avi' => 'video/x-msvideo',
  #   '.mov' => 'video/quicktime',
  #   '.mp4' => 'video/mp4',
  #   '.doc' => 'application/msword',
  #   '.pdf' => 'application/pdf'
  # }).freeze

  def create_pg_statement(item_json_ld)
    item = extract_item_info(item_json_ld)
    documents = extract_documents_info(item_json_ld)
    {item: item, documents: documents}
  end

  def extract_item_info(item_json_ld)
    item = {}
    item[:uri] = item_json_ld['alveo:metadata']['@id']
    item[:handle] = item_json_ld['generated']['handle']
    item[:collection_id] = item_json_ld['generated']['collection_id']
    item[:primary_text_path] = item_json_ld['alveo:metadata']['alveo:display_document']
    item[:json_metadata] = item_json_ld
    # TODO: This is a temporary hack, what should happen is that this remains blank
    # until the Item is indexed  by the Solr worker, which should add update messages
    # to the Postgres queue. This could run into issues if it is indexed in Solr before
    # it is added to Postgres. Perhaps just keep requeueing until it's created?
    item[:indexed_at] = Time.now
    item
  end

  def extract_documents_info(item_json_ld)
    documents = []
    item_json_ld['ausnc:document'].each { |document_json_ld|
      document = extract_document_info(document_json_ld)
      documents << document
    }
    documents
  end

  def extract_document_info(document_json_ld)
    document = {}
    document[:file_name] = document_json_ld['dc:identifier']
    document[:file_path] = document_json_ld['dc:source']
    document[:doc_type] = document_json_ld['dc:type']
    document[:mime_type] = Rack::Mime.mime_type(document_json_ld['dc:identifier'])
    document
  end


end
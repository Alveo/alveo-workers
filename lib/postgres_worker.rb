require 'active_record'
require 'activerecord-import'

require_relative 'worker'
require_relative 'models/item'
require_relative 'models/document'
require_relative 'postgres_helper'
require 'easy_logging'

class PostgresWorker < Worker

  include PostgresHelper
  include EasyLogging

  def initialize(options)
    logger.debug "#initialize"

    rabbitmq_options = options[:rabbitmq]
    super(rabbitmq_options)
    @activerecord_options = options[:activerecord]
    @batch_options = options[:batch].freeze
    if @batch_options[:enabled]
      @batch = []
      @item_headers = [:uri, :handle, :collection_id, :primary_text_path, :json_metadata, :indexed_at]
      @item_batch = []
      @documents_headers = [:file_name, :file_path, :doc_type, :mime_type, :item_id]
      @documents_batch = []
      @batch_mutex = Mutex.new
    end
  end

  def start_batch_monitor
    logger.debug "#start_batch_monitor"

    @batch_monitor = Thread.new {
      loop {
        sleep @batch_options[:timeout]
        commit_batch
      }
    }
  end

  def start
    super
    if @batch_options[:enabled]
      start_batch_monitor
    end
  end

  def stop
    super
    if @batch_options[:enabled]
      @batch_monitor.kill
      commit_batch
    end
  end

  def connect
    super
    # TODO: change this to a connection pool perhaps
    ActiveRecord::Base.establish_connection(@activerecord_options)
  end

  def close
    super
    ActiveRecord::Base.connection.close
  end

  def commit_batch
    logger.debug "#commit_batch"

    @batch_mutex.synchronize {
      begin
        item_imports = Item.import @item_headers, @item_batch, validate: false
        logger.info "commit_batch: [#{item_imports.ids.size}] item(s) imported"

        # the inserted items
        item_ids = item_imports.ids

        # failed items
        item_failed_instances = item_imports.failed_instances

        if !item_failed_instances.empty?
          handles = []
          item_failed_instances.each do |fi|
            # failed items, duplicated ones?
            # fi is item (model)
            handles << fi.handle
          end

          logger.error "commit_batch: failed item(s) handle[#{handles}]"
        end

        documents = []
        item_ids.each_with_index { |id, i|
          @documents_batch[i].each { |document|
            document << id
            documents << document
          }
        }
        doc_imports = Document.import @documents_headers, documents, validate: false
        logger.info "commit_batch: [#{doc_imports.ids.size}] document(s) imported"
      rescue Exception => e
        #     log error and raise again
        logger.error "commit_batch: message[#{e.message}], backtrace[#{e.backtrace}]"
        raise e
      ensure
        @item_batch.clear
        @documents_batch.clear
      end
    }
  end

  def process_message(headers, message)
    logger.debug "#process_message"

    if headers['action'] == 'create'
      pg_statement = create_pg_statement(message)
      if @batch_options[:enabled]
        batch_create(pg_statement)
      else
        create_item(pg_statement)
      end
    end
  end

  #
  # Validate item:
  #
  # - check duplicate by handle
  #
  def validate_item(item)
    rlt = false

    handle = item[:handle]
    item = Item.find_by_handle(handle)
    if item.nil?
    #   item not exists, valid
      rlt = true
    else
    #   duplicated item
      logger.warn "validate_item: duplicated item[#{handle}]"
    end

    rlt
  end

  def batch_create(pg_statement)
    logger.debug "#batch_create"

    # TODO: change it array import method and turn off validations to
    # maximise import speed, see:
    #
    # https://github.com/zdennis/activerecord-import/wiki/Examples
    # require 'pry'
    # binding.pry
    # TODO: Not currently hanndling associations on mass import
    # will have to mass import items first, then assign the returned
    # ids to the documents
    #
    @batch_mutex.synchronize {
      if validate_item(pg_statement[:item])
      #   valid item
        @item_batch << pg_statement[:item].values
      else
      #   invalid item
        logger.warn "ignore invalid item[#{pg_statement[:item].values}]"
      end

    }
    # @documents_batch << [pg_statement[:documents].first.values]
    document_values = []
    pg_statement[:documents].each { |document|
      document_values << document.values
    }
    @documents_batch << document_values

    logger.debug "batch_create: item_size[#{@item_batch.size}], doc_size[#{@documents_batch.size}], batch_size[#{@batch_options[:size]}]"

    if (@item_batch.size >= @batch_options[:size])
      commit_batch
    end

  end

  def create_item(pg_statement)
    logger.debug "create_item"

    begin
      item = Item.new(pg_statement[:item])
      item.documents.build(pg_statement[:documents])
      item.save!
    rescue Exception => e
      # log the error only, don't need to process at this stage
      logger.error "create_item: pg_statement[#{pg_statement}], errors[#{e.message}]"
    end

  end

end
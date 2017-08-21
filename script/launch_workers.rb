$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../lib")

require 'yaml'
require 'optparse'
require 'ostruct'
require 'upload_worker'
require 'solr_worker'
require 'sesame_worker'
require 'postgres_worker'
require 'easy_logging'

include EasyLogging

def main(options)
  worker_classes = options.worker_classes
  config = options.config
  workers = []
  pids = []
  Process.setproctitle('WorkerMaster')
  begin
    p 'Starting workers...'
    if options.daemonize
      Process.daemon(nochdir=true)
    end
    worker_classes.each { |worker_class|
      config_key = worker_class.name.underscore.to_sym
      config[:worker_launcher][config_key][:processes].times {
        pids << fork {
          Process.setproctitle(worker_class.name)
          worker = worker_class.new(config[config_key])
          running = true
          worker.connect
          worker.start
          workers << worker
          Signal.trap('TERM') {
           running = false
          }
          while running do
            sleep 1
          end
          worker.stop
          worker.close
        }
      }
    }
  Process.waitall
  rescue SignalException
    p 'Stopping workers...'
    pids.each { |pid|
      Process.kill('TERM', pid)
    }
  end
end

class String
  def underscore
    self.gsub(/::/, '/').
    gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2').
    gsub(/([a-z\d])([A-Z])/,'\1_\2').
    tr("-", "_").
    downcase
  end
end

def parse_options(args)
  parsed_options = OpenStruct.new
  parsed_options.daemonize = false
  parsed_options.processes = 1
  parsed_options.worker_classes = [UploadWorker, SolrWorker, SesameWorker, PostgresWorker]
  parsed_options.config = YAML.load_file("#{File.dirname(__FILE__)}/../config.yml")

  EasyLogging.log_destination = parsed_options.config[:common][:worker_log]

  #TODO: Add option for specifying a config files
  option_parser = OptionParser.new do |options|
    options.banner = "Usage: launch_workers.rb [options]"
    options.separator ""
    options.separator "Specific options:"
    options.on('-w', '--workers (upload|solr|sesame|postgres)+', 'Comma separated list of workers to launch (default=all)') do |workers|
      worker_classes = []
      workers.split(',').each { |worker|
        worker_classes << Module.const_get("#{worker.capitalize}Worker")
      }
      parsed_options.worker_classes = worker_classes
    end
    options.on('-d', '--daemon', 'Run as background daemon') do
      parsed_options.daemonize = true
    end
    options.on('-h', '--help', 'Show this help message') do
      puts option_parser  
      exit
    end
  end
  begin
    option_parser.parse!(args)  
  rescue
    puts option_parser
  end
  parsed_options
end

if __FILE__ == $PROGRAM_NAME
  options = parse_options(ARGV)
  main(options)
end
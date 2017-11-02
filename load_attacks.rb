#!/usr/bin/env ruby

require 'csv'
require 'httparty'
require_relative 'vars.rb'

timestamp = Time.now.strftime('%Y_%m_%d__%H_%M_%S')
working_dir = "/var/lib/mysql-files"
input_csv = "origins_msbio_etl_attacks.csv"
log_directory = "/home/arenschen/origins_msbio_pipeline/logs"
transaction_log = "create_new_attacks_transaction_log_#{timestamp}.txt"

csv_text = File.read(File.join(working_dir, input_csv))
csv = CSV.parse(csv_text, :headers => true)

File.open(File.join(log_directory,transaction_log),'w') do |file|
  csv.each do |row|
    file.write("POST: #{row.to_hash.to_json}\n")
    @response = HTTParty.post("#{@production_root_url}#{@attacks_url}", :body => row.to_hash.to_json, :headers => { "Content-type"=>"application/json", "Authorization"=>'Token token=""'})
    file.write("RESPONSE:\n#{@response.headers["status"]}\n#{@response}\n-------------------------------------------------------------------\n")
    unless @response.headers["status"] == "201 Created"
        file.write("PUT: #{row.to_hash.to_json}\n")
        @put_response = HTTParty.put("#{@production_root_url}#{@attacks_put_url}#{row["external_identifier"]}", :body => row.to_hash.to_json, :headers => { "Content-type"=>"application/json", "Authorization"=>'Token token=""'})
        file.write("RESPONSE:\n#{@put_response.headers["status"]}\n#{@put_response}\n-------------------------------------------------------------------\n")
    end
  end
end

#!/usr/bin/env ruby

require 'csv'
require 'httparty'
require_relative 'vars.rb'

timestamp = Time.now.strftime('%Y_%m_%d__%H_%M_%S')
working_dir = "/var/lib/mysql-files"
input_csv = "origins_msbio_etl_treatments.csv"
log_directory = "/home/arenschen/origins_msbio_pipeline/logs"
transaction_log = "create_new_treatments_transaction_log_#{timestamp}.txt"

csv_text = File.read(File.join(working_dir, input_csv))
csv = CSV.parse(csv_text, :headers => true)

File.open(File.join(log_directory, transaction_log),'w') do |file|
  csv.each do |row|
    #in msbase db, some treatments only have month/year for start or end dates. The following code checks for a year-month-day format, and adjusts the date so that treatments start on the first of a month and end on the last day of the month (eom = end of month) if there is no day specified
    unless row["start_date"].match(/\d{4}\-\d{2}\-\d{2}/)
      arr = row["start_date"].split("\/")
      if arr[0].size < 2
        arr[0] = "0#{arr[0]}"
      end
      row["start_date"] = "#{arr[1]}-#{arr[0]}-01"
    end
    unless row["end_date"].match(/\d{4}\-\d{2}\-\d{2}/)
      arr = row["end_date"].split("\/")
      if arr[0].size < 2
        arr[0] = "0#{arr[0]}"
      end
      eom = {"01"=>"31", "02"=>"28", "03"=>"31", "04"=>"30", "05"=>"31", "06"=>"30", "07"=>"31", "08"=>"31", "09"=>"30", "10"=>"31", "11"=>"30", "12"=>"31"}
      row["end_date"] = "#{arr[1]}-#{arr[0]}-#{eom[arr[0]]}"
    end
    file.write("POST:\n#{row.to_hash.to_json}\n")
    @response = HTTParty.post("#{@production_root_url}#{@treatments_url}", :body => row.to_hash.to_json, :headers => { 'Content-type'=>'application/json', 'Authorization'=>'Token token=""'})
    file.write("RESPONSE:\n#{@response.headers["status"]}\n#{@response}\n--------------------------------------------------------------\n")
    unless @response.headers["status"] == "201 Created"
      file.write("PUT: #{row.to_hash.to_json}\n")
      @put_response = HTTParty.put("#{@production_root_url}#{@treatments_put_url}#{row["external_identifier"]}", :body => row.to_hash.to_json, :headers => { "Content-type"=>"application/json", "Authorization"=>'Token token=""'})
      file.write("RESPONSE:\n#{@put_response.headers["status"]}\n#{@put_response}\n-------------------------------------------------------------------\n")
    end  
  end
end


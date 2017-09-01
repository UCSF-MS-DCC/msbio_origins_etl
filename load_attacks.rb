require 'csv'
require 'httparty'
require_relative 'vars.rb'

timestamp = Time.now.strftime('%Y_%m_%d__%H_%M_%S')
working_dir = "#{@working_dir_base}#{@working_dir}"
input_csv = "#{@csv_prefix}attacks.csv"
log_filename = "attacks_log_#{timestamp}.txt"
transaction_log = "attacks_transaction_log_#{timestamp}.txt"

failed_ids = []
successful_ids = []

# @emr = {"2101" => "52537", "2102" => "899097", "2103" => "898422", "2104" => "924920", "2105" => "46700", "2106" => "898562", "2107" => "962600", "2110" => "780076" } #some origins are in the database as EMR patients. This hash maps epic id number with msbio database external identifiers.

csv_text = File.read(File.join(working_dir, input_csv))
csv = CSV.parse(csv_text, :headers => true)

File.open(File.join(working_dir, transaction_log), 'w') { |file|

  csv.each do |row|
    row["source_id"] = 6
    row["patient_source_id"] = 6

    file.write("POST:\n#{row.to_hash.to_json}\n")
    @response = HTTParty.post("#{@production_root_url}#{@attacks_url}", :body => row.to_hash.to_json, :headers => { 'Content-type':'application/json', 'Authorization':'Token token=""'})
    file.write("RESPONSE:\n#{@response.headers["status"]}\n#{@response}\n------------------------------------------\n")

    if @response.headers["status"] != "201 Created"
      failed_ids.push([row["external_identifier"], @response.headers["status"]])
    else
      successful_ids.push(row["external_identifier"])
    end
  end
}

File.open(File.join(working_dir, log_filename), 'w') { |file|
  file.write("Process started: #{timestamp}\n")
  file.write("Origins attacks upload report\n")
  file.write("Successful uploads of attacks: #{successful_ids.size}\n")
  file.write("Failed uploads of attacks: #{failed_ids.size}\n")
  file.write("Failed ids:\n")
  failed_ids.each do |fid|
    file.write("#{fid}\n")
  end
}

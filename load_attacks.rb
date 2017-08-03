require 'csv'
require 'httparty'
require_relative 'vars.rb'

timestamp = Time.now.strftime('%Y_%m_%d')
working_dir = "#{@working_dir_base}#{@working_dir}"
input_csv = "attacks.csv"
log_filename = "attacks_log_#{timestamp}.txt"

failed_ids = []
successful_ids = []

@emr = {"2101" => "52537", "2102" => "899097", "2103" => "898422", "2104" => "924920", "2105" => "46700", "2106" => "898562", "2107" => "962600", "2110" => "780076" } #some origins are in the database as EMR patients. This hash maps epic id number with msbio database external identifiers.

csv_text = File.read(File.join(working_dir, input_csv))
csv = CSV.parse(csv_text, :headers => true)

csv.each do |row|
  row["source_id"] = 2 # regardless of EMR or origins patient source, putting all attacks in as origins
  row["external_identifier"] = "2000" + row["external_identifier"] #attack id. modifying from the origins database id to avoid conflicts with epic attack ids

  if @emr[row["patient_external_identifier"]]
    row["patient_external_identifier"] = @emr[row["patient_external_identifier"]] #patient id. Some origins patients are in the database as EMR with ids that don't follow the convention for most of the origins patients. See hash above.
    row["patient_source_id"] = 1 # EMR source id number.
  else
    old_id = row["patient_external_identifier"]
    row["patient_external_identifier"] = "2000" + old_id #patient id. The naming convention for this fiels (excepting origins-EMR patients) is 2000 + EPIC id number (ie EPIC2199 would be 20002199)
    row["patient_source_id"] = 2 # EPIC source id number.
  end

  @response = HTTParty.post("#{@production_root_url}#{@attacks_url}", :body => row.to_hash.to_json, :headers => { 'Content-type':'application/json', 'Authorization':'Token token=""'})

  if @response.headers["status"] != "201 Created"
    failed_ids.push([row["external_identifier"], @response.headers["status"]])
  else
    successful_ids.push(row["external_identifier"])
  end
end

File.open(File.join(working_dir, log_filename), 'w') { |file|
  file.write("#{timestamp}\n")
  file.write("Origins attacks upload report\n")
  file.write("Successful uploads of attacks: #{successful_ids.size}\n")
  file.write("Failed uploads of attacks: #{failed_ids.size}\n")
  file.write("Failed ids:\n")
  failed_ids.each do |fid|
    file.write("#{fid}\n")
  end
}

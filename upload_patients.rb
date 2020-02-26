#!/usr/bin/env ruby

require 'csv'
require 'httparty'
require_relative 'vars'

=begin
    The current algorithm for updating the bioscreen API is to run SQL queries on msbase epic tables to generate csv files of epic patients, visits, attacks, and treatments,
    renaming certain varialbes to match columns in msbioscreen tables.
    This script is designed to ingest subject data csv files, turn each row into JSON-formatted data bundle and send via https request to the champagne API subjects endpoint.

=end

dataSourceDir = "/var/lib/mysql-files"
inputCSV = "epic_msbio_etl_patients.csv"
logDirectory = "/home/arenschen/epic_msbio_etl/process_logs"
fullTransactionLog = "patientsFTLog.txt"
thinTransactionLog = "patientsTTLog.txt"

csvText = File.read(File.join(dataSourceDir, inputCSV))
csv = CSV.parse(csvText, :headers => true)

File.open(File.join(logDirectory,fullTransactionLog),'w') do |ftlog|
  File.open(File.join(logDirectory,thinTransactionLog),'w') do |ttlog|
    transactionIdx = 0
    csv.each do |row|
        # the genetics table column gb_of_c_stat3 and others may contain floats written in exponential notation (i.e. with a "E-" at the end of the digit string). Cast to float to avoid upsertion rejection.
        row["gb_of_c_stat3"] = row["gb_of_c_stat3"].to_f
        row["gb_of_c_cd40"] = row["gb_of_c_cd40"].to_f
        row["gb_of_a_eps15l1"] = row["gb_of_a_eps15l1"].to_f

        # temporary fix for demographic_info.handedness validation hiccup
        row["handedness"] = row["handedness"] == "U" ? "" : row["handedness"]
        # set some variables for reporting
        transactionIdx += 1
        subjectSource = row["source_id"]
	subjectExternalID = row["external_identifier"]
        postResponseCode = "n/a"
	putResponseCode = nil
	postResponseMessage = "n/a"
	putResponseMessage = nil
	dataRowToJSON = row.to_hash.to_json
	
	# First attempt to update a patient. If this succeeds, no need for an insert/POST request
	putResponse = HTTParty.put("https://prod.champagne.ucsf.edu/api/v1/sources/#{row['source_id']}/subjects/#{row['external_identifier']}", :body => { :patient => row.to_hash}.to_json, :headers => { "Content-type"=>"application/json", "Authorization"=>Vars::API_KEY})
	putResponseCode = putResponse.code
	if putResponseCode == 202 # subject exists and was successfully updated
		ftlog.write("#{transactionIdx})\tsource_id:#{subjectSource}\tEPIC ID:#{subjectExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\nData:#{dataRowToJSON}\n\n")
		ttlog.write("#{transactionIdx})\tsource_id:#{subjectSource}\tEPIC ID:#{subjectExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\n")
	elsif putResponseCode == 422 # subject was not found, attempt to create new subject
       		postResponse = HTTParty.post("https://prod.champagne.ucsf.edu/api/v1/subjects", :body => {:patient => row.to_hash}.to_json, :headers => { 'Content-type'=>'application/json', 'Authorization'=>Vars::API_KEY})
		postResponseCode = postResponse.code
		if postResponseCode == 201 # subject was successfully created
			ftlog.write("#{transactionIdx})\tsource_id:#{subjectSource}\tEPIC ID:#{subjectExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\nData:#{dataRowToJSON}\n\n")
			ttlog.write("#{transactionIdx})\tsource_id:#{subjectSource}\tEPIC ID:#{subjectExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\n")
		else # something went wrong, log all errors
			postResponseMessage = postResponse.message
			ftlog.write("#{transactionIdx})\tsource_id:#{subjectSource}\tEPIC ID:#{subjectExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{postResponseMessage}\nData:#{dataRowToJSON}\n\n")
			ttlog.write("#{transactionIdx})\tsource_id:#{subjectSource}\tEPIC ID:#{subjectExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{postResponseMessage}\n")
		end
	else #something went wrong, log all errors
		putResponseMessage = putResponse.message
		ftlog.write("#{transactionIdx})\tsource_id:#{subjectSource}\tEPIC ID:#{subjectExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{putResponseMessage}\nData:#{dataRowToJSON}\n\n")
		ttlog.write("#{transactionIdx})\tsource_id:#{subjectSource}\tEPIC ID:#{subjectExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{putResponseMessage}\n")
	end
end # close csv.each do block
end # close ttlog block
end # close ftlog block
	
=begin
    HTTPARTY reference
    response.code == 200, 404, etc
    response.message == "Accepted", "Unauthorized", etc
    response.body
    response.headers
    response.headers.inspect
=end

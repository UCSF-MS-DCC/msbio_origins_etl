#!/usr/bin/env ruby

require 'csv'
require 'httparty'
require_relative 'vars'

dataSourceDir = "/var/lib/mysql-files"
inputCSV = "epic_msbio_etl_visits.csv"
logDirectory = "/home/arenschen/epic_msbio_etl/process_logs"
fullTransactionLog = "visitsFTLog.txt"
thinTransactionLog = "visitsTTLog.txt"

csvText = File.read(File.join(dataSourceDir, inputCSV))
csv = CSV.parse(csvText, :headers => true)

File.open(File.join(logDirectory, fullTransactionLog),'w') do |ftlog|
File.open(File.join(logDirectory, thinTransactionLog),'w') do |ttlog|
	transactionIdx = 0
	csv.each do |row|
   		sourceID = row["patient_external_identifier"].to_i < 2100 ? 2 : 6 
		visitData = {"external_identifier" => row["external_identifier"].to_i,
	                  "patient_external_identifier" => row["patient_external_identifier"].to_i,
	                  "patient_source_id" => sourceID,
	                  "source_id" => sourceID,
	                  "date" => row["date"],
	                  "age_at_visit" => row["age_at_visit"],
	                  "msss" => row["msss"],
	                  "edss" => row["edss"],
	                  "fssc_visual" => row["fssc_visual"],
	                  "fssc_brainstem" => row["fssc_brainstem"],
	                  "fssc_pyramidal" => row["fssc_pyramidal"],
	                  "fssc_cerebellar" => row["fssc_cerebellar"],
	                  "fssc_sensory" => row["fssc_sensory"],
	                  "fssc_bowel" => row["fssc_bowel"],
	                  "fssc_mental" => row["fssc_mental"],
	                  "pasat_score" => row["pasat_score"],
	                  "nhpt_dominant_time" => row["nhpt_dominant_time"],
	                  "nhpt_nondominant_time" => row["nhpt_nondominant_time"],
	                  "timed_walk_trial1_time" => row["timed_walk_trial1_time"],
	                  "timed_walk_trial2_time" => row["timed_walk_trial2_time"],
			  "timed_walk_trial1_incomplete_details" => row["timed_walk_trial1_incomplete_details"],
			  "timed_walk_trial2_incomplete_details" => row["timed_walk_trial2_incomplete_details"],
	                  "disease_course" => row["disease_course"],
	                  "disease_duration" => row["disease_duration"],
			  "sdmt_score" => row["sdmt_score"],
			  "csf_igg" => row["csf_igg"],
			  "csf_ocb" => row["csf_ocb"],
			  "csf_cell_count" => row["csf_cell_count"],
			  "brain_volume" => row["brain_volume"],
			  "white_matter_volume" => row["white_matter_volume"],
			  "grey_matter_volume" => row["grey_matter_volume"],
			  "cortical_grey_matter_volume" => row["cortical_grey_matter_volume"],
			  "ventricular_csf_volume" => row["ventricular_csf_volume"],
			  "mri_date" => row["mri_date"],
			  "nhpt_nondominant_incomplete_details" => row["nhpt_nondominant_incomplete_details"],
			  "nhpt_dominant_incomplete_details" => row["nhpt_dominant_incomplete_details"],
       			  "ambulation_index" => row["ambulation_index"]
	                }.to_json
		# set some variables for reporting
		transactionIdx += 1
		subjectSourceID = row["source_id"]
		visitExternalID = row["external_identifier"]
		postResponseCode = "n/a"
		putResponseCode = nil
		postResponseMessage = "n/a"
		putResponseMessage = nil

		putResponse = HTTParty.put("https://prod.champagne.ucsf.edu/api/v1/sources/#{sourceID}/visits/#{row['external_identifier'].to_i}", :body => visitData, :headers => { "Content-type"=>"application/json", "Authorization"=>Vars::API_KEY})
		putResponseCode = putResponse.code
		if putResponseCode == 202 #accepted
			ftlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tVisit ID:#{visitExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\nData:#{visitData}\n\n")
			ttlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tVisit ID:#{visitExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\n")
		elsif putResponse.code == 422 #visit is not in database
    			postResponse = HTTParty.post("https://prod.champagne.ucsf.edu/api/v1/visits", :body => visitData, :headers => { 'Content-type'=>'application/json', 'Authorization'=>Vars::API_KEY})
			postResponseCode = postResponse.code
      			if postResponse.code == 201 # created
				ftlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tVisit ID:#{visitExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\nData:#{visitData}\n\n")
				ttlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tVisit ID:#{visitExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\n")
			else # something went wrong, log all errors
				postResponseMessage = postResponse.message
				ftlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tVisit ID:#{visitExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{postResponseMessage}\nData:#{visitData}\n\n")
				ttlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tVisit ID:#{visitExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{postResponseMessage}\n")
			end
		else # something went wrong, log all errors
			putResponseMessage = putResponse.message
			ftlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tVisit ID:#{visitExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{putResponseMessage}\nData:#{visitData}\n\n")
			ttlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tVisit ID:#{visitExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{putResponseMessage}\n")
		end
	end # close csv.each do block
  end # close open ttlog block
end # close open ftlog block

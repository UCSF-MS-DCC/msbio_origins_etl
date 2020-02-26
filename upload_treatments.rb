#!/usr/bin/env ruby

require 'csv'
require 'httparty'
require_relative 'vars'

dataSourceDir = "/var/lib/mysql-files"
inputCSV = "epic_msbio_etl_treatments.csv"
logDirectory = "/home/arenschen/epic_msbio_etl/process_logs"
fullTransactionLog = "treatmentsFTLog.txt"
thinTransactionLog = "treatmentsTTLog.txt"

csvText = File.read(File.join(dataSourceDir, inputCSV))
csv = CSV.parse(csvText, :headers => true)

File.open(File.join(logDirectory, fullTransactionLog),'w') do |ftlog|
File.open(File.join(logDirectory, thinTransactionLog),'w') do |ttlog|
	transactionIdx = 0
	csv.each do |row|
		#in msbase db, some treatments only have month/year for start or end dates. The following code checks for a year-month-day format, and adjusts the date so that treatments start on the first of a month and end on the last day of the month (eom = end of month) if there is no day specified
		unless row["start_date"].match(/\d{4}\-\d{2}\-\d{2}/)
			if row["start_date"].include? "\/"
				arr = row["start_date"].split("\/")
				if arr[0].size < 2
					arr[0] = "0#{arr[0]}"
         			end
         			row["start_date"] = "#{arr[1]}-#{arr[0]}-01"
       			end
    		end

    		unless row["end_date"].match(/\d{4}\-\d{2}\-\d{2}/)
      			if row["end_date"].include? "\/"
        			arr = row["end_date"].split("\/")
        			if arr[0].size < 2
          				arr[0] = "0#{arr[0]}"
        			end
        			eom = {"01"=>"31", "02"=>"28", "03"=>"31", "04"=>"30", "05"=>"31", "06"=>"30", "07"=>"31", "08"=>"31", "09"=>"30", "10"=>"31", "11"=>"30", "12"=>"31"}
        			row["end_date"] = "#{arr[1]}-#{arr[0]}-#{eom[arr[0]]}"
      			end
    		end

    		sourceID = row["patient_external_identifier"].to_i < 2100 ? 2 : 6
    		row["patient_source_id"] = sourceID
    		row["source_id"] = sourceID
		#set some variables for reporting 
		transactionIdx += 1
                subjectSourceID = sourceID
                treatmentExternalID = row["external_identifier"]
                postResponseCode = "n/a"
                putResponseCode = nil
                postResponseMessage = "n/a"
                putResponseMessage = nil
                treatmentData = row.to_hash.to_json

		putResponse = HTTParty.put("https://prod.champagne.ucsf.edu/api/v1/sources/#{sourceID}/treatments/#{row['external_identifier']}", :body => treatmentData, :headers => { "Content-type"=>"application/json", "Authorization"=>Vars::API_KEY})
		putResponseCode = putResponse.code
		if putResponseCode == 202 #success
			ftlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tTreatment ID:#{treatmentExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\nData:#{treatmentData}\n\n")
			ttlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tTreatment ID:#{treatmentExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\n")
		elsif putResponseCode == 422
			postResponse = HTTParty.post("https://prod.champagne.ucsf.edu/api/v1/treatments", :body => treatmentData, :headers => { 'Content-type'=>'application/json', 'Authorization'=>Vars::API_KEY})
			postResponseCode = postResponse.code
			if postResponseCode == 201 # created
				ftlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tTreatment ID:#{treatmentExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\nData:#{treatmentData}\n\n")
				ttlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tTreatment ID:#{treatmentExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\n")
			else # something went wrong, log errors
				postResponseMessage = postResponse.message
				ftlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tTreatment ID:#{treatmentExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{postResponseMessage}\nData:#{treatmentData}\n\n")
				ttlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tTreatment ID:#{treatmentExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{postResponseMessage}\n")
			end
		else #something went wrong, log errors
			putResponseMessage = putResponse.message
			ftlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tTreatment ID:#{treatmentExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{putResponseMessage}\nData:#{treatmentData}\n\n")
			ttlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tTreatment ID:#{treatmentExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{putResponseMessage}\n")
		end
  	end #close csv each do block
end #close file open ttlog block
end #close file open ftlog block



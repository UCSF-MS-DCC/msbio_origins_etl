#!/usr/bin/env ruby

require 'csv'
require 'httparty'
require_relative 'vars'

dataSourceDir = "/var/lib/mysql-files"
inputCSV = "epic_msbio_etl_attacks.csv"
logDirectory = "/home/arenschen/epic_msbio_etl/process_logs"
fullTransactionLog = "attacksFTLog.txt"
thinTransactionLog = "attacksTTLog.txt"

csvText = File.read(File.join(dataSourceDir, inputCSV))
csv = CSV.parse(csvText, :headers => true)

File.open(File.join(logDirectory,fullTransactionLog),'w') do |ftlog|
File.open(File.join(logDirectory,thinTransactionLog),'w') do |ttlog|

 	transactionIdx = 0
 
	csv.each do |row|
    		sourceID = row["patient_external_identifier"].to_i < 2100 ? 2 : 6
    		row["source_id"] = sourceID
    		row["patient_source_id"] = sourceID

		# set some variables for reporting
		transactionIdx += 1
		subjectSourceID = sourceID
		attackExternalID = row["external_identifier"]
		postResponseCode = "n/a"
		putResponseCode = nil
		postResponseMessage = "n/a"
		putResponseMessage = nil
		attackData = row.to_hash.to_json

       		putResponse = HTTParty.put("https://prod.champagne.ucsf.edu/api/v1/sources/#{sourceID}/attacks/#{row["external_identifier"]}", :body => attackData, :headers => { "Content-type"=>"application/json", "Authorization"=>Vars::API_KEY})
		putResponseCode = putResponse.code
                if putResponseCode == 202 #accepted
                        ftlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tAttack ID:#{attackExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\nData:#{attackData}\n\n")
                        ttlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tAttack ID:#{attackExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\n")
		elsif putResponseCode == 422 #attack not found
			postResponse = HTTParty.post("https://prod.champagne.ucsf.edu/api/v1/attacks", :body => attackData, :headers => { "Content-type"=>"application/json", "Authorization"=>Vars::API_KEY})
			postResponseCode = postResponse.code
			if postResponseCode == 201 #created
				ftlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tAttack ID:#{attackExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\nData:#{attackData}\n\n")
                        	ttlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tAttack ID:#{attackExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\n")
			else #something when wrong, log all errors
				postResponseMessage = postResponse.message
				ftlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tAttack ID:#{attackExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{postResponseMessage}\nData:#{attackData}\n\n")
                                ttlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tAttack ID:#{attackExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{postResponseMessage}\n")
			end
		else #something went wrong
			putResponseMessage = putResponse.message
			ftlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tAttack ID:#{attackExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{postResponseMessage}\nData:#{attackData}\n\n")
                        ttlog.write("#{transactionIdx})\tsource_id:#{subjectSourceID}\tAttack ID:#{attackExternalID}\tupdate code:#{putResponseCode}\tinsert code:#{postResponseCode}\terror message:#{postResponseMessage}\n")
		end
   	end #close csv.each do block
  end #close file open ttlog block
end #close file open ftlog block

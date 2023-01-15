# keybase-binding

* 3.0
	* Requirements
		* Export absolutely everything from keybase into a single zip file (Attachments separate)
	* Features
		* Do minimal processing on the data, create separate tools for metadata extraction
	* Epics
		* List of all teams and save to file
		* Export all messages from single team and save to file
			* Attachment downloading for team chats
		* Export all DM's and save to files
			* Attachment downloading for DM chats
		* List all git repos and save to file
		* Clone all git repos, including all branches, zip them up, delete source code
		* List all followers and save to file
		* List all following and save to file
		* Get metadata from all following and followers
		* List all shared and personal file folders as well as contents
		* Export all file folder into a directory's, zip directory's, delete all files
	* Design decisions
		* No need for sql databases this time, other tools can load this data in
Data files are too large to post on GitHub.

Most tables created purely for this analysis are persisted on Google Docs. These tables include:
* filtered_tape_mounts: Tape mounts with at least one write, and including the file family of the mounted volume. Used alongside the small_file_billinginfo_reads and Enstore file tables to calculate simulated host volumes of all small files with a read request on record.
* small_file_billinginfo_reads: Read requests for small files from the dCache billinginfo table including simulated host volume from above, used alongside Enstore's tape_mounts table to determine mount wait times for small file reads.
* storageinfo_sfa_stores: Write requests for small file writes from the dCache storageinfo table, including added file_family column from Enstore volume table, used along with filtered_tape_mounts to determine mount wait times for small file writes.

Other tables, persisted in the production Enstore DBs are the following:
* volume: Enstore table used to determine valid simulated host volumes of requests above according to file family.
* file: Enstore tsed to determine file family of requested files (reads and writes), whether or not a file is an SFA file, and file creation time (based on BFID timestamp).
* tape_mounts: Enstore table containing start and end time of all tape mounts, including number of files read and written for each mount.
* storageinfo: dCache table containing records of file writes to Enstore (this includes all non-SFA and SFA file requests), and read requests require retrieveing a file from Enstore (this does not include a lot of SFA file requests), for the last ~6 months.
* billinginfo: dCache table containing records of all read requests to dCache (this includes non-SFA and SFA read requests), for the last ~6 months.

If you are interested in reviewing the data, request access to [the Google Drive folder](https://drive.google.com/drive/folders/14XC3mC4r0ngTagiwPwMrDuZf5Ippqjgt?usp=sharing) with details.

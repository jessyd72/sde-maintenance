"""
Completes recommended geodatabase maintenance on a 
traditional versioned database using either an existing
connection string or a new connection string built during run. 
Connection file or requirements to build connection must be stored
in the config.json file. An environemnt must be specified in
the env.json file (dev, stage, or prod). 
Recommended geodatabase maintenance includes:

	- Reconciling and posting all child versions
	- Compressing the database to state 0
	- Rebuilding all indexes (spatial, attribute, and database)
	- Analzye datasets to recalculate statistics

Author: 
	J. Beasley, Geographic Technologies Group (GTG)

Version History:
	12/15/2022	|	J. Beasley	|	Script created
"""


# imports
import arcpy
import datetime
import json
import logging 
import os
import sys

arcpy.env.overwriteOutput = True


def buildCxn(cfg):
	'''
	Reads json dictionary for database connection inforamtion.
	If a connection string is provided, that will be prioritized. 
	If no connection string is provided, a new connection will be 
	built using provided parameters in config file. 
	The json dictionary is expected to have the following keys:
		sde_cxn, rdbms, instance, auth, un, pw, db_name, verison
	Args:
		cfg: JSON dict containing the following keys at minimum- 
			sde_cxn, rdbms, instance, auth, un, pw, db_name, verison.
	Returns:
		sde: path to sde (Enterprise geodatabase) connection file.
		built: Boolean indicating if the connection file was created 
			in this function. Used to determine if file can be removed
			after database maintenance completes. 
	'''

	logging.info("Starting to build connection file")

	# Set database connection
	built = 0
	db_cxn_vars = [cfg[x] for x in ["rdbms", "instance", "auth", "un", "pw", "db_name"]]
	try:
		if cfg["sde_cxn"]:
			# if cxn string provided, use that
			sde = cfg["sde_cxn"]
			logging.info(f"Using connection string: {sde}")
		elif all(db_cxn_vars):
			# if no cxn string and all required cxn params
			# provied, build new cxn string
			logging.info("Buidling new connection string")
			rdbms, instance, auth, un, pw, db_name = db_cxn_vars
			version = cfg["version"]
			try:
				arcpy.CreateDatabaseConnection_management(sde_cxn_fldr,
													f"{un}@{db_name}", 
													rdbms, 
													instance, 
													auth, 
													un, 
													pw, 
													database=db_name,
													version=version)
				sde = os.path.join(sde_cxn_fldr, f"{un}@{db_name}.sde")
				logging.info(f"Using connection string: {sde}")
				built = 1
			except Exception as e:
				logging.error(e)
		else:
			# cannot connect to database
			logging.error("Missing required information to connect to database. \n \
				Confirm the env and config files contain either a connection \n \
				string or information to build a new connection file.")
			sys.exit(1)
	except KeyError:
		logging.error("Key missing from config file. \n \
			The config (json) must contain the following keys, regardless if the \
			key has a value: sde_cxn, rdbms, instance, auth, un, pw, db_name, verison")
		sys.exit(1)
	except Exception as e:
		logging.error(e)
		sys.exit(1)

	return(sde, built)


def reconcileVersions(sde):
	'''
	Using the input database connection file, gets list of all versions 
	except DEFAULT for reconiliation. 
	First, all versions are recondiled to their targets without post. If
	first reconciliation completes, a second reconiliation occurs with 
	posting to their target. Versions are maintained. 
	Args:
		sde: connection file to sde (enterprise geodatabase). 
	'''

	logging.info("Starting to reconcile versions")

	# Set the workspace environment
	arcpy.env.workspace = sde

	# Use a list comprehension to get a list of version names where the owner
	# is the current user and make sure sde.default is not selected.
	# removed ver.isOwner == True, TODO: determine if this works for Solano
	logging.info("Listing all child versions")
	verList = [ver.name for ver in arcpy.da.ListVersions() if 
				ver.name.lower() != 'sde.default'] 
	
	logging.info(f"    versions: {*verList,}")

	logging.info('Starting the 1st reconciliation')

	arcpy.ReconcileVersions_management(sde,
									"ALL_VERSIONS",
									"SDE.Default",
									verList,
									"LOCK_ACQUIRED",
									"NO_ABORT",
									"BY_OBJECT", #TODO: look into for conflicts
									"FAVOR_TARGET_VERSION",
									"NO_POST",
									"KEEP_VERSION",
									f"{log_fldr}\RecLog_{timestamp}.txt")
	logging.info('Reconciling part 1 complete')

	logging.info('Starting the 2nd reconciliation with post')

	arcpy.ReconcileVersions_management(sde,
									"ALL_VERSIONS",
									"SDE.Default",
									verList,
									"LOCK_ACQUIRED",
									"NO_ABORT",
									"BY_OBJECT",
									"FAVOR_TARGET_VERSION",
									"POST", 
									"KEEP_VERSION",
									f"{log_fldr}\RecLog_wPost_{timestamp}.txt")

	logging.info('Reconciling part 2 complete')
	logging.info('Versions have been posted after reconciliation')


def compressDB(sde):
	'''
	Using the input database connection file, compresses datbaase. 
	Args:
		sde: connection file to sde (enterprise geodatabase). 
	'''

	logging.info("Starting database compression")

	# The database connection file that connects to the enterprise geodatabase to be compressed.
	arcpy.Compress_management(sde) 

	logging.info('Compression complete')


def rebuildIndex(sde):
	'''
	Using the input database connection file, lists all datasets within
	database, including feature classes and datasets within feature datasets.
	For each dataset that is owned by the user the database connection is
	configured, the indexes will be rebuilt. Includes ALL indexes (spatial, 
	attribute, and database). 
	Args:
		sde: connection file to sde (enterprise geodatabase). 
	'''

	logging.info("Starting to rebuild indexes")

	# Set the workspace environment
	arcpy.env.workspace = sde

	# Get a list of all the datasets the user has access to.
	# First, get all the stand alone tables, feature classes and rasters.
	dataList = arcpy.ListTables() + arcpy.ListFeatureClasses() + arcpy.ListRasters()

	# Next, for feature datasets get all of the datasets and featureclasses
	# from the list and add them to the master list.
	for dataset in arcpy.ListDatasets("", "Feature"):
		arcpy.env.workspace = os.path.join(sde, dataset)
		dataList += arcpy.ListFeatureClasses() + arcpy.ListDatasets()

	# Reset the workspace
	arcpy.env.workspace = sde

	# Get the user name for the workspace
	userName = arcpy.Describe(sde).connectionProperties.user.lower()

	logging.info(f"Filtering dataset list to those owned by {userName}")
	# remove any datasets that are not owned by the connected user.
	userDataList = [ds for ds in dataList if ds.lower().find(f".{userName}.") > -1]

	# Execute rebuild indexes
	# Note: to use the "SYSTEM" option the workspace user must be an administrator.
	arcpy.RebuildIndexes_management(sde, "NO_SYSTEM", userDataList, "ALL")

	logging.info('Rebuild complete')


def analyzeDatasets(sde):
	'''
	Using the input database connection file, lists all datasets within
	database owned by the user the database connection is configured, 
	including feature classes and datasets within feature datasets.
	Each dataset is analyzed (statistics are updated). 
	Args:
		sde: connection file to sde (enterprise geodatabase). 
	'''

	logging.info("Starting to analyze datasets")

	# set the workspace environment
	arcpy.env.workspace = sde

	# Get the user name for the workspace
	userName = arcpy.Describe(sde).connectionProperties.user

	# Get a list of all the datasets the user owns by using a wildcard that 
	# incldues the user name
	# First, get all the stand alone tables, feature classes and rasters.
	dataList = arcpy.ListTables(userName + "*") + \
				arcpy.ListFeatureClasses(userName + "*") + \
				arcpy.ListRasters(userName + "*")

	# Next, for feature datasets get all of the datasets and featureclasses
	# from the list and add them to the master list.
	for dataset in arcpy.ListDatasets(userName + "*", "Feature"):
		arcpy.env.workspace = os.path.join(sde, dataset)
		dataList += arcpy.ListFeatureClasses(userName + "*") + \
					arcpy.ListDatasets(userName + "*")

	# reset the workspace
	arcpy.env.workspace = sde

	# Execute analyze datasets
	# Note: to use the "SYSTEM" option the workspace user must be an administrator.
	arcpy.AnalyzeDatasets_management(sde, 
									"NO_SYSTEM", 
									dataList, 
									"ANALYZE_BASE",
									"ANALYZE_DELTA",
									"ANALYZE_ARCHIVE")

	logging.info("Analyze complete")


def deleteCxn(sde):
	'''
	Determines if the input database connection file exists. If so,
	deletes connection file. This function should only be called
	if the connection file was built for temporary use. 
	Args:
		sde: connection file to sde (enterprise geodatabase). 
	'''	

	logging.info("Starting to delete connection string")

	if os.path.exists(sde):
		logging.info("File exists")
		try:
			os.remove(sde)
			logging.info("File deleted")
		except PermissionError:
			logging.warn("User does not have necessary permissions to \
						delete connection file")
	else:
		logging.warn(f"Could not find connection file {sde}")
	

if __name__ == "__main__":

	# timestamp
	timestamp = datetime.datetime.today().strftime("%Y%m%d")
	start_time = datetime.datetime.now().strftime("%H:%M:%S")

	# set paths
	home_fldr = os.path.dirname(__file__)

	cfg_fldr = os.path.join(home_fldr, "configs")
	env_file = os.path.join(cfg_fldr, "env.json")
	cfg_file = os.path.join(cfg_fldr, "config.json")
	sde_cxn_fldr = os.path.join(home_fldr, "sde_cxn")
	log_fldr = os.path.join(home_fldr, "logs")

	# set logging
	log_file = os.path.join(log_fldr, f"db_maintenance_{timestamp}.log")
	logging.basicConfig(level=logging.INFO,
						format='%(levelname)s: %(asctime)s %(message)s',
						datefmt='%Y/%m/%d %H:%M:%S',
						handlers=[logging.FileHandler(log_file),
								logging.StreamHandler(sys.stdout)])
	
	logging.info(start_time)
	logging.info("Starting run of DB maintenance...")
	logging.info("Reading configs")

	# read configs
	with open(env_file) as f:
		env = (json.load(f))["env"]
	
	logging.info(f"Working in env: {env}")

	with open(cfg_file) as f:
		cfg = (json.load(f))[env]

	# run funcs
	try:
		sde, built = buildCxn(cfg)
		reconcileVersions(sde)
		compressDB(sde)
		rebuildIndex(sde)
		analyzeDatasets(sde)
		if built:
			deleteCxn(sde)

		logging.info("Completed DB maintenance! \n\n")

	except Exception as e:

		logging.error(e, exc_info=True)
		logging.error("DB maintenance failed to complete. \n\n")


# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Setup stage, RUN IT BEFORE ALL
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Declare storage's path for each tier
# MAGIC

# COMMAND ----------

# Test shell
import os
from dotenv import load_dotenv

load_dotenv('../.env.local')
# Get environment variables
environment = os.getenv('ENVIRONMENT')
print(f"Environment: {environment}")
if environment == 'local':
    print("Running in local environment")
    data_path = os.getenv('API_URL')
    print(data_path)

# COMMAND ----------

# Mount ADLS Gen2
# Required each time the cluster is restarted which should be only on the first notebook as they run in order

tiers = ["bronze", "silver", "gold"]
adls_paths = {tier: f"abfss://{tier}@footballanalyzesa.dfs.core.windows.net/" for tier in tiers}

# Accessing paths
bronze_adls = adls_paths["bronze"]
silver_adls = adls_paths["silver"]
gold_adls = adls_paths["gold"] 

dbutils.fs.ls(bronze_adls)
dbutils.fs.ls(silver_adls)
dbutils.fs.ls(gold_adls)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Import needed libs

# COMMAND ----------

import requests
import json
import concurrent.futures
from itertools import chain
import time
import threading
from queue import Queue
from threading import Semaphore
from itertools import product

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Install logging package from DBFS

# COMMAND ----------

# MAGIC %pip install --force-reinstall git+https://github.com/lehoangkhoi01/football-analyze-db.git@main

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from custom_logging.logger import DatabricksLogger
logger = DatabricksLogger().get_logger()
logger.info("This should appear with proper formatting!")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Prepare for calling API
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Define common fetching function
# MAGIC - Declare root URL and other endpoints
# MAGIC - Define request's header
# MAGIC - Define `fetchAPI` function
# MAGIC

# COMMAND ----------

# Construct the API URL
rootUrl = "https://v3.football.api-sports.io" # Configure to use variable later
leaguesEndpoint = f"{rootUrl}/leagues"
teamsEndpoint = f"{rootUrl}/teams"
fixturesEndpoint = f"{rootUrl}/fixtures"
# playersEndpoint = f"{rootUrl}/players"

# Retrieve the API key from Azure Key Vault-backed secret scope
api_key = dbutils.secrets.get(scope="api-football-key-scope", key="api-football-key")

# Define headers
headers = {
    'x-rapidapi-host': 'v3.football.api-sports.io',
    'x-rapidapi-key': api_key  
}

# Define function to fetch API
def fetchAPI(url):
    try:
        # Make GET request to fetch data
        response = requests.get(url, headers=headers)
        # Check if request is success
        response.raise_for_status()
        print(response.json())
        if response.status_code == 200:
            print('Data fetched successfully')
            return response.json()
        else:
            raise Exception('Error from fetching API')
    except requests.exceptions.RequestException as e:
            raise Exception('Error from fetching API: ', e)




# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Fetch each endpoints and save raw data into bronze storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Call to fetch and save 'Leagues' data
# MAGIC - Define file's path for output of 'League' data
# MAGIC - Save fetched 'League' data into Bronze storage layer

# COMMAND ----------

saveLeagueDataPath = 'api_football/leagues.json'

try:
  response = fetchAPI(leaguesEndpoint)
  resultCount = response.get('results')
  
  if not resultCount:
    print('Empty data from fetching Leagues endpoint')
  else:
    # Specify ADLS path
    file_path = f"{bronze_adls}/{saveLeagueDataPath}"
    # Save the JSON data
    data = response.get('response', [])
    print(f"Data rows: ", resultCount)
    json_data = json.dumps(data, indent=4)
    dbutils.fs.put(file_path, json_data, overwrite=True)
    print(f"Data saved to {file_path}")
except:
    print('There is error from fetching Leagues data')




# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Call to fetch and save 'Teams' data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define fetch 'Teams' data function
# MAGIC - Due to request's rate limit (10 request/minutes), handle it by defining delay time between requests
# MAGIC - Use multi workers
# MAGIC - When fetching 'Teams' data, we take notice into 2 params: `league_id` and `season`

# COMMAND ----------


# Configuration
MAX_WORKERS = 3  # Conservative number to stay under limit
REQUESTS_PER_MINUTE = 10
DELAY = 60 / REQUESTS_PER_MINUTE  # 6 seconds between requests

# Track last request time globally
last_request_time = 0
request_lock = threading.Lock()

def fetch_teams(league_id, season=2023):
    url = f"{teamsEndpoint}?league={league_id}&season={season}"
    global last_request_time
    try:
        with request_lock:
            elapsed = time.time() - last_request_time
            if elapsed < DELAY:
                time.sleep(DELAY - elapsed)
            last_request_time = time.time()

        # Fetch data
        response = fetchAPI(url)
        resultCount = response.get('results')
        if not resultCount:
            print(f'Empty data from fetching Teams endpoint for league {league_id} and season {season}')
            return []
        else:
            data = response.get('response', [])
            return data
    except requests.exceptions.RequestException as e:
            print(f"Error fetching data from API: {e}")


# COMMAND ----------

availableSeason = [2022, 2023]
saveTeamDataPath = 'api_football/teams.json'
file_path = f"{bronze_adls}/{saveTeamDataPath}"

# read 'Leagues' data from bronze storage
df = spark.read.option("multiline", "true").json(f"{bronze_adls}/{saveLeagueDataPath}")
leagues = df.select('league.id').collect()
#----------------

# Instead looping through 'leagues' list, we will choose to loop through pre-defined list of league's id since there is limitation for API request per day (due to FREE subscription)
# The pre-defined list of league/competitions will included most of major competitions in football, so we can have more other information later
pre_defined_league_ids = [1, 2, 3, 4, 5, 15, 140, 39, 45, 143, 135, 78, 61]
test_leagues = [140, 39, 45, 2, 3, 143, 135]

# Define function for fetching teams with combinations of league and season
def process_combination_fetch_teams():
    # Generate all league_id/year combinations
    combinations = list(product(test_leagues, availableSeason))
    results = []

    # Process with controlled parallelism
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Map combinations to fetch_teams function
        future_to_combo = {
            executor.submit(fetch_teams, league_id, year): (league_id, year)
            for league_id, year in combinations
        }

        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_combo):
            league_id, year = future_to_combo[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Processing failed for {league_id}/{year}: {str(e)}")
    return results


all_data = process_combination_fetch_teams()

# Save raw data into bronze storage
if not all_data:
    print('Empty data')
else:
    flattenedList = list(chain.from_iterable(all_data))
    print(f"Data rows: ", len(flattenedList))
    json_data = json.dumps(flattenedList, indent=4)
    dbutils.fs.put(file_path, json_data, overwrite=True)
    print(f"Data saved to {file_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Call to fetch 'Fixtures' data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define fetch 'Fixture' data function

# COMMAND ----------

# Configuration
MAX_WORKERS = 3  # Conservative number to stay under limit
REQUESTS_PER_MINUTE = 10
DELAY = 60 / REQUESTS_PER_MINUTE  # 6 seconds between requests

# Thread-safe request tracker
request_times = Queue()

# Global semaphore to control total requests
request_semaphore = Semaphore(REQUESTS_PER_MINUTE)

def fetch_fixtures(league_id, season=2023):
    url = f"{fixturesEndpoint}?league={league_id}&season={season}"
    try:
        with request_semaphore:
            # Start timing the request
            start_time = time.time()

            # Fetch data
            response = fetchAPI(url)

            # Calculate remaining delay time
            request_duration = time.time() - start_time
            remaining_delay = max(0, DELAY - request_duration)
            time.sleep(remaining_delay)

            resultCount = response.get('results')
            if not resultCount:
                print(f'Empty data from fetching Fixtures endpoint for league {league_id} and season {season}')
                return []
            else:
                data = response.get('response', [])
                return data
    except requests.exceptions.RequestException as e:
            print(f"Error fetching data from API: {e}")


# COMMAND ----------

vailableSeason = [2022, 2023]
saveFixtureDataPath = 'api_football/fixtures.json'
file_path = f"{bronze_adls}/{saveFixtureDataPath}"

# read 'Leagues' data from bronze storage
df = spark.read.option("multiline", "true").json(f"{bronze_adls}/{saveLeagueDataPath}")
leagues = df.select('league.id').collect()
#----------------

# Instead looping through 'leagues' list, we will choose to loop through pre-defined list of league's id since there is limitation for API request per day (due to FREE subscription)
# The pre-defined list of league/competitions will included most of major competitions in football, so we can have more other information later
pre_defined_league_ids = [1, 2, 3, 4, 5, 15, 140, 39, 45, 143, 135, 78, 61]
test_leagues = [140, 39, 45, 2, 3]

# Define function for fetching teams with combinations of league and season
def process_combination_fetch_fixtures():
    # Generate all league_id/year combinations
    combinations = list(product(test_leagues, availableSeason))
    results = []

    # Process with controlled parallelism
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Map combinations to fetch_fixtures function
        future_to_combo = {
            executor.submit(fetch_fixtures, league_id, year): (league_id, year)
            for league_id, year in combinations
        }

        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_combo):
            league_id, year = future_to_combo[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Processing failed for {league_id}/{year}: {str(e)}")
    return results


all_data = process_combination_fetch_fixtures()

# Save raw data into bronze storage
if not all_data:
    print('Empty data')
else:
    flattenedList = list(chain.from_iterable(all_data))
    print(f"Data rows: ", len(flattenedList))
    json_data = json.dumps(flattenedList, indent=4)
    dbutils.fs.put(file_path, json_data, overwrite=True)
    print(f"Data saved to {file_path}")
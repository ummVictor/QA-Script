import argparse
import pymongo
import pandas as pd
from datetime import datetime

def connectMongodb():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["test_database"]
    return db

def parseUploadBlock(db, block_data, collection_name):
    if block_data.endswith('.xlsx'):
        df = pd.read_excel(block_data)
    else:
        df = pd.read_csv(block_data, delimiter='\t')
    records = df.to_dict(orient='records')
    collection = db[collection_name]
    collection.insert_many(records)
  
def printCollectionContents(db, collection_name):
    collection = db[collection_name]
    for document in collection.find():
        print(document)

def clearCollection(db, collection_name):
    collection = db[collection_name]
    collection.delete_many({})
  
def exportCSV(db, collection_name, user_report):
    collection = db[collection_name]
    data = list(collection.find({'Test Owner': user_report}))
    if data:
        df = pd.DataFrame(data)
        df.to_csv(f"{user_report}_report.csv", index=False)
        print(f"Exported data for user '{user_report}' to {user_report}_report.csv")
    else:
        print(f"No data found for user '{user_report}'")
        
def listRepeatableBugs(db):
    result = []
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        cursor = collection.find({"Repeatable?": "Yes"})
        for document in cursor:
            result.append(document)
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        cursor = collection.find({"Repeatable?": "yes"})
        for document in cursor:
            result.append(document)
    return result

def listWorkDone(db, test_owner):
    result = []
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        cursor = collection.find({"Test Owner": test_owner})
        for document in cursor:
            result.append(document)
    return result

def listReportsBuild(db, build_date):
    result = []
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        cursor = collection.find({"Build #": build_date})
        for document in cursor:
            result.append(document)
    return result

def listBlocker(db):
    result = []
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        cursor = collection.find({"Blocker?": "Yes"})
        for document in cursor:
            result.append(document)
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        cursor = collection.find({"Blocker?": "yes"})
        for document in cursor:
            result.append(document)
    return result

def getTestCases(db):
    result = []
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        first_test_case = collection.find_one({}, sort=[("_id", pymongo.ASCENDING)])
        middle_test_case = collection.find_one({}, skip=collection.count_documents({}) // 2)
        final_test_case = collection.find_one({}, sort=[("_id", pymongo.DESCENDING)])
        result.extend([first_test_case, middle_test_case, final_test_case])
    return result

def write_results_to_file(results, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        for result in results:
            f.write(str(result) + '\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MongoDB Data Ingestion and Export Script')
    parser.add_argument('file', type=str, help='File to ingest into MongoDB')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose mode')
    parser.add_argument('--collection', type=str, default='collection', help='Collection name in MongoDB')
    parser.add_argument('--export-csv', action='store_true', help='Export data to CSV file')
    parser.add_argument('--user-reports', type=str, help='User report to export to CSV')

    args = parser.parse_args()

    db = connectMongodb()

    clearCollection(db, args.collection)

    parseUploadBlock(db, args.file, args.collection)

    if args.export_csv and args.user_reports:
        exportCSV(db, args.collection, args.user_reports)

    if args.verbose:
        
        work_done_by = listWorkDone(db, "Victor Um")
        repeatable_bugs = listRepeatableBugs(db)
        blocker_bugs = listBlocker(db)
        build_date = datetime(2024, 3, 19, 0, 0)  # Construct a datetime object for the build date
        reports_on_build = listReportsBuild(db, build_date)
        test_cases = getTestCases(db)
        
        write_results_to_file(work_done_by, "work_done_by.txt")
        write_results_to_file(repeatable_bugs, "repeatable_bugs.txt")
        write_results_to_file(blocker_bugs, "blocker_bugs.txt")
        write_results_to_file(reports_on_build, "reports_on_build.txt")
        write_results_to_file(test_cases, "test_cases.txt")
#!/usr/bin/env python 3.5

#Author:        Sasan Bahadaran
#Date:          5/7/16
#Organization:  Commerce Data Service
#Description:   This script crawls the files directory and finds each metadata
#xml file.  It then splits the master file into one xml file per document.
#Next, it renames core elements and transforms the content to JSON.
#Lastly, it sends the documents from the json file to Solr for indexing.

import sys, json, xmltodict, os, logging, time, argparse, glob, requests,re
import dateutil.parser

from datetime import datetime


#change extension of file name to specified extension
def changeExt(fname, ext):
    seq = (os.path.splitext(fname)[0], ext)
    return '.'.join(seq)

#get field(date) and return in a proper iso format
def formatDate(x, field):
    date = dateutil.parser.parse(x.pop(field))
    dateiso = date.isoformat()+'Z'
    return dateiso

#validate date
def validDate(s):
    try:
        datetime.strptime(s, "%Y")
        return s
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def splitFiles(fname):
    try:
        filecontent = []
        fn = ""
        dir_path = os.path.join(os.path.dirname(fname),os.path.splitext(fname)[0])
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        with open(os.path.abspath(fname)) as fd:
            filecontent.append(fd.readline())
            for line in fd:
                if line.strip().startswith("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"):
                    #print("end of doc")
                    if not os.path.isfile(os.path.join(dir_path)):
                        with open(os.path.join(dir_path,fn),'w') as outfile:
                            outfile.writelines(filecontent)
                    else:
                        logging.info("--file: "+fn+" already exists")
                        print("file exists")
                    del filecontent[:]
                    filecontent.append(fd.readline())
                elif line.strip().startswith("<us-patent-grant"):
                    searchobj = re.findall('file="(.*?)"', line)
                    fn = searchobj[0]
                    #print(fn)
                    filecontent.append(line)
                else:
                    filecontent.append(line)
            if not os.path.isfile(os.path.join(dir_path)):
                with open(os.path.join(dir_path,fn),'w') as outfile:
                    outfile.writelines(filecontent)
                    #print("last file")
    except IOError as e:
        logging.error("I/O error({0}): {1}".format(e.errno,e.strerror))
    except:
        logging.error("Unexpected error:", sys.exc_info()[0])
        raise

#this function contains the code for parsing the xml file
#and writing the results out to a json file
def parseXML(dir_name):
    try:
        for file in dir_name:
            with open(os.path.abspath(file)) as fd:
                fn = changeExt(fd,'json')
                print(fn)
                doc = xmltodict.parse(fd.read())
                doc_info = doc['us-patent-grant']['us-bibliographic-data-grant']['publication-reference']['document-id']
                doc_info['appid'] = doc_info.pop('doc-number')
                doc_info['doc_date'] = doc_info.pop('date')
                #textdata - not sure what this should be set to.....

                #transform output to json and save to file with same name
                with open(fn,'w') as outfile:
                    json.dump(doc,outfile)
                    logging.info("-- Processing of XML file complete")
    except IOError as e:
        logging.error("I/O error({0}): {1}".format(e.errno,e.strerror))
    except:
        logging.error("Unexpected error:", sys.exc_info()[0])
        raise

def readJSON(fname):
    try:
        with open(os.path.abspath(fname)) as fd:
            doc = json.loads(fd.read())
            records = doc['main']['DATA_RECORD']
            for x in records:
                docid = x.get('DOCUMENT_IMAGE_ID',x.get('DOCUMENT_NM'))
                jsontext = json.dumps(x)
                #need to change this line
                print(os.path.join(os.path.dirname(fname)))
                with open(os.path.join(os.path.dirname(fname),'solrcomplete.txt'),'a+') as logfile:
                    logfile.seek(0)
                    if docid+"\n" in logfile:
                       logging.info("-- file: "+docid+"  already processed by Solr")
                       continue
                    else:
                       logging.info("-- Sending file: "+docid+" to Solr")
                       response = sendToSolr('ptab', jsontext)
                       r = response.json()
                       status = r["responseHeader"]["status"]
                       if status == 0:
                           logfile.write(docid+"\n")
                           logging.info("-- Solr update for file: "+docid+" complete")
                       else:
                           logging.info("-- Solr error for doc: "+docid+" error: "+', '.join("{!s}={!r}".format(k,v) for (k,v) in rdict.items()))
    except IOError as e:
        logging.error("I/O error({0}): {1}".format(e.errno,e.strerror))
    except:
        logging.error("Unexpected error:", sys.exc_info()[0])
        raise

#send document to Solr for indexing
def sendToSolr(core, json):
     #add try-catch block
     jsontext = '{"add":{ "doc":'+json+',"boost":1.0,"overwrite":true, "commitWithin": 1000}}'
     url = os.path.join(solrURL,"solr",core,"update")
     headers = {"Content-type" : "application/json"}
     
def processFile(fname):
    logging.info("-- Processing file: "+fname)
    dir_path = os.path.join(os.path.dirname(fname),os.path.splitext(fname)[0])
    if (args.skipsplit):
        logging.info("-- Skipping File Split process")
        logging.info("-- Starting XML Parse process")
        #parseXML(dir_path)
        if (args.skipsolr):
            logging.info("-- Skipping Solr process.")
        else:
            logging.info("-- Starting Solr process")
            #readJSON(dir_path)
    else:
        logging.info("-- Starting File Split process")
        splitFiles(fname)
        logging.info("-- Starting XML Parse process")
        #parseXML(dir_path)
        if (args.skipsolr):
            logging.info("-- Skipping Solr process") 
        else:
             logging.info("-- Starting Solr process")
             #readJSON(dir_path)

if __name__ == '__main__':
    scriptpath = os.path.dirname(os.path.abspath(__file__))
    solrURL = "http://54.208.116.77:8983"

    #logging configuration
    logging.basicConfig(
                        filename='logs/parsegrant-log-'+time.strftime('%Y%m%d'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s -%(message)s',
                        datefmt='%Y%m%d %H:%M:%S'
                       )

    parser = argparse.ArgumentParser()
    parser.add_argument(
                        "-d",
                        "--dates",
                        required=False,
                        help="Process file(s) for specific date(s) - format YYYY",
                        nargs='*',
                        type=validDate
                       )
    parser.add_argument(
                        "-s",
                        "--skipsolr",
                        required=False,
                        help="Pass this flag to skip Solr processing",
                        action='store_true'
                       )
    parser.add_argument(
                        "-p",
                        "--skipsplit",
                        required=False,
                        help="Pass this flag to skip File Split process",
                        action='store_true'
                       )
    args = parser.parse_args()
    if args.dates:
        logging.info("Date arguments set to: "+",".join(args.dates))
    #logging.info("File Split set to: "+str(args.skipsplit))
    logging.info("-- [JOB START]  ----------------")

    if args.dates:
       for date in args.dates:
            #crawl through each main directory and find the metadata xml file
            for filename in glob.iglob(os.path.join(scriptpath,'files/GRANTS',date,'*.xml'),recursive=True):
                processFile(filename)
    else:
        #crawl through each main directory and find the metadata xml file
        for filename in glob.iglob(os.path.join(scriptpath,'files/GRANTS','*/*.xml')):
            processFile(filename)

    logging.info("-- [JOB END] ----------------")

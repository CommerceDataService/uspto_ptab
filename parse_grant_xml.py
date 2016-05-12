#!/usr/bin/env python 3.5

#Author:        Sasan Bahadaran
#Date:          5/7/16
#Organization:  Commerce Data Service
#Description:   This script crawls the files directory and finds each
#xml file.  It then take the master file and turns it into a properly
#formed combined XML file. Next, it renames core elements and transforms
#the content to JSON. Lastly, it sends the documents from the json file 
#to Solr for indexing.

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

#validate filetype argument
def validType(s):
    if s not in ("p","g"):
        msg = "Not a valid type: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)
    else:
        return s

#writes to output file
def outputFile(fname, input):
    try:
        if not os.path.isfile(fname):
            logging.info("-- Writing to output file: "+fname)
            with open(fname,'w') as outfile:
                outfile.writelines(input)
            logging.info("-- Writing to file complete: "+fname)
        else:
             logging.info("-- File: "+fname+" already exists")
    except IOError as e:
        logging.error("-- I/O error({0}): {1}".format(e))

#this function alters the XML document in order to form a unified single XML
#document contained in a root node
def combineFiles(fname):
    try:
        filecontent = []
        fn = os.path.splitext(fname)[0]+"_alt.xml"
        if not os.path.isfile(fn):
            with open(os.path.abspath(fname)) as fd:
                filecontent.append(fd.readline())
                filecontent.append(fd.readline())
                filecontent.append("<main>\n")
                for line in fd:
                    if line.strip().startswith("<?xml version"):
                        continue
                    elif line.strip().startswith("<!DOCTYPE"):
                        continue
                    else:
                        filecontent.append(line)
                else:
                    filecontent.append("</main>")
                outputFile(fn, filecontent)
                logging.info("-- Combine file process complete")
        else:
            logging.info("-- File: "+fn+" already exists")
    except IOError as e:
        logging.error("-- I/O error({0}): {1}".format(e.errno,e.strerror))
        raise
    except:
        logging.error("-- Unexpected error:", sys.exc_info()[0])
        raise

#this function contains the code for parsing the xml file
#and writing the results out to a json file
def parseXML(fname):
    try:
        fn = changeExt(fname,'json')
        if not os.path.isfile(fn):
            with open(fname) as fd:
                doc = xmltodict.parse(fd.read())
                for x in doc['main']['us-patent-'+filetype[1]]:
                    line = x['us-bibliographic-data-'+filetype[1]]['publication-reference']['document-id']
                    line['appid'] = line.pop('doc-number')
                    line['doc_date'] = line.pop('date')
                    #textdata field needs to be set also, but that is yet to be determined

            #transform output to json and save to file with same name
            outputFile(fn,json.dumps(doc))
            logging.info("-- Processing of XML file complete")
        else:
            logging.info("-- File: "+fn+" already exists.")

    except KeyError as e:
        logging.error("-- File: "+fn+" Key Error: {0} ".format(e))
        pass
    except IOError as e:
        logging.error("-- I/O error({0}): {1}".format(e.errno,e.strerror))
    except:
        logging.error("-- Unexpected error:", sys.exc_info()[0])
        raise

def readJSON(fname):
    try:
        with open(fname) as fd:
            doc = json.loads(fd.read())
            for x in doc['main']['us-patent-'+filetype[1]]:
                docid = x['us-bibliographic-data-'+filetype[1]]['publication-reference']['document-id']['appid']
                jsontext = json.dumps(x)
                with open(os.path.join(os.path.dirname(fname),'solrcomplete.txt'),'a+') as logfile:
                    logfile.seek(0)
                    if docid+"\n" in logfile:
                       logging.info("-- File: "+docid+"  already processed by Solr")
                       continue
                    else:
                       logging.info("-- Sending file: "+docid+" to Solr")
                       response = sendToSolr('grants', jsontext)
                       r = response.json()
                       status = r["responseHeader"]["status"]
                       if status == 0:
                           logfile.write(docid+"\n")
                           logging.info("-- Solr update for file: "+docid+" complete")
                       else:
                           logging.info("-- Solr error for doc: "+docid+" error: "+', '.join("{!s}={!r}".format(k,v) for (k,v) in rdict.items()))
    except IOError as e:
        logging.error("-- I/O error({0}): {1}".format(e.errno,e.strerror))
    except:
        logging.error("-- Unexpected error:", sys.exc_info()[0])
        raise

#send document to Solr for indexing
def sendToSolr(core, json):
     try:
         jsontext = '{"add":{ "doc":'+json+',"boost":1.0,"overwrite":true, "commitWithin": 1000}}'
         url = os.path.join(solrURL,"solr",core,"update")
         headers = {"Content-type" : "application/json"}
     except:
         logging.error("-- Unexpected error: ", sys.exc_info()[0])

def processFile(fname):
    logging.info("-- Processing file: "+fname)
    altfn = os.path.splitext(fname)[0]+"_alt.xml"
    fn = changeExt(altfn,'json')
    if (args.skipcombine):
        logging.info("-- Skipping File Split process")
        logging.info("-- Starting XML Parse process")
        parseXML(altfn)
        if (args.skipsolr):
            logging.info("-- Skipping Solr process.")
        else:
            logging.info("-- Starting Solr process")
            readJSON(fn)
    else:
        logging.info("-- Starting File Split process")
        combineFiles(fname)
        logging.info("-- Starting XML Parse process")
        parseXML(altfn)
        if (args.skipsolr):
            logging.info("-- Skipping Solr process")
        else:
             logging.info("-- Starting Solr process")
             readJSON(fn)

if __name__ == '__main__':
    scriptpath = os.path.dirname(os.path.abspath(__file__))
    solrURL = "http://54.208.116.77:8983"
    filetype = []

    #logging configuration
    logging.basicConfig(
                        filename='logs/parsegrant-log-'+time.strftime('%Y%m%d'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s -%(message)s',
                        datefmt='%Y%m%d %H:%M:%S'
                       )

    parser = argparse.ArgumentParser()
    parser.add_argument(
                        "-t",
                        "--type",
                        required=True,
                        help="Specifies type of document (p or g)",
                        type=validType
                       )
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
                        "-c",
                        "--skipcombine",
                        required=False,
                        help="Pass this flag to skip File Combine process",
                        action='store_true'
                       )
    args = parser.parse_args()
    if args.type == "g":
        filetype.append("GRANTS")
        filetype.append("grant")
    elif args.type == "p":
        filetype.append("PUBS")
        filetype.append("application")
    logging.info("--------ARGUMENTS------------")
    logging.info("File type set to: "+filetype[0])
    if args.dates:
        logging.info("Date arguments set to: "+",".join(args.dates))
    logging.info("Skip File Combine set to: "+str(args.skipcombine))
    logging.info("Skip Solr set to: "+str(args.skipsolr))
    logging.info("-- [JOB START]  ----------------")

    if args.dates:
        for date in args.dates:
            #crawl through each main directory and find the metadata xml file
            for filename in glob.iglob(os.path.join(scriptpath,'files',filetype[0],date,'*.xml'),recursive=True):
                if not filename.endswith("_alt.xml"):
                    processFile(filename)
    else:
        #crawl through each main directory and find the metadata xml file
        for filename in glob.iglob(os.path.join(scriptpath,'files',filetype[0],'*/*.xml')):
            if not filename.endswith("_alt.xml"):
                processFile(filename)

    logging.info("-- [JOB END] ----------------")

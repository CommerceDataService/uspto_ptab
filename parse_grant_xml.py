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

#validate filetype argument
def validType(s):
    if s not in ("p","g","pt"):
        msg = "Not a valid file type value: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)
    else:
        return s

#validate date
def validDate(s):
    for fmt in ("%Y%m%d","%Y"):
        try:
            datetime.strptime(s, fmt)
            return s
        except ValueError:
            pass
    msg = "Not a valid date: '{0}'.".format(s)
    raise argparse.ArgumentTypeError(msg)

#replace key in dictionary
def replaceKey(data,x,y):
    data[x] = data.pop(y)
    return data

#replace date value with a proper iso format
def formatDate(value):
    date = dateutil.parser.parse(value)
    dateiso = date.isoformat()+'Z'
    return dateiso

#read parsed PDF doc for PTAB files
def readDoc(txtfn):
    try:
        with open(txtfn) as dr:
            text = dr.read()
            return text
    except IOError as e:
        logging.error("-- File: "+txtfn+" I/O error({0}): {1}".format(e.errno,e.strerror))

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
        print("filename: "+fn)
        if not os.path.isfile(fn):
            with open(fname) as fd:
                print("begin read: "+str(datetime.now()))
                doc = xmltodict.parse(fd.read())
                print("end read: "+str(datetime.now()))
                if args.ftype in ("g","p"):
                     print("in g/p - filetype: "+filetype[3])
                     iter = 0
                     for x in doc['main']['us-patent-'+filetype[3]]:
                         iter +=1
                         print(iter)
                         line = x['us-bibliographic-data-'+filetype[3]]['publication-reference']['document-id']
                         kitems = {"appid":"doc-number","doc_date":"date"}
                         for key,value in kitems.items():
                             print("key: "+key+" value: "+value)
                             replaceKey(line,key,value)
                         #line['appid'] = line.pop('doc-number')
                         #line['doc_date'] = line.pop('date')
                         #textdata field needs to be set also, but that is yet to be determined
                else:
                    print("in else!!!")
                    print("args.ftype: "+args.ftype)
                    for x in doc['main']['DATA_RECORD']:
                        txtfn = os.path.join(os.path.dirname(fn),'PDF_image',docid+'.txt')
                        if os.path.isfile(txtfn):
                            kitems = {"appid":"BD_PATENT_APPLICATION_NO","doc_date":"DOCUMENT_CREATE_DT"}
                            for key,value in kitems:
                                replaceKey(x,key,value)

                            kitems2 = ["LAST_MODIFIED_TS","PATENT_ISSUE_DT","DECISION_MAILED_DT","PRE_GRANT_PUBLICATION_DT","APPLICANT_PUB_AUTHORIZATION_DT"]
                            for item in kitems2:
                                x[item] = formatDate(x.pop(item))
                            x['textdata'] = readDoc(txtfn)
                            #x['LAST_MODIFIED_TS'] = formatDate(x, 'LAST_MODIFIED_TS')
                            #x['PATENT_ISSUE_DT'] = formatDate(x, 'PATENT_ISSUE_DT')
                            #x['DECISION_MAILED_DT'] = formatDate(x, 'DECISION_MAILED_DT')
                            #x['PRE_GRANT_PUBLICATION_DT'] = formatDate(x, 'PRE_GRANT_PUBLICATION_DT')
                            #x['APPLICANT_PUB_AUTHORIZATION_DT'] = formatDate(x, 'APPLICANT_PUB_AUTHORIZATION_DT')
                            #x['appid'] = x.pop('BD_PATENT_APPLICATION_NO')
                            #x['doc_date'] = x.pop('DOCUMENT_CREATE_DT')
                            ##put in different function
                            #with open(txtfn) as dr:
                            #    text = dr.read()
                            #    x['textdata'] = text
            print("output file")
            #transform output to json
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
            for x in doc['main'][filetype[3]]:
                docid = x['us-bibliographic-data-'+filetype[3]]['publication-reference']['document-id']['appid']
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
    if (args.ftype in ("g","p")):
        print("in p/g processFile")
        altfn = os.path.splitext(fname)[0]+"_alt.xml"
        fn = changeExt(altfn,'json')
        if not (args.skipcombine):
            logging.info("-- Starting File Split process")
            #combineFiles(fname)
    else:
        altfn = fname
        fn = changeExt(altfn,'json')
    logging.info("-- Starting XML Parse process")
    parseXML(altfn)
    if (args.skipsolr):
        logging.info("-- Skipping Solr process")
    else:
        logging.info("-- Starting Solr process")
        #readJSON(fn)

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
                        "--ftype",
                        required=True,
                        help="Specifies type of document (g, p, or pt)",
                        type=validType
                       )
    parser.add_argument(
                        "-d",
                        "--dates",
                        required=False,
                        help="Process file(s) for specific date(s) - if type = g or p, format YYYY, if type = pt, format YYYYMMDD",
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
    logging.info("-- [JOB START]  ----------------")
    if args.ftype == "g":
        filetype = ["GRANTS","*","","grant"]
    elif args.ftype == "p":
        filetype = ["PUBS","*","","application"]
    elif args.ftype == "pt":
        filetype = ["PTAB","PTAB*","PTAB*"]
    logging.info("--------ARGUMENTS------------")
    logging.info("File Type set to: "+filetype[0])
    if args.dates:
        logging.info("Dates set to: "+",".join(args.dates))
    logging.info("Skip Combine set to: "+str(args.skipcombine))
    logging.info("Skip Solr set to: "+str(args.skipsolr))

    if args.dates:
        for date in args.dates:
            if args.ftype in ("g","p"):
                date = str(dateutil.parser.parse(date).year)
            #crawl through each main directory and find the metadata xml file
            for filename in glob.iglob(os.path.join(scriptpath,'files',filetype[0],filetype[2]+date,'*.xml')):
                if not filename.endswith("_alt.xml"):
                    processFile(filename)
    else:
        #crawl through each main directory and find the metadata xml file
        for filename in glob.iglob(os.path.join(scriptpath,'files',filetype[0],filetype[1],'*.xml')):
            if not filename.endswith("_alt.xml"):
                processFile(filename)

    logging.info("-- [JOB END] ----------------")

#/bin/bash
#set -x

###################################################################################################################
#
#Script to download,extract, and parse PTAB pdf files from USPTO bulk data site. 
#
#argument options:
#           (1) pass -d OR --date AND YYYYMMDD to download files from a specific date on
#           (2) pass -a OR --all to download all files
#           (3) pass -n OR --none to skip downloading of files and execute unzipping process
#
###################################################################################################################

#create lock file
function lock ( ) {
  if "$1" == true
  then
    if [ -f ${lockFile} ]
    then
      echo
      echo "${lockFile} exits."
      echo "Looks like a copy of $scriptName is already running... "
      echo "Abort"
      log "ERR" " Already running, please check logs for reason... "
      exit 0
    else
      touch ${lockFile}
    fi
  else
    if [ -f $lockFile ] ; then
      rm $lockFile
    fi
  fi
}

function usage
{
    echo "usage: ./retrieve_files.sh [[-d YYYY YYYY]| [-a] | [-n] [url]"
}

function log()
{
  type=$1
  message=$2

  if [ $type = "WARN" -o $type = "ERR" ]
  then
    echo >> $statusDirectory/err-$processingTime
    echo -e "$type: `date +%Y.%m.%d-%H:%M:%S` -- $scriptName -- $message" >> $statusDirectory/retrieve-err-$processingTime
  fi
  #
  # always write into log file
  #
  echo
  echo -e "$type: `date +%Y.%m.%d-%H:%M:%S` -- $scriptName -- $message"
  echo >> $statusDirectory/log-$processingTime
  echo -e "$type: `date +%Y.%m.%d-%H:%M:%S` -- $scriptName -- $message" >> $statusDirectory/retrieve-log-$processingTime
}

#=============================== MAIN BODY OF SCRIPT ===============================

##### Constants
processingTime=`date +%Y%m%d-%H%M%S`
scriptName=$0
statusDirectory=logs
baseURL=$()
dropLocation="files"
startDate=1976
endDate=$(date +%Y) 
retrieveAll=false
retrieveNone=false
lockFile="/tmp/file_download.lck"

touch $statusDirectory/log-$processingTime

lock true

case "$1" in
-d | --date )
  shift
  if date "+%Y" -d $1 >/dev/null 2>&1
  then
    startDate=$1
    if [ ! -z "$2" ]
    then
      echo $2
      if date "+%Y" -d $2 >/dev/null 2>&1 
      then
        endDate=$2
        if [ ! -z "$3" ]
        then
          baseURL=$3
        else
          log "ERR" "URL to files is not present: $3"
          lock false
          exit 1
        fi
      else
        log "ERR" "end date passed in is not valid: $2"
        lock false
        exit 1
      fi
    fi
    log "INFO" "Date parameters: \n\tStartDate: $startDate \n\tEndDate:   $endDate"
  else
    log "ERR" "start date passed in is not valid: $1"
    lock false
    exit 1
  fi
  ;;
-a | --all )
  retrieveAll=true
  if [ ! -z "$2" ]
  then
    echo $2
    baseURL=$2
    log "INFO" "Date parameters: \n\tStartDate: $startDate \n\tEndDate:   $endDate"
    log "INFO" "Retrieve All parameter set to TRUE"
    log "INFO" "URL set to: $baseURL"
  else
    log "ERR" "URL to files is not present: $2"
    lock false
    exit 1
  fi
  ;;
-n | --none )
  retrieveNone=true
  if [ ! -z "$2" ]
  then
    echo $2
    baseURL=$2
    log "INFO" "Date parameters: \n\tStartDate: $startDate \n\tEndDate:   $endDate"
    log "INFO" "Retrieve NONE parameter set to TRUE"
    log "INFO" "URL set to: $baseURL"
  else
    log "ERR" "URL to files is not present: $2"
    lock false
    exit 1
  fi
  ;;
-h | --help )
  usage
  lock false
  exit
  ;;
* )
  usage
  lock false
  log "ERR" "argument passed in is not valid: $1"
  exit 1
esac

#create directory for downloaded files and logs(if does not exist already)
mkdir -p $dropLocation
mkdir -p $statusDirectory

log "INFO" "-[JOB START] $(date): ------------"

#startDate=$(date '+%C%y%m%d' -d "$startDate -$(date -d $startDate +%u) days + 5 day")
begDate=$startDate

if ! $retrieveNone
then
  log "INFO" "Starting file download process"
  while [ $begDate -le $endDate ]
  do
    #year=$(date -d $begDate +%Y) 
    #week=$(date -d $begDate +%V)
    #if [ $year -eq  2015 ]
    #then
    #  week="$(printf "%02d" $((10#$week-1)))"
    #fi
    #zipFilePath=${baseURL}PTAB_${begDate}_WK${week}.zip
    wget -q --spider $baseURL/$begDate
    if [ $? -eq 0 ]
    then
      log "INFO" "Downloading files from: $baseURL/$begDate"
      #wget -A zip -r -nc -np -P $dropLocation $baseURL/$begDate >> $statusDirectory/retrieve-log-$processingTime 2>&1
      wget -nc -r -A zip -nc -P $dropLocation -o $statusDirectory/retrieve-grant-log-$processingTime $baseURL/$begDate
    else 
      log "ERR" "file does not exist: $baseURL"
    fi
    begDate=$((begDate+1))
    #begDate=$(date '+%C%y%m%d' -d "$begDate+7 days")
  done
  log "INFO" "File download process complete"
#if --none flag is set then skip download process
else
  log "INFO" "skipping file download process"
fi

#unzip all zip files unless they have already been unzipped
log "INFO" "Starting file unzipping process"

find $dropLocation -type f -name "*.zip" -exec unzip -n {} -d $dropLocation \;

log "INFO" "File unzip process complete"

log "INFO" "-[JOB END]-- $(date): ------------"

lock false

#!/bin/bash

################################################
#	TDfload.sh
#	Usage: TDfload.sh -t tablename -i InputFile -d delimiter 
################################################

usage() {
  cat <<EOF
  usage : $0  -t tablename -i Input file -d delimiter
  Script to Run Teradata Fast Load
  OPTIONS:
	Mandatory
        -t      Table Name
        -i      Input File
        -d      Delimiter
        -h      Show this message
EOF
}

while getopts "t:i:d:h:" OPTION
do
    case $OPTION in
        t) export TABLENAME=$OPTARG;;
        i) export INFILE=$OPTARG;;
	d) export DELIMITER=$OPTARG;;
        h) usage
           exit
         ;;
        ?) usage
           exit
         ;;
    esac
done

. /appl/conf/$USER/global.properties
. /appl/common/scripts/log-funcs
. /appl/common/scripts/hadoop-funcs

export CLASSPATH=$CLASSPATH:/usr/lib/hadoop/lib/terajdbc4.jar:/usr/lib/hadoop/lib/tdgssconfig.jar

DATE_TODAY=${DATE_TODAY:-$(date +"%Y%m%d")}

work_dir=/staging/$USER
log_dir=/logs/$USER
log_file=$log_dir/${TABLENAME}_${DATE_TODAY}.log
echo $DATE_TODAY
export DATE_TODAY
export work_dir

if [[ -z $TABLENAME ]];then
    usage
    RC=1
    echo "ERROR - FastLoad Table $TABLENAME does not exist or not Generated.  Command:  $0 $*" 
    exit $RC
fi

if [[ -z $TERADATA_USERNAME ]]; then
  echo  "ERROR: TERADATA_USERNAME environment variable needs to be set"
  exit 1
fi

if [[ -z $TERADATA_PASSWORD ]]; then
  echo  "ERROR: TERADATA_PASSWORD environment variable needs to be set"
  exit 1
fi

log_init $log_file a

log "INFO:START ${TMP_TBL}_ fastload"

$(dirname $0)/TDfload.rb | fastload 

RC=$?

if [ $RC -ne 0 ];
  then
    log "ERROR - Fast Export Failed for script $QUERY_SCRIPT with return code $RC" 
    exit $RC

  else
    log "INFO - Fast Load Successful for script $QUERY_SCRIPT" 
fi


log "INFO:END ${TABLENAME} fast load completed"


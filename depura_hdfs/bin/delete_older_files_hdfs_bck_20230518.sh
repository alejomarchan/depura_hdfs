#!/bin/bash
set -x
clear && printf '\e[3J'

#Declaracion de Fechas
export FECHA_INICIO=$(date '+%d/%m/%Y %H:%M:%S')
export FECHA_LOG=$(date +%Y%m%d)
export FECHA_LOG_ALL=$(date +%Y%m%d%H%M%S)

export SCRIPT=$(readlink -f "$0")
export PATH_BIN=$(dirname "${SCRIPT}")

#Declaracion Directorios
export PATH_APP="${PATH_BIN%/*}"
export PATH_CONF="${PATH_APP}/conf"
export PATH_FUNCTIONS="${PATH_APP}/functions"
export PATH_LOG="${PATH_APP}/log"
export PATH_SQL="${PATH_APP}/sql"
export PATH_TMP="${PATH_APP}/tmp"
export PATH_HDFS_TMP="/user/hdpadmin/tmp_delete"

#Número de días a dejar vivos
dias=90
cores=10
batch_size=1000
#Directorio de hadoop
hadoop_path="/transferencias/clienteredsftp/SBC/corrup_file /user/cervantesc/U2000.core /user/cervantesc/U2000_TAISHAN /user/epaezrui/urm /user/gonzalezgui/.staging /user/hbecerra/ligas /user/hdpadmin/.staging /user/hdpadmin/BKP_Procesos /user/hdpadmin/VOZ_FIJA /user/hdpadmin/active_users_export /user/hdpadmin/huawei /user/hdpadmin/new_export /user/jcarogre/active_users_export /user/sfreiman/export /user/temis/.staging /staging/clienteredsftp/ENIQ"
hdfs dfs -mkdir "${PATH_HDFS_TMP}"

for folder_out in ${hadoop_path}; do
	#for folder_in in $(hdfs dfs -ls "${folder_out}" | grep '^d' | awk '{print $NF}'); do
	for folder_in in $(hdfs dfs -ls "${folder_out}" | awk '/^d/ {sub(/^[^ ]+ [^ ]+ [^ ]+ [^ ]+ /, ""); print $8,$9,$10}'); do
		folder_in=$(echo -e "${folder_in}" | sed 's/\%20/ /g')
#		list_files=$(hdfs dfs -ls "${folder_in}" | awk '!/^d/ {print $0}' | awk -v days=${dias} '$6 < strftime("%Y-%m-%d", systime() - days * 24 * 60 * 60) "{ print $8 }"')
		#list_files=$(hdfs dfs -ls "${folder_in}" | awk '!/^d/ {print $0}' | awk -v days=${dias} '!/^d/ && $6 < strftime("%Y-%m-%d", systime() - days * 24 * 60 * 60) { print substr($0, index($0,$8)) }')
		hdfs dfs -ls "${folder_in}" | awk '!/^d/ {print $0}' | awk -v days=${dias} '!/^d/ && $6 > strftime("%Y-%m-%d", systime() - days * 24 * 60 * 60) { print substr($0, index($0,$8)) }' | xargs -I {} hdfs dfs -mv "${folder_in}/*" "${PATH_HDFS_TMP}/"
		hdfs dfs -rm -f -skipTrash "${folder_in}/"{}
		hdfs dfs -mv "${PATH_HDFS_TMP}/*" "${folder_in}/"
	done
	hdfs dfs -ls "${folder_out}" | awk '!/^d/ {print $0}' | awk -v days=${dias} '!/^d/ && $6 > strftime("%Y-%m-%d", systime() - days * 24 * 60 * 60) { print substr($0, index($0,$8)) }' | xargs -I {} hdfs dfs -mv "${folder_out}/*" "${PATH_HDFS_TMP}/"
	hdfs dfs -rm -f -skipTrash "${folder_out}/*"
	hdfs dfs -mv "${PATH_HDFS_TMP}/*" "${folder_out}/"
done

hdfs dfs -rm -r -skipTrash "${PATH_HDFS_TMP}"
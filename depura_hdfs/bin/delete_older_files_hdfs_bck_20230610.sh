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
dias=10
cores=10
batch_size=1000
#Directorio de hadoop
hadoop_path=$(cat ${PATH_CONF}/hdfs_folder.conf)
#hdfs dfs -mkdir "${PATH_HDFS_TMP}"

while IFS= read -r folder; do
	hdfs dfs -ls -R "${folder}" | awk '/^d/ {for (i=8; i<=NF; i++) printf "%s%s", $i, (i<NF?OFS:ORS)}' > "${PATH_TMP}/alejo.txt"
	while IFS= read -r line; do
		# Process each line here
		awk_output=$(printf '%s\0' "$line"/* | xargs -0 hdfs dfs -stat '%Y,%F,%n/' | awk -v days="$dias" 'BEGIN {RS = "/"; FS = ","; basepath = ARGV[1]; delete ARGV[1];srand(); modtime = (srand()*1000 - days * 86400);} $2 == "regular file" && $1 < modtime {sub(/^([^ ]* ){3}/,""); printf("%s%c", basepath "/" $0, 0)}')
		if [ -n "$awk_output" ]; then
            # awk command produced non-empty output, execute xargs -0 hdfs dfs -ls
			echo ${awk_output}
            printf '%s\0' "${awk_output}" | xargs -0 hdfs dfs -ls
        else
            echo "awk command failed for line: $line"
        fi
		
		#xargs -0 hdfs dfs -rm -f -skipTrash
		break
	done <"${PATH_TMP}/alejo.txt"
	#awk_output=$(printf '%s\0' "$folder"/* | xargs -0 hdfs dfs -stat '%Y,%F,%n/' | awk -v days="$dias" 'BEGIN {RS = "/"; basepath = ARGV[1]; delete ARGV[1]; modtime = (srand() - days * 86400) * 1000} $2 == "regular file" && $1 < modtime {sub(/^([^,]*,){2}/,""); printf("%s%c", basepath "/" $0, 0)}' "$folder")
	awk_output=$(printf '%s\0' "$folder"/* | xargs -0 hdfs dfs -stat '%Y,%F,%n/' | awk -v days="$dias" -F "," 'BEGIN { RS = "/"; basepath = ARGV[1]; delete ARGV[1]; srand(); modtime = (srand() - days*86400)*1000; } $2 == "regular file" && $1 < modtime { sub(/^([^ ]* ){3}/,""); printf("%s%s", basepath "/" $3, ORS) }' "$folder" | wc -l)
	if [ "$awk_output" -gt 0 ]; then
		# awk command produced non-empty output, execute xargs -0 hdfs dfs -ls
		#echo ${awk_output}
		#printf '%s\0' "$folder"/* | xargs -0 hdfs dfs -stat '%Y,%F,%n/' | awk -v days="$dias" -F "," 'BEGIN { RS = "/"; basepath = ARGV[1]; delete ARGV[1]; srand(); modtime = (srand() - days*86400)*1000; } $2 == "regular file" && $1 < modtime { sub(/^([^ ]* ){3}/,""); printf("%s%s", basepath "/" $3, ORS) }' "$folder" | xargs -n1 hdfs dfs -ls
		printf '%s\0' "$folder"/* | xargs -0 hdfs dfs -stat '%Y,%F,%n/' | awk -v days="$dias" -F "," 'BEGIN { RS = "/"; basepath = ARGV[1]; delete ARGV[1]; srand(); modtime = (srand() - days*86400)*1000; } $2 == "regular file" && $1 < modtime { sub(/^([^ ]* ){3}/,""); printf("%s", basepath "/" $3, "\n") }' "$folder" | xargs -0 hdfs dfs -ls
	else
		echo "awk command failed for line: $line"
	fi
	break
done <"${PATH_CONF}/hdfs_folder.conf"

###################################################
#printf '%s\0' "$folder_in"/* |
#xargs -0 hdfs dfs -stat '%Y %F %n/' |
#awk -v days="$dias" '
#    BEGIN {
#        RS = "/";
#        basepath = ARGV[1];
#        delete ARGV[1];
#        srand();
#        modtime = strftime("%s")*1000 - days * 86400
#    }
#    $3 == "file" && $1 < modtime {
#        sub(/^([^ ]* ){3}/,"");
#        printf("%s%c", basepath "/" $0, 0)
#    }
#' "$folder_in" |
#xargs -0 hdfs dfs -rm -f -skipTrash
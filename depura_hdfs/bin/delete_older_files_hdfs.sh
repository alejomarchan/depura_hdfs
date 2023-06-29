#!/bin/bash
set -x
clear && printf '\e[3J'

#Nombre script
export PROCESS_NAME=`basename "$0" .sh`


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

#Definiendo nombres log
export LOG_FILE_NAME=${FECHA_LOG}"_"${PROCESS_NAME}".log"
export LOG_FILE_NAME_ALL=${FECHA_LOG_ALL}"_"${PROCESS_NAME}"_all.log"

#Definiendo la ubicacion
export LOG_FILE="${PATH_LOG}/${LOG_FILE_NAME}"
export LOG_FILE_ALL="${PATH_LOG}/${LOG_FILE_NAME_ALL}"

#Archivo Lista HDFS
export file_hadoop_list="file_hadoop_list.txt"
export file_hadoop_list_tmp="file_hadoop_list_tmp.txt"

#Número de días a dejar vivos
dias=20
cores=10
maxFilesLote=30000

#Variable para anotar cantidad de archivo encontrados
amount_files=0

#Directorio de hadoop
hadoop_path=$(cat ${PATH_CONF}/hdfs_folder.conf)
#hdfs dfs -mkdir "${PATH_HDFS_TMP}"

#Función para la escritura de logs
function log {
	STATUS=$1
	LOG_FILE=$2
	MESSAGE=$3

	PREFIX=$(date "+%Y/%m/%d %H:%M:%S")

	case $STATUS in
			0) PREFIX=$PREFIX" - [INF] - ";;
			1) PREFIX=$PREFIX" - [WRN] - ";;
			2) PREFIX=$PREFIX" - [ERR] - ";;
			*) PREFIX=$PREFIX" - ";;
	esac

	echo $PREFIX$MESSAGE | tee -a $LOG_FILE
}

# Descripción: busca los archivos en el hdfs que tengan más de x días y los almacena en un txt
# Parámetros:
#   $1 - El directorio del HDFS donde quiero buscar los archivos
#   $2 - El directorio de Unix donde se almacenará el archivo con el listado
#   $3 - La cantidad de días establecidos
#   $4 - El nombre del archivo con el que almacenará en unix el listado de los archivos del HDFS que cumplen la condición
# Retorna:
#   Un archivo en un directorio Unix.
fn_get_oldest_files_hdfs() {
    local path_hdfs="$1"
    local path_unix_tmp="$2"
	local dias="$3"
	local file_ouput_name="$4"
	printf '%s\0' "$path_hdfs"/* | xargs -0 hdfs dfs -stat '%Y,%F,%n/' | sort -k1 | awk -v days=$dias -F "," 'BEGIN { srand(); threshold = (srand() - days*86400)*1000; } $2 == "regular file" && $1 < threshold { print }'>"${path_unix_tmp}/${file_ouput_name}"
}

# Descripción: Función que ejecuta el comando rm de los archivos que se encuentren dentro del listado
# Parámetros:
#   $1 - El directorio del HDFS donde quiero eliminar los archivos
#   $2 - El directorio de Unix donde se encuentra el archivo con el listado
#   $3 - El nombre del archivo en unix con el listado de los archivos del HDFS que cumplen la condición
# Retorna:
#   Borra los archivos contenidos en la lista.
fn_hdfs_command() {
    local path_hdfs="$1"
    local path_unix_tmp="$2"
	local file_ouput_name="$3"
	if [ -f "${path_unix_tmp}/${file_ouput_name}" ]; then
		if ! [ -z "${path_hdfs}" ]; then
			hdfs dfs -test -d "${path_hdfs}"
			if [ $? -eq 0 ]; then
				#cat "${path_unix_tmp}/${file_ouput_name}" | awk -v basepath="${path_hdfs}" '{ sub(/^([^,]*,){2}/,""); printf("%s%c", basepath "/" $0, 0) }' | xargs -0 hdfs dfs -ls -C
				cat "${path_unix_tmp}/${file_ouput_name}" | awk -v basepath="${path_hdfs}" '{ sub(/^([^,]*,){2}/,""); printf("%s%c", basepath "/" $0, 0) }' | xargs -0 hdfs dfs -rm -skipTrash
			else
				echo "El directorio ${path_hdfs} no existe"
			fi
		else
			echo "La variable path_hdfs vino vacía"
		fi
	else
		echo "No existe el archivo ${path_unix_tmp}/${file_ouput_name}"
	fi
}

log 0 "${LOG_FILE}" "Inicio del proceso ${PROCESS_NAME}"

exec > "${LOG_FILE_ALL}" 2>&1


while IFS= read -r outer_folder; do
	log 0 "${LOG_FILE}" "Revisión HDFS: directorio externo ${outer_folder}"
	hdfs dfs -ls -R "${outer_folder}" | grep "^d" | awk '/^d/ {for (i=8; i<=NF; i++) printf "%s%s", $i, (i<NF?OFS:ORS)}'>"${PATH_TMP}/inner_hdfs_folder.txt"
	while IFS= read -r inner_folder; do
		# Process each line here
		fn_get_oldest_files_hdfs "${inner_folder}" "${PATH_TMP}" "${dias}" "${file_hadoop_list}"
		# Cantidad de archivos a revisar
		RECORDS=$(echo $(cat "${PATH_TMP}/${file_hadoop_list}" | wc -l))
		log 0 "${LOG_FILE}" "Revisión HDFS: directorio interno ${outer_folder}. Cantidad de archivo encontrados: ${RECORDS}"
		amount_files=$(($amount_files+$RECORDS))
		# Armado del lote de archivos separado por maxFilesLote
		sed -n "1,${maxFilesLote}p" "${PATH_TMP}/${file_hadoop_list}" > "${PATH_TMP}/lote_actual.txt"
		sed "1,${maxFilesLote} d" "${PATH_TMP}/${file_hadoop_list}" > "${PATH_TMP}/${file_hadoop_list_tmp}"
		rm -f "${PATH_TMP}/${file_hadoop_list}"
		mv "${PATH_TMP}/${file_hadoop_list_tmp}" "${PATH_TMP}/${file_hadoop_list}"
		while [ $RECORDS -gt 0 ]; do
			fn_hdfs_command "${inner_folder}" "${PATH_TMP}" "lote_actual.txt"
			RECORDS=$(echo $(cat "${PATH_TMP}/${file_hadoop_list}" | wc -l))
			# Armado del lote de archivos separado por maxFilesLote
			sed -n "1,${maxFilesLote}p" "${PATH_TMP}/${file_hadoop_list}" > "${PATH_TMP}/lote_actual.txt"
			sed "1,${maxFilesLote} d" "${PATH_TMP}/${file_hadoop_list}" > "${PATH_TMP}/${file_hadoop_list_tmp}"
			rm -f "${PATH_TMP}/${file_hadoop_list}"
			mv "${PATH_TMP}/${file_hadoop_list_tmp}" "${PATH_TMP}/${file_hadoop_list}"
		done
		#xargs -0 hdfs dfs -rm -f -skipTrash
	done < "${PATH_TMP}/inner_hdfs_folder.txt"
	rm -f "${PATH_TMP}/${file_hadoop_list}" "${PATH_TMP}/lote_actual.txt" "${PATH_TMP}/${file_hadoop_list_tmp}" #"${PATH_TMP}/inner_hdfs_folder.txt"
	
	fn_get_oldest_files_hdfs "${outer_folder}" "${PATH_TMP}" "${dias}" "${file_hadoop_list}"
	RECORDS=$(echo $(cat "${PATH_TMP}/${file_hadoop_list}" | wc -l))
	log 0 "${LOG_FILE}" "Cantidad archivos en directorio externo ${outer_folder}: ${RECORDS}"
	amount_files=$(($amount_files+$RECORDS))
	sed -n "1,${maxFilesLote}p" "${PATH_TMP}/${file_hadoop_list}" > "${PATH_TMP}/lote_actual.txt"
	sed "1,${maxFilesLote} d" "${PATH_TMP}/${file_hadoop_list}" > "${PATH_TMP}/${file_hadoop_list_tmp}"
	rm -f "${PATH_TMP}/${file_hadoop_list}"
	mv "${PATH_TMP}/${file_hadoop_list_tmp}" "${PATH_TMP}/${file_hadoop_list}"
	while [ $RECORDS -gt 0 ]; do
		fn_hdfs_command "${outer_folder}" "${PATH_TMP}" "lote_actual.txt"
		
		RECORDS=$(echo $(cat "${PATH_TMP}/${file_hadoop_list}" | wc -l))
		# Armado del lote de archivos separado por maxFilesLote
		sed -n "1,${maxFilesLote}p" "${PATH_TMP}/${file_hadoop_list}" > "${PATH_TMP}/lote_actual.txt"
		sed "1,${maxFilesLote} d" "${PATH_TMP}/${file_hadoop_list}" > "${PATH_TMP}/${file_hadoop_list_tmp}"
		rm -f "${PATH_TMP}/${file_hadoop_list}"
		mv "${PATH_TMP}/${file_hadoop_list_tmp}" "${PATH_TMP}/${file_hadoop_list}"
	done
	rm -f "${PATH_TMP}/${file_hadoop_list}" "${PATH_TMP}/lote_actual.txt" "${PATH_TMP}/${file_hadoop_list_tmp}"
done <"${PATH_CONF}/hdfs_folder.conf"

log 0 "${LOG_FILE}" "Cantidad total de archivos depurados --> ${amount_files}"
log 0 "${LOG_FILE}" "Fin del proceso ${PROCESS_NAME}"
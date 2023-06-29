# depura_hdfs
Proceso para depurar miles de archivos de Hadoop HDFS por lotes que tenga más de x días de creados. 

El shell delete_older_files_hdfs.sh recibe por parámetro un archivo de configuración que contiene los directorios del hdfs que se desean depurar. El script hace una búsqueda recursiva dentro de esos directorios, lista los archivos que cumplen la condición de tener x día, crea un archivo temporal con la ubicación y el nombre de esos archivos y luego procede a eliminarlos por lotes.

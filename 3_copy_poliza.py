'''
Created on Tue March 08 2021 13:30:00 
copy_poliza 
author: @rluna10

'''
#Importaando librerias
import os
import shutil

# Obteniendo ruta de directorio actual
archivos = os.listdir('.')  # "." el punto significa directorio actual

# Archivo a copiar
archivo = 'axa_generic.pdf'

# listar carpetas en directorio actual
for d in archivos:
    if os.path.isdir(d):
            #copiar pdf a lista de carpetas:
                shutil.copy(archivo, d)
                print('Finish :D')
        


    

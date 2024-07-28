import polars as pl

import xlsxwriter
#######################################  DATA TICKETS  ##########################

##### TICKETS HISTÓRICOS

dth= pl.read_csv(
    source='.\Entorno\Archivos\Tickets\Tickets Historico.txt',
    separator= ';',
    has_header=True,
    ignore_errors=True,
    columns=[
        'Numero Ticket',
        'Ubicacion',
        'Service Desk',
        'Estado',
        'Fecha Creacion',
        'Fecha Termino',
        'Fecha Cierre'
    ]
    )


dth=dth.rename({'Numero Ticket':'TicketID'})

dth=dth.select(
    pl.col('TicketID'),
    pl.col('Ubicacion'),
    pl.col('Service Desk'),
    pl.col('Estado'),
    pl.col('Fecha Creacion').str.to_date("%Y-%m-%d"),
    pl.col('Fecha Termino').str.to_date("%Y-%m-%d"),
    pl.col('Fecha Cierre').str.to_date("%Y-%m-%d")
)

########################################3

### TICKETS ACTUAL

dta= pl.read_csv(
    source='.\Entorno\Archivos\Tickets\Tickets Actual.csv',
    separator= '|',
    has_header=True,
    ignore_errors=True,
    columns=[
        'Numero Ticket',
        'Ubicacion',
        'Service Desk',
        'Estado',
        'Fecha Creacion',
        'Fecha Termino',
        'Fecha Cierre'
    ]
    )


dta=dta.rename({'Numero Ticket':'TicketID'})

dta=dta.select(
    pl.col('TicketID'),
    pl.col('Ubicacion'),
    pl.col('Service Desk'),
    pl.col('Estado'),
    pl.col('Fecha Creacion').str.to_date("%Y-%m-%d"),
    pl.col('Fecha Termino').str.to_date("%d/%m/%Y"),
    pl.col('Fecha Cierre').str.to_date("%d/%m/%Y")
)

dta=dta.filter(
    pl.col('TicketID').str.starts_with('WO')
)

##########################################333

##### CONSOLIDACIÓN DE TICKETS
df_Tickets=pl.concat([dta,dth],how='vertical')


### ELIMINAR DUPLICADOS

df_Tickets=df_Tickets.sort( # 2) Ordenamos los duplicados primero según el TicketID y luego por Fecha para que se agrupen en líneas consecutivas
    by=['TicketID','Fecha Creacion']
).unique(  # 3) En base a lo anterior, elegimos el registro que queremos mantener como único
    subset=['TicketID'], ## en versiones anteriores se usa el "by"
    keep='last',
    maintain_order=True    
)



### DIVIDIR COLUMNA UBICACION

df_Tickets=df_Tickets.with_columns(
pl.col('Ubicacion').str.split_exact(" - ",1).struct.rename_fields(['Agencia','AgenciaID'])
).unnest('Ubicacion').cast({"AgenciaID": pl.Int64})

### AGREGAR NUEVA COLUMNA Fecha Real Fin

df_Tickets=df_Tickets.with_columns (
	pl.coalesce(['Fecha Termino','Fecha Cierre']).alias('Fecha Real Fin'),
)

### DIFERENCIA DE FECHAS - DIAS CIERRE

df_Tickets=df_Tickets.with_columns (
    (pl.col('Fecha Real Fin') - pl.col('Fecha Creacion')).dt.total_days().alias('Dias Cierre')
)

### CLASIFICACIÓN EN GRUPO DIAS

df_Tickets=df_Tickets.with_columns(
    pl.when( pl.col('Dias Cierre').is_null() ).then(None)
    .when( pl.col('Dias Cierre')<=3 ).then(pl.lit('0 a 3 dias'))
    .when( pl.col('Dias Cierre')<=7 ).then(pl.lit('4 a 7 dias'))
    .when( pl.col('Dias Cierre')<=15 ).then(pl.lit('8 a 15 dias'))
    .otherwise(pl.lit('+15 dias')).alias('Grupo Dias')
)


############################ DATA ATENCIONES ###############################

rutas_excel=['.\Entorno\Archivos\Atenciones\Atenciones Centro.xlsx',
      '.\Entorno\Archivos\Atenciones\Atenciones Norte.xlsx',
      '.\Entorno\Archivos\Atenciones\Atenciones Sur.xlsx']


# df_Atenciones=pl.DataFrame() Opcional definir un DataFrame vacío
for excel in rutas_excel:
    
    aux_excel=pl.read_excel(
    source= excel,
    engine='xlsx2csv',
    sheet_id=1,
    read_options={
        "columns":['Numero Ticket','Tipo de Ticket','Proveedor','Costo Atencion'],
        "dtypes":{'Costo Atencion':pl.Utf8}
    }   )
    
    aux_excel=aux_excel.rename({'Numero Ticket':'TicketID'})
    df_Atenciones=pl.concat([aux_excel],how='vertical')

df_Atenciones= df_Atenciones.select(
    pl.col(['TicketID','Tipo de Ticket','Proveedor']),
    pl.when( ( (pl.col(['Costo Atencion']).str.to_uppercase().replace(',','.'))=='SIN COSTO') | ( (pl.col(['Costo Atencion']).str.to_uppercase().replace(",","."))=='COSTO CERO') ).then(pl.lit('0')).otherwise(pl.col('Costo Atencion')).alias('Costo Atencion')
    
)##.cast({'Costo Atencion':pl.Float64})

def convertir_decimal(a:str): 
    try:
        dec= round(float(a),2)
        return dec
    except:       
        return None

df_Atenciones=df_Atenciones.with_columns(
    pl.col('Costo Atencion').map_elements(convertir_decimal,return_dtype=float)
)
    
###################################


############################ COMBINAR Y EXPORTAR DATA TICKETS - DATA ATENCIONES ###############################

Resultado_Consolidado=df_Tickets.join(
    df_Atenciones,
    on='TicketID',
    how='inner'
).select(
    'TicketID',
    'AgenciaID',
    'Agencia',
    'Service Desk',
    'Estado',
    'Fecha Creacion',
    pl.col('Fecha Real Fin').alias('Fecha Cierre'),
    'Grupo Dias',
    pl.col('Tipo de Ticket').alias('Tipo Ticket'),
    pl.col('Costo Atencion').alias('Costo')
)

Resultado_Consolidado.write_excel(
	workbook='Consolidado.xlsx', 
	worksheet='Reporte',
	autofit=True ,
	dtype_formats= {pl.Date:"dd/mm/yyyy"} , 
	float_precision=2,  
	table_style= 'Table Style Medium 4'
)
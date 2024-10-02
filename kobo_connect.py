library(httr)
library(jsonlite)
library(readr)
kobo_server_url <- "https://kf.kobotoolbox.org/"
kc_server_url <- "https://kc.kobotoolbox.org/"

form_reg <- 421335 #Registrasi
form_app <- 623420 #App

### Registrasi ###
url_reg <- paste0(kc_server_url,"api/v1/data/",form_reg,"?format=csv")
rawdata_reg  <- GET(url_reg,authenticate("vamprk2020","Icraf2019!"),progress())
registKoboData  <- read_csv(content(rawdata_reg,"raw",encoding = "UTF-8"))
saveRDS(registKoboData, "data/registKoboData")

### App ###
url_app <- paste0(kc_server_url,"api/v1/data/",form_app,"?format=csv")
rawdata_app  <- GET(url_app,authenticate("vamprk2020","Icraf2019!"),progress())
vamKoboData  <- read_csv(content(rawdata_app,"raw",encoding = "UTF-8"))
saveRDS(vamKoboData, "data/vamKoboData")



AKUN KOBO
koboform.cifor.org/api/v1/data/75
walm_admin
cifor2024

TESTING LOKAL DB
52.247.245.245/cbrms/map

AKUN VM

PYKOBO
https://github.com/pvernier/pykobo/blob/main/pykobo/form.py
https://medium.com/@misamuna/using-python-to-export-kobotoolbox-data-914b5ad555e7

SQLALCHEMY
https://coderpad.io/blog/development/sqlalchemy-with-postgresql/
https://www.datacamp.com/tutorial/tutorial-postgresql-python

INSERT TABLE
https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
https://www.geeksforgeeks.org/how-to-insert-a-pandas-dataframe-to-an-existing-postgresql-table/
https://medium.com/@askintamanli/fastest-methods-to-bulk-insert-a-pandas-dataframe-into-postgresql-2aa2ab6d2b24
https://github.com/NaysanSaran/pandas2postgresql/blob/master/notebooks/Psycopg2_Bulk_Insert_Speed_Benchmark.ipynb


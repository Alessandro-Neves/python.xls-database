characteristics = {
  "abertura" : 1,
  "acabamento" : 2,
  "bitola" : 16,
  "pressao_max" : 17,
  "pressao_min" : 18,
  "norma" : 19
}

# manipuladores de data e hora
from datetime import date, datetime, timedelta
import calendar

# métodos de sistema
import sys
import math

# ler/manipular arquivos do excel (.xls)
import pandas as pd

# conexão no banco de dados mysql
import mysql.connector

# conectar oa mysql
database = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root",
  database="novo_gestor_web"
)

# apagar logs de erro anteriores
open("error-log.txt", "w").close()


def register_characteristic(company_id, main_characteristic_id, characteristic, product_id):

  if(str(characteristic) == "nan"):
    return True

  # criar cursor database
  cursor = database.cursor()

  sql_search_characteristic = 'SELECT id from characteristics_descriptions WHERE description = "{}" AND characteristic = "{}" AND company = "{}"'.format(characteristic, main_characteristic_id, company_id)
  cursor.execute(sql_search_characteristic)

  find_characteristic_res = cursor.fetchall()
  finded= bool(find_characteristic_res)

  if not finded:
    print("[ Caracteristica não encontrada: {} ]".format(characteristic, end=""))
    return False
  sql_search_characteristic = 'SELECT id from characteristics_descriptions WHERE description = "{}" AND characteristic = "{}" AND company = "{}"'.format(characteristic, main_characteristic_id, company_id)
  cursor.execute(sql_search_characteristic)

  find_characteristic_res = cursor.fetchall()
  finded= bool(find_characteristic_res)

  if not finded:
    print("[ Caracteristica não encontrada: {} ]".format(characteristic, end=""))
    return False
  
  characteristic_id = int(find_characteristic_res[0][0])

  sql_insert_product_service_characteristic = "INSERT INTO characteristics_product_services (characteristic_description, products_service) VALUES (%s, %s)"

  try:
    cursor.execute(sql_insert_product_service_characteristic, (int(characteristic_id), int(product_id)))
    return True
  except:
    print(" [Falha ao adiciona caracteritica de {} ao produto {}: ({}, {})] ".format(main_characteristic_id, product_id, characteristic_id, product_id), end="")
    return False


def main():
  # criar cursor database
  cursor = database.cursor()

  # ler planilha
  xls = pd.ExcelFile('./produtos.xls')

  # input company 
  company = int(input("id da empresa:\t"))

  # verificar se a empresa existe no banco de dados
  sql_search_company = 'SELECT * from companies where id = "{}"'.format(company)
  cursor.execute(sql_search_company)
  find_company = bool(len(cursor.fetchall()))

  # casos a empresa não exista, encerra a execução
  if not find_company:
    print("\n\n[ Empresa não encontrada ]\n")
    sys.exit(0)

  # ler primeira folha de planilha
  spreadsheet = xls.parse(0)

  # numero de lines resultantes da query sql
  number_of_lines = spreadsheet['CODIGO'].size

  # query sql para inserir elementos como product_service
  sql_insert = "INSERT INTO products_services (title, description, specification, category, company, disabled, updatedAt) VALUES (%s, %s, %s, %s, %s, %s, %s)" 

  # company = 0

  # manipula o dados da planilha e insere no banco de dados
  # log de erro caso ocorra na query sql

  for index in range(number_of_lines):
    if index > 1:
      break

    category = 21
    title = spreadsheet['NOME_PRODUTO'][index]
    code = spreadsheet['CODIGO'][index]
    aperture = spreadsheet['ABERTURA'][index]
    default = spreadsheet['NORMA'][index]
    gauge = spreadsheet['BITOLA'][index]
    min_pressure = spreadsheet['PRESSÃO DE FUNCIONAMENTO MINIMO'][index]
    max_pressure = spreadsheet['PRESSÃO DE FUNCIONAMENTO MÁXIMO'][index]
    workmanship = spreadsheet['ACABAMENTO'][index]
    # price = spreadsheet['PRECO'][index]

    specification = ""

    specification = specification + '<p style="text-align: start"><strong>{}: </strong>{}</p>'.format("Código", code)
    specification = specification + '<p style="text-align: start"><strong>{}: </strong>{}</p>'.format("Titulo", title)
    specification = specification + '<p style="text-align: start"><strong>{}: </strong>{}</p>'.format("Abertura", aperture)
    specification = specification + '<p style="text-align: start"><strong>{}: </strong>{}</p>'.format("Norma", default)
    specification = specification + '<p style="text-align: start"><strong>{}: </strong>{}</p>'.format("Bitola", gauge)
    specification = specification + '<p style="text-align: start"><strong>{}: </strong>{}</p>'.format("Pressão Minima", min_pressure)
    specification = specification + '<p style="text-align: start"><strong>{}: </strong>{}</p>'.format("Pressão Máxima", max_pressure)
    # specification = specification + '<p style="text-align: start"><strong>{}: </strong>{}</p>'.format("Preço", price)

    error_log = open("error-log.txt", "a")

    try:
      cursor.execute(sql_insert, (str(title), " ", str(specification), str(category), company, 0, datetime.now()))
      product_id = cursor.lastrowid

      aperture_insert = register_characteristic(company, characteristics["abertura"], aperture, product_id)
      gauge_insert = register_characteristic(company, characteristics["bitola"], gauge, product_id)
      press_min_insert = register_characteristic(company, characteristics["pressao_min"], min_pressure, product_id)
      press_max_insert = register_characteristic(company, characteristics["pressao_max"], max_pressure, product_id)
      default_insert = register_characteristic(company, characteristics["norma"], default, product_id)
      workmanship_insert = register_characteristic(company, characteristics["acabamento"], workmanship, product_id)

      if(aperture_insert and gauge_insert and press_min_insert and press_max_insert and default_insert and workmanship_insert):
        database.commit()
        pass
      else:
        print("[ Erro ao cadastrar caracteristica de produto - fazendo rollback da transação ]", end="")
        raise Exception("Doing rollback!")

      database.rollback()
    except Exception as e:
      print("\n[ Erro ao inserir elemento no banco de dados - código {} ]".format(code))
      error_log.write(str(code) + "\t\t-\t\t" + str(code) + "at" + str(title) + "\t" + str(price) + "\t" + str(aperture) + "\t" + str(default) + "\t" + str(gauge) + "\t" + str(min_pressure) + "\t" + str(max_pressure) + "\t" + str(datetime.now()) + "\n")
      print(e)

  # fechar arquivo de log
  error_log.close()
  print("\n[ Dados persistidos ]")

main()
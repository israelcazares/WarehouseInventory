#!/usr/bin/env python
# -*- coding: utf-8 -*-

import httplib
from urlparse import urlparse

import pandas as pd
import xml.etree.cElementTree as ET
import os.path

import pymysql
import config
from sqlalchemy import create_engine
import newrelic.agent

#inicializar newrelic
newrelic.agent.initialize('/usr/local/lib/python2.7/dist-packages/newrelic-2.74.0.54/newrelic/newrelic.ini')

def get_xml(url):
	u = urlparse(url)
	httpClient = httplib.HTTPConnection(u.hostname, u.port)
	httpClient.connect()
	httpClient.request('GET', u.path+'?'+u.query, None)
	response = httpClient.getresponse()
	
	xml = ''
	if response.status == httplib.OK:
		xml = response.read()
	else:
		print 'Error:${alignr} Sin respuesta del servidor'
	
	return xml

def iter_xml(articulos):
	count=1
	for item in articulos.iterfind('.//item'):
		row={}
		row['SKU'] = item.find('codigo_fabricante').text
		row['SKU_Proveedor'] = item.find('clave').text
		row['Proveedor'] = 'CVA'
		row['PartNumber'] = row['SKU']
		row['BrandName'] = item.find('marca').text

		'''ficha_tecnica = item.find('ficha_tecnica').text
		ficha_comercial = item.find('ficha_comercial').text
		
		if ficha_tecnica == None or ficha_comercial == None:
			continue'''
		
		subcat = item.find('subgrupo').text
		if subcat == None:
			subcat = ''
		
		'''row['DescriptionLong'] = u"{0}\n{0}".format(ficha_tecnica, ficha_comercial)
		row['DescriptionShort'] = item.find('descripcion').text
		row['Category'] = 'Almacenamiento'''
		row['SubCat'] = subcat

		importe = round(float(item.find('precio').text), 2)
		moneda = item.find('moneda').text

		if moneda == 'Dolar' or moneda == 'Dolares':
			importe = importe * config.dolar_cva

		iva = round(importe * 0.16, 2)
		subtotal = round(importe + iva, 2)
		margen = round(subtotal * config.delta / 100, 2)
		precioFinal = round(subtotal + margen, 2)

		row['Precio'] = importe
		row['PrecioIva'] = subtotal
		row['PrecioFinal'] = precioFinal
		row['Stock'] = item.find('disponible').text
		'''row['Dolar'] = config.dolar_cva
		row['Height'] = 0
		row['Width'] = 0
		row['Lenght'] = 0
		row['Weight'] = 0
		row['thumb'] = item.find('imagen').text
		row['largeImg'] = item.find('imagen').text'''

		yield row

def main():
	xml=get_xml('ServiceURL')
	ruta = os.path.dirname(os.path.realpath(__file__))
	xml_file = open('{0}/cva_todo.xml'.format(ruta), 'w')
	xml_file.write(xml)
	xml_file.close()

	# Convertir xml a pandas
	etree = ET.fromstring(xml)
	df = pd.DataFrame(list(iter_xml(etree)))
	df.set_index('SKU', inplace=True)
	df.sort_index(axis=1)


	conn = pymysql.connect(host='1.1.1.1', port=3306, user='user', passwd='password', db='homestore')
	cur = conn.cursor()
	cur.execute("Truncate table productos")
	cur.close()
	conn.close()

	engine =   create_engine("mysql+pymysql://user:pass@1.1.1.1:3306/homestore")
	df.to_sql('productos', engine, index=True, if_exists='append')

if __name__ == '__main__':
	main()


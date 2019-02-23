#!/usr/bin/env python
# -*- coding: utf-8 -*-

import httplib
from urlparse import urlparse

import pandas as pd
import xml.etree.cElementTree as ET
import os.path
import base64

import pymysql
import config
from sqlalchemy import create_engine
import newrelic.agent

#inicializar newrelic
newrelic.agent.initialize('/usr/local/lib/python2.7/dist-packages/newrelic-2.74.0.54/newrelic/newrelic.ini')
#ruta
ruta = os.path.dirname(os.path.realpath(__file__))

def get_soap(url):
	u = urlparse(url)
	keyData = base64.b64decode(config.key_pch_servicio).split(':')
	method = 'ObtenerListaArticulos'
	data = '<?xml version="1.0" encoding="UTF-8"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="http://{0}/" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" SOAP-ENV:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><SOAP-ENV:Body><ns1:{1}><cliente xsi:type="xsd:int">{2}</cliente><llave xsi:type="xsd:string">{3}</llave></ns1:{1}></SOAP-ENV:Body></SOAP-ENV:Envelope>'.format(u.hostname, method, keyData[0], keyData[1])
	httpClient = httplib.HTTPConnection(u.hostname, u.port)
	httpClient.connect()
	httpClient.request('POST', u.path, data, {
		'Host': u.hostname,
		'Content-Type': 'text/xml; charset=utf-8',
		'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
		'SOAPAction': 'http://{0}/{1}/{2}'.format(u.hostname, u.path, method)
	})
	response = httpClient.getresponse()
	
	xml = ''
	if response.status == httplib.OK:
		xml = response.read()
	else:
		print 'Error: Sin respuesta del servidor'
	
	return xml

def get_producto(sku):
	try:
		df = pd.read_hdf('{0}/productos.h5'.format(ruta), 'productos', where=u'SKU="{0}"'.format(sku))
		lista = df.to_dict(orient='records')
		if len(lista) > 0:
			row = lista[0]
			return row
		else:
			return None
	except:
		return None

def iter_xml(articulos):
	for item in articulos.iterfind('.//datos/item'):
		row={}
		row['SKU'] = item.find('skuFabricante').text

		subcat = item.find('linea').text
		if subcat == None:
			subcat = ''
		
		#row['Category'] = item.find('seccion').text #'Almacenamiento'
		#row['SubCat'] = subcat

		'''if row['Category'] != 'DISCOS DUROS' and row['Category'] != 'MEMORIAS':
			continue

		row['Category'] = 'Dispositivos de Video'''
		row['SubCat'] = item.find('linea').text
		
		row['SKU_Proveedor'] = item.find('sku').text
		row['Proveedor'] = 'PCH'
		row['PartNumber'] = row['SKU']
		row['BrandName'] = item.find('marca').text
		
		importe = round(float(item.find('precio').text), 2)
		moneda = item.find('moneda').text

		if moneda == 'USD':
			importe = importe * config.dolar_pch

		iva = round(importe * 0.16, 2)
		subtotal = round(importe + iva, 2)
		margen = round(subtotal * config.delta / 100, 2)
		precioFinal = round(subtotal + margen, 2)

		row['Precio'] = importe
		row['PrecioIva'] = subtotal
		row['PrecioFinal'] = precioFinal
		row['Stock'] = item.find('.//existencia').text
		
		yield row

def main():
	xml=get_soap('ServiceURL')
	ruta = os.path.dirname(os.path.realpath(__file__))
	xml_file = open('{0}/pch_todo.xml'.format(ruta), 'w')
	xml_file.write(xml)
	xml_file.close()

	# Convertir xml a pandas
	etree = ET.fromstring(xml)
	df = pd.DataFrame(list(iter_xml(etree)))

	if not df.empty:
		df.set_index('SKU', inplace=True)
		engine =   create_engine("mysql+pymysql://user:pass@1.1.1.1:3306/homestore")
		df.to_sql('productos', engine, index=True, if_exists='append')
	

if __name__ == '__main__':
	main()



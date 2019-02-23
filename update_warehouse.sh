#!/bin/bash


cd /home/ubuntu/proveedores


echo "Actualizando CVA en el WareHouse..."
./cva/cva_to_warehouse.py


echo "Actualizando PCH en el WareHouse..."
./pch/pch_to_warehouse.py

echo 'Fin :)'
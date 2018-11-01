#!/bin/sh

python3 botgen.py -f cl.txt
rm /opt1/labs/kafka/l2/w1/LabClick/src/main/resources/data/cl.txt*
mv cl.txt  /opt1/labs/kafka/l2/w1/LabClick/src/main/resources/data/cl.txt

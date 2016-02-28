#!/bin/bash
#mvn clean package
spark-submit --class ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment5.Q5 \
   target/bigdata2016w-0.1.0-SNAPSHOT.jar --input TPC-H-0.1-TXT 

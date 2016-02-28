#!/bin/bash
if test "$#" -eq 3; then
	if [ $3 = "-c" ]; then
        	mvn clean package
	fi
fi
spark-submit --class ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment5.$1 \
   target/bigdata2016w-0.1.0-SNAPSHOT.jar --input TPC-H-0.1-TXT --date $2

#!/bin/bash
if [$# -ne 2]; 
    then 
	echo "Usage: ./generate-bundle.sh <path to source jar> <path to output directory>"
    else
	sh $SP_HOME/bin/jartobundle.sh $1 $2
fi

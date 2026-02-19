#!/bin/bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
source venv/bin/activate
rm -rf ~/.ivy2/cache          
rm -rf ~/.ivy2/jars
python stream_processor.py

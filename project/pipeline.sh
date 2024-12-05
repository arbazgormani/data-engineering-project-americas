#!/bin/bash

echo "Running the ETL pipeline........"
python3 project/pipeline.py

# Verify if ETL pipeline executed successfully
if [ $? -eq 0 ]; then
    echo "ETL pipeline executed successfully. Running test cases..."
    python3 project/test.py
else
    echo "ETL pipeline failed. Skipping test cases."
fi
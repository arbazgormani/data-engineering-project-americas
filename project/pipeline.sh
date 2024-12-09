#!/bin/bash

RED="\033[31m"
GREEN="\033[32m"
YELLOW="\033[33m"
BLUE="\033[34m"
BOLD="\033[1m"
RESET="\033[0m"

echo -e "${BOLD}${BLUE}========================================="
echo -e "         Starting the ETL Pipeline        "
echo -e "=========================================${RESET}"
echo -e "${YELLOW}Running the ETL pipeline...${RESET}"

python3 project/pipeline.py

# Verify if ETL pipeline executed successfully
if [ $? -eq 0 ]; then
    echo -e "${GREEN}${BOLD}✔ ETL pipeline executed successfully!${RESET}"
    echo -e "${YELLOW}Running test cases...${RESET}"
    python3 project/test.py
    echo -e "${GREEN}${BOLD}✔ Test cases executed successfully!${RESET}"
else
    echo -e "${RED}${BOLD}✘ ETL pipeline failed.${RESET}"
    echo -e "${RED}Skipping Test cases.${RESET}"
fi

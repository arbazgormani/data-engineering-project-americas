#!/bin/bash

RED="\033[31m"
GREEN="\033[32m"
YELLOW="\033[33m"
BLUE="\033[34m"
BOLD="\033[1m"
RESET="\033[0m"

echo -e "${BOLD}${YELLOW}========================================="
echo -e "         Starting The Test Cases                 "
echo -e "=========================================${RESET}"
echo -e "${BLUE}Running test cases...${RESET}"
python3 project/test.py

# Verify if test cases executed successfully
if [ $? -eq 0 ]; then
    echo -e "${GREEN}${BOLD}✔ Test cases executed successfully!${RESET}"
else
    echo -e "${RED}${BOLD}✘ Test cases failed.${RESET}"
fi

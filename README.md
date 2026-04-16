# MCAD-RPD
<img width="3752" height="2006" alt="screenshot of the joined table page in MCAD-RPD" src="https://github.com/user-attachments/assets/bcf13f41-cf33-418d-9c28-d2643c58e3b7" />


## Overview

This repository contains the source code for MCAD-RPD, a small-scoped analysis of the Marine Cyber Attack Database (MCAD) created with R, Python, and DuckDB.

## GitHub Action

A GitHub Action is configured to run the data processing script daily, along with rendering the HTML pages from Flask to allow for static hosting on GitHub Pages.

## Installation

Run the setup script:
```bash
bash setup.sh
```

## Usage

Process data:
```bash
Rscript src/main.r
```

View data:
```bash
source .venv/bin/activate
python src/display.py
```

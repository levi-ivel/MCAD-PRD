#!/bin/bash

# Zet renv op
Rscript -e "if(!requireNamespace('renv', quietly=TRUE)) install.packages('renv')"

# Installeer packages R
Rscript renv.r

# Zet venv op
python -m venv .venv

# Installeer packages Python
.venv/bin/pip install -r requirements.txt

echo "Voltooid!"
echo "Om data te verwerken: Rscript src/main.r"
echo "Om data te bekijken: source .venv/bin/activate && python src/display.py"

macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirement.txt
python main.py

Windows
python3 -m venv .venv
.venv\Scripts\Activate
pip install -r requirement.txt
python main.py

pytest test_unitaire/test_cleaning_functions.py 
python -m pytest --cov="[silver]transformers"
python -m pytest --cov="[silver]transformers" --cov-report=term-missing

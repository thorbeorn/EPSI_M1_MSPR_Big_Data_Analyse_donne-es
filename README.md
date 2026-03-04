macOS / Linux
cd "[docker]conf"
docker-compose up -d
cd ..
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirement.txt
python etl.py

Windows
cd "[docker]conf"
cd ..
docker-compose up -d
python3 -m venv .venv
.venv\Scripts\Activate
pip install -r requirement.txt

python main.py

pytest test_unitaire/test_cleaning_functions.py 
python -m pytest --cov="[silver]transformers"
python -m pytest --cov="[silver]transformers" --cov-report=term-missing
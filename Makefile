install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main --cov=mylib test_*.py

format:	
	black *.py 

lint:
	#disable comment to test speed
	#pylint --disable=R,C --ignore-patterns=test_.*?py *.py mylib/*.py
	#ruff linting is 10-100X faster than pylint
	ruff check *.py mylib/*.py --ignore E501

all: install lint test format 

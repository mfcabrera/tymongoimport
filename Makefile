venv:
	bin/setup-venv.sh

migrate: venv
	. venv/bin/activate

dev-venv:
	pip install -r dev-requirements.txt

install: venv migrate

flake8:
	flake8 ty/

clean:
	rm -rf venv/

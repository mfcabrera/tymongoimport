venv:
	bin/setup-venv.sh

migrate: venv
	. venv/bin/activate

dev-venv:
	pip install -r dev-requirements.txt

install: venv migrate

clean:
	rm -rf venv/

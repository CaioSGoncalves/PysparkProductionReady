all: test build

test:
	pytest tests

build:
	# rm -rf ./dist && mkdir ./dist
	rm -f ./dist/*
	cp ./main.py ./dist
	cp ./config.json ./dist
	zip -r dist/jobs.zip jobs
	zip -r dist/shared.zip shared

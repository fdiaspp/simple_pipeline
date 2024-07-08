default: build


build: 
	docker build -t simple_pipeline_spark:latest -f infra/main.dockerfile .

run: build
	docker run simple_pipeline_spark:latest 
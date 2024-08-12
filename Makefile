# Default values for dates
START_DATE ?= ""
END_DATE ?= ""

# Command to build and run the Docker Compose setup
run:
	docker-compose build --build-arg START_DATE=$(START_DATE) --build-arg END_DATE=$(END_DATE)
	docker-compose up

# Clean up the containers and images
clean:
	docker-compose down --rmi all

APP_NAME := kqlfile

.PHONY: build test fmt vet lint clean run-sample bench

build:
	go build ./cmd/$(APP_NAME)

test:
	go test ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

lint: vet

clean:
	rm -f $(APP_NAME) *.exe

run-sample:
	go run ./cmd/$(APP_NAME) --input testdata/people.csv --query "T | where age > 30 | project name, age"

bench:
	./scripts/bench.sh --size-gb 1 --rows-per-chunk 100000 --output testdata/bench.csv

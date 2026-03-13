BINARY := granicus
BUILD_DIR := build
CMD_DIR := cmd/granicus

.PHONY: build test vet clean install

build:
	go build -o $(BUILD_DIR)/$(BINARY) ./$(CMD_DIR)

test:
	go test ./...

vet:
	go vet ./...

clean:
	rm -rf $(BUILD_DIR)

install: build
	cp $(BUILD_DIR)/$(BINARY) /usr/local/bin/$(BINARY)

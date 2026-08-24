PLUGIN_ID := local-queue
GOARCH ?= $(shell go env GOARCH)
GOOS ?= $(shell go env GOOS)

ifeq ($(GOOS),darwin)
EXT := dylib
else ifeq ($(GOOS),windows)
EXT := dll
else
EXT := so
endif

.PHONY: test race build clean

test:
	go test ./...

race:
	go test -race ./...

build:
	@mkdir -p plugins/$(GOOS)/$(GOARCH)
	go build -buildmode=c-shared -o plugins/$(GOOS)/$(GOARCH)/$(PLUGIN_ID).$(EXT) .
	@rm -f plugins/$(GOOS)/$(GOARCH)/$(PLUGIN_ID).h

clean:
	go clean

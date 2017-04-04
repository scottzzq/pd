GO=GO15VENDOREXPERIMENT="1" go

all: dev 

dev: build

build:
	rm -rf vendor && ln -s _vendor/vendor vendor
	$(GO) build  -o bin/pd-server cmd/pd-server/main.go
	rm -rf vendor
	rm -rf ./default.pd/
	rm -rf log

client:
	rm -rf vendor && ln -s _vendor/vendor vendor
	$(GO) build  -o bin/pd-client cmd/pd-client/main.go
	rm -rf vendor

clean:
	rm -rf ./default.pd/
	rm -rf log

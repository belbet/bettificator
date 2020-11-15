binary = bettificator

all: linux
	
linux:
	GOOS=linux GOARCH=amd64 go build -o ./bin/$(binary)
macos:
	GOOS=darwin GOARCH=amd64 go build -o ./bin/$(binary)
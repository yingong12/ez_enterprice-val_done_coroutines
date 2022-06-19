os?=linux
port?=8686


run: 
	go run main.go bootstrap.go

build:export GOOS=$(os)
build:export GOARCH=amd64
build:
	@echo "building binary for $(GOOS)..."
	go build -o ./val_done_coroutines 
	@echo "done!"
	
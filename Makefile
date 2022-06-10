GOCC=go
MKDIR_P=mkdir -p
GIT_SUBM=git submodule

BIN_PATH=./build
BIN="./build/cid-hoarder"

.PHONY: check dependencies build clean

build: 
	$(MKDIR_P) $(BIN) fi
	$(GOCC) build -o $(BIN)


dependencies:
	$(GIT_SUBM) update --init
	cd go-libp2p-kad-dht
	git checkout cid-hoarder
	cd ../


clean:
	rm -r $(BIN_PATH)
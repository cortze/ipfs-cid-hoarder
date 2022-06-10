GOCC=go1.17.7
MKDIR_P=mkdir -p
GIT_SUBM=git submodule

BIN_PATH=./build
BIN="./build/ipfs-cid-hoarder"

.PHONY: check dependencies build install clean

build: 
	$(GOCC) build -o $(BIN)


install:
	$(GOCC) install


dependencies:
	$(GIT_SUBM) update --init
	cd go-libp2p-kad-dht && git checkout cid-hoarder


clean:
	rm -r $(BIN_PATH)



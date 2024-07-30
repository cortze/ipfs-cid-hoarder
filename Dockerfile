# Create a go image to compile the tool
FROM golang:1.22.2 AS builder

# Install git for building the dependencies
WORKDIR /
RUN apt-get install git

# Copy the relevant code into the Ipfs-Cid-Hoarder folder
# and compile the tool
COPY ./ /ipfs-cid-hoarder 
WORKDIR /ipfs-cid-hoarder
RUN make dependencies
RUN make build

# Expose the port for the Libp2p hosts
EXPOSE 9010

# define the endpoint
ENTRYPOINT ["/ipfs-cid-hoarder/build/ipfs-cid-hoarder"]



# Create a go image to compile the tool
FROM golang:1.19.7-buster AS builder

# Install git for building the dependencies
WORKDIR /
RUN apt-get install git

# Copy the relevant code into the Ipfs-Cid-Hoarder folder
# and compile the tool
COPY ./ /ipfs-cid-hoarder 
WORKDIR /ipfs-cid-hoarder
RUN make dependencies
RUN make build

# Bring the binary into a low profile debian image
FROM debian:buster-slim

# Copy the hoarder's binary into the new image
WORKDIR /
COPY --from=builder /ipfs-cid-hoarder/build/ /cli

# Expose the port for the Libp2p hosts
EXPOSE 9010

# define the endpoint
ENTRYPOINT ["/cli/ipfs-cid-hoarder"]



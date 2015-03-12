FROM phusion/baseimage:0.9.16

# Use baseimage-docker's init system.
CMD ["/sbin/my_init"]

RUN apt-get update && apt-get install -y wget git

# install and setup go
RUN wget https://storage.googleapis.com/golang/go1.4.2.linux-amd64.tar.gz
RUN tar -C /usr/local -zxf go1.4.2.linux-amd64.tar.gz
RUN mkdir /go
ENV GOPATH=/go
ENV PATH=$PATH:/usr/local/go/bin:$GOPATH/bin

# build kawana
RUN go get github.com/tools/godep
ADD . /go/src/github.com/chriskite/kawana
WORKDIR /go/src/github.com/chriskite/kawana
RUN godep go install github.com/chriskite/kawana

# setup kawana service
RUN mkdir /etc/service/kawana
ADD kawana.sh /etc/service/kawana/run

# Clean up APT when done.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

EXPOSE 9291
EXPOSE 9292

FROM golang:latest

COPY Server.go . 

RUN go build Server.go

CMD ["./Server", "9988"]

# docker build . -t go-server
# docker run -p 9988:9988 go-server
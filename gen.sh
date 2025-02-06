# protobuf generate
protoDir="./proto"
protoFile="${protoDir}/codec.proto"

if [ -f "codec.pb.go" ]; then
    rm "$protoFile"
fi

protoc --go_out=${protoDir} ${protoFile}
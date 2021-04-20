import grpc from "k6/net/grpc";
import { check } from "k6";

const GOPATH = __ENV.GOPATH ? __ENV.GOPATH : __ENV.HOME + "/go";

export const client = new grpc.Client();
client.load(["../../..", GOPATH + "/src/github.com/gogo/protobuf"], "client/pfs/pfs.proto");

// connect ensures that the exported grpc client is connected.
export function connect() {
    if (__ITER == 0) {
        client.connect("localhost:30650", {
            plaintext: true,
        });
    }
}

// grpcOK collects a metric based on the success of the grpc response.  These metrics are presented
// at the end of a test run as a % success number.
export function grpcOK(op, response) {
    const msg = op + " ok?";
    const c = {};
    c[msg] = (r) => r && r.status === grpc.StatusOK;
    check(response, c);
}

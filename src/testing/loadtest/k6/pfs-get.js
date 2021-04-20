import { client, connect, grpcOK } from "./lib/grpc.js";
import { startCommit, finishCommit, deleteRepo } from "./lib/pfs.js";

export function setup() {
    const data = startCommit();
    connect();
    const put = client.invoke("pfs.API/PutFile", {
        file: {
            commit: data.commit,
            path: "load-test-file",
        },
        value: "dGhpcyBpcyBhIHRlc3QgZmlsZQo=",
    });
    grpcOK("put file", put);
    finishCommit(data);
    client.close();
    return data;
}

export function teardown(data) {
    return deleteRepo(data);
}

export default (data) => {
    connect();
    const read = client.invoke("pfs.API/GetFile", {
        file: {
            commit: data.commit,
            path: "load-test-file",
        },
        offset_bytes: 0,
        size_bytes: 20,
    });
    grpcOK("get file", read);
};

vcl 4.1;

sub vcl_recv {
    if(req.http.Authorization) {
        return(hash);
    }
}

backend default {
    .host = "host.docker.internal";
    .port = "8777";
}

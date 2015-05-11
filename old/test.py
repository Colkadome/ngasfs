import sys
import ntpath
import requests
from ngamsPClient import ngamsPClient
from urlparse import urlparse

def postFile(sLoc, path, *options):
    # get options
    verbose = "-v" in options

    o = urlparse(sLoc)
    host = o.hostname
    port = o.port

    # send POST request to upload file, and get response.
    filename = ntpath.basename(path)

    client = ngamsPClient.ngamsPClient(host, port)
    status = client.archive(path, mimeType="application/octet-stream")

    print status.getMessage()

if __name__ == "__main__":
    postFile("http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/",sys.argv[1])
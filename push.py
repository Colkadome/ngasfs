import requests

server_location = "http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/ARCHIVE"

headers = {"Content-type":"application/octet-stream","Content-Disposition":'filename="testfile.txt"'}
r = requests.post(server_location, headers=headers, files={'testfile.txt': open('testfile.txt', mode='rb')})
print(r.text)
